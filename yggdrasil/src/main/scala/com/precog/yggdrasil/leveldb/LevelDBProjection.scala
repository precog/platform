package com.precog.yggdrasil
package leveldb

import com.precog.common._ 
import com.precog.util._ 
import com.precog.util.Bijection._

import org.iq80.leveldb._
import org.fusesource.leveldbjni.JniDBFactory._
import org.fusesource.leveldbjni.DataWidth

import java.io._
import java.nio.Buffer
import java.nio.ByteBuffer
import java.util.Map.Entry
import java.util.concurrent.TimeoutException
import Bijection._

import com.weiglewilczek.slf4s.Logger
import scala.collection.JavaConverters._
import scala.collection.Iterator
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

import scalaz.{Ordering => _, Source => _, _}
import scalaz.Scalaz._
import scalaz.effect._
import scalaz.iteratee._
import scalaz.iteratee.Input._
import scalaz.syntax.plus._
import scalaz.syntax.monad._
import scalaz.syntax.applicativePlus._
import scalaz.syntax.biFunctor
import scalaz.Scalaz._
import IterateeT._

import blueeyes.json.JPath
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import blueeyes.json.JsonParser
import blueeyes.json.Printer
import blueeyes.json.xschema._
import blueeyes.json.xschema.Extractor._
import blueeyes.json.xschema.DefaultSerialization._

object LevelDBProjectionComparator {

  import Printer._

  def apply(_descriptor: ProjectionDescriptor) = new DBComparator {
    val projection: ByteProjection = new LevelDBByteProjection {
      val descriptor = _descriptor
    }

    def name = compact(render(_descriptor.serialize))
    def compare(k1: Array[Byte], k2: Array[Byte]) = projection.keyOrder.order(k1, k2).toInt

    // default implementations
    def findShortestSeparator(start: Array[Byte], limit: Array[Byte]) = start
    def findShortSuccessor(key: Array[Byte]) = key
  }
}

object LevelDBProjection {
  private final val comparatorMetadataFilename = "comparator"

  def apply(baseDir : File, descriptor: ProjectionDescriptor): ValidationNEL[Throwable, LevelDBProjection] = {
    val baseDirV = if (! baseDir.exists && ! baseDir.mkdirs()) (new RuntimeException("Could not create database basedir " + baseDir): Throwable).fail[File] 
                   else baseDir.success[Throwable]

    baseDirV.toValidationNel map { (bd: File) => new LevelDBProjection(bd, descriptor) }
  }

//  val descriptorFile = "projection_descriptor.json"
//  def descriptorSync(baseDir: File): Sync[ProjectionDescriptor] = new CommonSync[ProjectionDescriptor](baseDir, descriptorFile)

//  private class CommonSync[T](baseDir: File, filename: String)(implicit extractor: Extractor[T], decomposer: Decomposer[T]) extends Sync[T] {
//    import java.io._
//
//    val df = new File(baseDir, filename)
//
//    def read = 
//      if (df.exists) Some {
//        IO {
//          val reader = new FileReader(df)
//          try {
//            { (err: Extractor.Error) => err.message } <-: JsonParser.parse(reader).validated(extractor)
//          } finally {
//            reader.close
//          }
//        }
//      } else {
//        None
//      }
//
//    def sync(data: T) = IO {
//      Validation.fromTryCatch {
//        val tmpFile = File.createTempFile(filename, ".tmp", baseDir)
//        val writer = new FileWriter(tmpFile)
//        try {
//          compact(render(data.serialize(decomposer)), writer)
//        } finally {
//          writer.close
//        }
//        tmpFile.renameTo(df) // TODO: This is only atomic on POSIX systems
//        Success(())
//      }
//    }
//  }
}

class LevelDBProjection private (val baseDir: File, val descriptor: ProjectionDescriptor) extends LevelDBByteProjection with Projection[IterableDataset] {
  import LevelDBProjection._

  val chunkSize = 32000 // bytes
  val maxOpenFiles = 25

  val logger = Logger("col:" + baseDir)
  logger.debug("Opening column index files")

  private val createOptions = (new Options)
    .createIfMissing(true)
    .maxOpenFiles(maxOpenFiles)
    .blockSize(1024 * 1024) // Based on rudimentary benchmarking. Gains in the high single digit percents

  private lazy val idIndexFile: DB = factory.open(new File(baseDir, "idIndex"), createOptions)
  //private lazy val valIndexFile: DB = {
  //   factory.open(new File(baseDir, "valIndex"), createOptions.comparator(comparator))
  //}

  private final val syncOptions = (new WriteOptions).sync(true)

  def close: IO[Unit] = IO {
    logger.debug("Closing column index files")
    idIndexFile.close()
    //valIndexFile.close()
  }

  def sync: IO[Unit] = IO { } 

  def valueOffsets(values: Array[Byte]): List[Int] = {
    val buf = ByteBuffer.wrap(values)
    val positions = descriptor.columns.map(_.valueType.format).foldLeft(List(0)) {
      case (v :: vx, FixedWidth(w))  => (v + w) :: v :: vx
      case (v :: vx, LengthEncoded)  => (v + buf.getInt(v)) :: v :: vx
    } 

    positions.tail.reverse
  }

  def insert(id : Identities, v : Seq[CValue], shouldSync: Boolean = false): IO[Unit] = IO {
    val (idBytes, valueBytes) = project(id, v)

    if (shouldSync) {
      idIndexFile.put(idBytes, valueBytes, syncOptions)
    } else {
      idIndexFile.put(idBytes, valueBytes)
    }
  }

  ///////////////////
  // ID Traversals //
  ///////////////////

  val readAheadSize = 2 // TODO: Make configurable
  val readPollTime = 100l

  import java.util.concurrent.{ArrayBlockingQueue,TimeUnit}
  import java.util.concurrent.atomic.AtomicBoolean
  import org.fusesource.leveldbjni.internal.JniDBIterator
  import org.fusesource.leveldbjni.KeyValueChunk
  import org.fusesource.leveldbjni.KeyValueChunk.KeyValuePair

  class ChunkReader(iterator: JniDBIterator, expiresAt: Long) extends Thread {
    val bufferQueue = new ArrayBlockingQueue[Pair[ByteBuffer,ByteBuffer]](readAheadSize) // Need a key and value buffer for each readahead

    // pre-fill the buffer queue
    (1 to readAheadSize).foreach {
      _ => bufferQueue.put((ByteBuffer.allocate(chunkSize), ByteBuffer.allocate(chunkSize)))
    }

    def returnBuffers(chunk: KeyValueChunk) {
      bufferQueue.put((chunk.keyData, chunk.valueData))
    }
    
    val chunkQueue  = new ArrayBlockingQueue[Input[KeyValueChunk]](readAheadSize + 1)

    val running = new AtomicBoolean(true)

    override def run() {
      while (running.get && iterator.hasNext) {
        var buffers : Pair[ByteBuffer,ByteBuffer] = null
        while (running.get && buffers == null) {
          if (System.currentTimeMillis > expiresAt) {
            iterator.close()
            running.set(false)
            chunkQueue.put(emptyInput)
            return
          }
          buffers = bufferQueue.poll(readPollTime, TimeUnit.MILLISECONDS)
        } 

        if (buffers != null) {
          val chunk = elInput(iterator.nextChunk(buffers._1, buffers._2, DataWidth.VARIABLE, DataWidth.VARIABLE))

          while (running.get && ! chunkQueue.offer(chunk, readPollTime, TimeUnit.MILLISECONDS)) {
            if (System.currentTimeMillis > expiresAt) {
              iterator.close()
              running.set(false)
              chunkQueue.put(emptyInput)
              return
            }
          }
        }
      }

      chunkQueue.put(eofInput) // We're here because we reached the end of the iterator, so block and submit

      iterator.close()
    }
  }

  private final val emptyJavaIterator = new java.util.Iterator[KeyValuePair] {
    def hasNext() = false
    def next() = throw new NoSuchElementException("Empty iterator")
    def remove() = throw new UnsupportedOperationException("Iterators cannot remove")
  }

  def traverseIndex(expiresAt: Long): IterableDataset[Seq[CValue]] = IterableDataset[Seq[CValue]](1, new Iterable[(Identities,Seq[CValue])]{
    def iterator = new Iterator[(Identities,Seq[CValue])] {
      val iter = idIndexFile.iterator.asInstanceOf[JniDBIterator]
      iter.seekToFirst

      val reader = new ChunkReader(iter, expiresAt)
      reader.start()

      private[this] var currentChunk: Input[KeyValueChunk] = reader.chunkQueue.take()

      private final def nextIterator = 
        currentChunk.fold(
          empty = throw new TimeoutException("Iteration expired"),
          el    = chunk => chunk.getIterator(),
          eof   = emptyJavaIterator
        )

      private[this] var chunkIterator: java.util.Iterator[KeyValuePair] = nextIterator

      def hasNext: Boolean = if(currentChunk.isEof) false else {
        if (chunkIterator.hasNext) true else {
          currentChunk.foreach(chunk => reader.returnBuffers(chunk))
          currentChunk = reader.chunkQueue.take()
          chunkIterator = nextIterator
          chunkIterator.hasNext
        }
      }

      def next: (Identities,Seq[CValue]) = {
        val kvPair = chunkIterator.next()
        unproject(kvPair.getKey, kvPair.getValue)
      }
    }
  })

  @inline final def getAllPairs(expiresAt: Long): IterableDataset[Seq[CValue]] = traverseIndex(expiresAt)

//  def traverseIndexEnumerator[E, F[_]](expiresAt: Long)(f: (Identities, Seq[CValue]) => E)(implicit MO: F |>=| IO): EnumeratorT[X, Vector[E], F] = {
//    import MO._
//    import MO.MG.bindSyntax._
//
//    new EnumeratorT[X, Vector[E], F] { self =>
//      def apply[A] = {
//        val iterIO : IO[ChunkReader] = IO(idIndexFile.iterator) map { i => i.seekToFirst; val reader = new ChunkReader(i.asInstanceOf[JniDBIterator], expiresAt); reader.start(); reader }
//
//        def step(s : StepT[X, Vector[E], F, A], ioF: F[ChunkReader]): IterateeT[X, Vector[E], F, A] = {
//          @inline def _done = iterateeT[X, Vector[E], F, A](ioF.flatMap(reader => MO.promote(IO(reader.running.set(false)))) >> s.pointI.value)
//
//          @inline def next(k: Input[Vector[E]] => IterateeT[X, Vector[E], F, A], reader: ChunkReader) = {
//            val buffer = new ArrayBuffer[E](chunkSize / 8) // Assume longs as a *very* rough target
//            val chunkInput : Input[KeyValueChunk] = reader.chunkQueue.poll(readPollTime, TimeUnit.MILLISECONDS)
//            val _empty = k(emptyInput) >>== (s => step(s, MO.MG.point(reader)))
//
//
//            if (chunkInput == null) {
//              _empty
//            } else {
//              chunkInput.fold(
//                empty = _empty,
//                el = chunk => {
//                  val chunkIter: java.util.Iterator[KeyValuePair] = chunk.getIterator()
//                  while (chunkIter.hasNext) {
//                      val kvPair = chunkIter.next()
//                    buffer += unproject(kvPair.getKey, kvPair.getValue)(f)
//                  }
//                  val outChunk = Vector(buffer: _*)
//
//                  // return the backing buffers for the next readahead
//                  reader.returnBuffers(chunk)
//
//                  k(elInput(outChunk)) >>== (s => step(s, MO.MG.point(reader)))
//                },
//                eof = _done
//              )
//            }
//          }
//
//          s.fold(
//            cont = k => {
//              if (System.currentTimeMillis >= expiresAt) {
//                iterateeT(ioF.flatMap(reader => MO.promote(IO(reader.running.set(false)))) >>  Monad[F].point(StepT.serr[X, Vector[E], F, A](new TimeoutException("Iteration expired"))))
//              } else {
//                iterateeT(ioF.flatMap(reader => next(k, reader).value))
//              }
//            },
//            done = (_, _) => _done,
//            err = _ => _done
//          )
//        }
//
//        step(_, MO.promote(iterIO))
//      }
//    }
//  }
//
//  def getAllPairs(expiresAt: Long) : EnumeratorP[X, Vector[(Identities, Seq[CValue])], IO] = new EnumeratorP[X, Vector[(Identities, Seq[CValue])], IO] {
//    def apply[F[_]](implicit MO: F |>=| IO) = traverseIndex[(Identities, Seq[CValue]), F](expiresAt) {
//      (id, b) => (id, b)
//    }
//  }
//
//  def getAllColumnPairs(columnIndex: Int, expiresAt: Long) : EnumeratorP[X, Vector[(Identities, CValue)], IO] = new EnumeratorP[X, Vector[(Identities, CValue)], IO] {
//    def apply[F[_]](implicit MO: F |>=| IO) = traverseIndex[(Identities, CValue), F](expiresAt) {
//      (id, b) => (id, b(columnIndex))
//    }
//  }
//
//  def getAllIds(expiresAt: Long) : EnumeratorP[X, Vector[Identities], IO] = new EnumeratorP[X, Vector[Identities], IO] {
//    def apply[F[_]](implicit MO: F |>=| IO) = traverseIndex[Identities, F](expiresAt) {
//      (id, b) => id
//    }
//  }
//
//  def getAllValues(expiresAt: Long) : EnumeratorP[X, Vector[Seq[CValue]], IO] = new EnumeratorP[X, Vector[Seq[CValue]], IO] {
//    def apply[F[_]](implicit MO: F |>=| IO) = traverseIndex[Seq[CValue], F](expiresAt) {
//      (id, b) => b
//    }
//  }


//  def traverseIndexRange[X, E, F[_]](range: Interval[Identities])(f: (Identities, Seq[CValue]) => E)(implicit MO : F |>=| IO): EnumeratorT[X, Vector[E], F] = {
//    import MO._
//    import MO.MG.bindSyntax._
//
//    new EnumeratorT[X, Vector[E], F] { 
//      def apply[A] = {
//        val iterIO = IO(idIndexFile.iterator) map { iter =>
//          range.start match {
//            case Some(id) => iter.seek(id.as[Array[Byte]])
//            case None => iter.seekToFirst()
//          }
//
//          iter
//        }
//
//        def step(s: StepT[X, Vector[E], F, A], iterF: F[DBIterator]): IterateeT[X, Vector[E], F, A] = {
//          @inline def _done = iterateeT[X, Vector[E], F, A](iterF.flatMap(iter => MO.promote(IO(iter.close))) >> s.pointI.value)
//
//          @inline def next(iter: DBIterator, k: Input[Vector[E]] => IterateeT[X, Vector[E], F, A]) = if (iter.hasNext) {
//            val rawValues = iter.asScala.map(n => (n, n.getKey.as[Identities])).take(chunkSize)
//            val chunk = Vector(range.end.map(end => rawValues.takeWhile(_._2 < end)).getOrElse(rawValues).map { case (n, ids) => unproject(n.getKey, n.getValue)(f) }.toSeq: _*)
//            
//            if (chunk.isEmpty) {
//              _done
//            } else {
//              k(elInput(chunk)) >>== (s => step(s, MO.MG.point(iter)))
//            }
////              val n = iter.next
////              val id = n.getKey.as[Identities]
////              range.end match {
////                case Some(end) if end <= id => _done
////                case _ => k(elInput(unproject(n.getKey, n.getValue)(f))) >>== step
////              }
//          } else {
//            _done
//          } 
//
//          s.fold(
//            cont = k => iterateeT(iterF flatMap (next(_, k).value)),
//            done = (_, _) => _done,
//            err = _ => _done
//          )
//        }
//
//        step(_, MO.promote(iterIO))
//      }
//    }
//  }
//
//  def getPairsByIdRange[X](range: Interval[Identities]): EnumeratorP[X, Vector[(Identities, Seq[CValue])], IO] = new EnumeratorP[X, Vector[(Identities, Seq[CValue])], IO] {
//    def apply[F[_]](implicit MO: F |>=| IO) = traverseIndexRange[X, (Identities, Seq[CValue]), F](range) {
//      (id, b) => (id, b)
//    }
//  }

  //////////////////////
  // Value Traversals //
  //////////////////////

//  /**
//   * Retrieve all distinct values.
//   */
//  def getAllValues[F[_] : MonadIO, A] : EnumeratorT[Unit, Array[Byte], F, A] = {
//    def valuePart(arr: Array[Byte]) = arr.take(arr.length - 8)
//    def enumerator(iter : DBIterator, close : F[Unit]): EnumeratorT[Unit, Array[Byte], F, A] = { s =>
//      s.fold(
//        cont = k => if (iter.hasNext) {
//          logger.trace("Processing next valIndex value")
//          val valueKey = iter.next.getKey
//          val value = valuePart(valueKey)
//
//          // advance the iterator until the next value to be taken from it is either the end of the iterator
//          // or not equal to the current value (this operation gives 'distinct' semantics)
//          while (iter.hasNext && comparator.compare(valueKey, iter.peekNext.getKey) == 0) {
//            logger.trace("  advancing iterator on same value")
//            iter.next
//          }
//
//          k(elInput(value)) >>== enumerator(iter, close)
//        } else {
//          logger.trace("No more values")
//          iterateeT(close >> s.pointI.value)
//        }, 
//        done = (_, _) => { logger.trace("Done with iteration"); iterateeT(close >> s.pointI.value) },
//        err = _ => { logger.trace("Error on iteration"); iterateeT(close >> s.pointI.value) }
//      )
//    }
//       
//    val iter = valIndexFile.iterator 
//    iter.seekToFirst()
//    enumerator(iter, IO(iter.close).liftIO[F])
//  }
//
//  /**
//   * Retrieve all IDs for values in the given range [start,end]
//   */
//  def getIdsByValueRange[F[_] : MonadIO, A](range : Interval[Array[Byte]]): EnumeratorT[Unit, Identities, F, A] = {
//    def enumerator(iter: DBIterator, close: F[Unit]): EnumeratorT[Unit, Identities, F, A] = { s =>
//      s.fold(
//        cont = k => if (iter.hasNext) {
//          val n = iter.next
//          val (cv, ci) = n.getKey.splitAt(n.getKey.length - 8)
//          range.end match {
//            case Some(end) if comparator.compare(cv, end) >= 0 => iterateeT(close >> s.pointI.value)
//            case _ => k(elInput(ci.as[Identities])) >>== enumerator(iter, close)
//          }
//        } else {
//          iterateeT(close >> s.pointI.value)
//        },
//        done = (_, _) => iterateeT(close >> s.pointI.value),
//        err = _ => iterateeT(close >> s.pointI.value)
//      )
//    }
//
//    val iter = valIndexFile.iterator
//    range.start match {
//      case Some(v) => iter.seek(columnKeys(0L, v)._2)
//      case None    => iter.seekToFirst()
//    }
//
//    enumerator(iter, IO(iter.close).liftIO[F])
//  }
//
//  /**
//   * Retrieve all IDs for the given value
//   */
//  def getIdsForValue[F[_] : MonadIO, A](v : Array[Byte]): EnumeratorT[Unit, Identities, F, A] = 
//    getIdsByValueRange(Interval(Some(v), Some(v)))
//
//  /**
//   * Retrieve all values in the given range [start,end]
//   */
//  def getValuesInRange[F[_] : MonadIO, A](range : Interval[Array[Byte]]): EnumeratorT[Unit, Array[Byte], F, A] = {
//    def enumerator(iter: DBIterator, close: F[Unit]): EnumeratorT[Unit, Array[Byte], F, A] = { s =>
//      s.fold(
//        cont = k => if (iter.hasNext) {
//          val n = iter.next
//          val (cv,ci) = n.getKey.splitAt(n.getKey.length - 8)
//          range.end match {
//            case Some(end) if comparator.compare(cv, end) >= 0 => iterateeT(close >> s.pointI.value)
//            case _ => k(elInput(cv)) >>== enumerator(iter, close)
//          }
//        } else {
//          iterateeT(close >> s.pointI.value)
//        },
//        done = (_, _) => iterateeT(close >> s.pointI.value),
//        err = _ => iterateeT(close >> s.pointI.value)
//      )
//    }
//
//    val iter = valIndexFile.iterator
//    range.start match {
//      case Some(v) => 
//        val (_, valIndexBytes) = columnKeys(0L, v)
//        iter.seek(valIndexBytes)
//      case None => iter.seekToFirst()
//    }
//    enumerator(iter, IO(iter.close).liftIO[F])
//  }
}
