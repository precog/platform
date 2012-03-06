package com.precog.yggdrasil
package leveldb

import com.precog.analytics.Path
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

class LevelDBProjection private (val baseDir: File, val descriptor: ProjectionDescriptor) extends LevelDBByteProjection with Projection {
  import LevelDBProjection._

  val chunkSize = 32000 // bytes
  val chunkBuffer = ByteBuffer.allocate(chunkSize)
  val maxOpenFiles = 25

  val logger = Logger("col:" + baseDir)
  logger.debug("Opening column index files")

  private val createOptions = (new Options)
    .createIfMissing(true)
    .maxOpenFiles(maxOpenFiles)
  private lazy val idIndexFile: DB = factory.open(new File(baseDir, "idIndex"), createOptions.comparator(LevelDBProjectionComparator(descriptor)))
  //private lazy val valIndexFile: DB = {
  //   factory.open(new File(baseDir, "valIndex"), createOptions.comparator(comparator))
  //}

  private final val syncOptions = (new WriteOptions).sync(true)

  def close: IO[Unit] = IO {
    logger.info("Closing column index files")
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

  def traverseIndex[E, F[_]](expiresAt: Long)(f: (Identities, Seq[CValue]) => E)(implicit MO: F |>=| IO): EnumeratorT[X, Vector[E], F] = {
    import MO._
    import MO.MG.bindSyntax._

    new EnumeratorT[X, Vector[E], F] { self =>
      import org.fusesource.leveldbjni.internal.JniDBIterator
      import org.fusesource.leveldbjni.KeyValueChunk.KeyValuePair

      def apply[A] = {
        val iterIO  = IO(idIndexFile.iterator) map { i => i.seekToFirst; i.asInstanceOf[JniDBIterator] }

        def step(s : StepT[X, Vector[E], F, A], iterF: F[JniDBIterator]): IterateeT[X, Vector[E], F, A] = {
          @inline def _done = iterateeT[X, Vector[E], F, A](iterF.flatMap(iter => MO.promote(IO(iter.close))) >> s.pointI.value)

          @inline def next(iter: JniDBIterator, k: Input[Vector[E]] => IterateeT[X, Vector[E], F, A]) = if (iter.hasNext) {
            val buffer = new ArrayBuffer[E](chunkSize / 8) // Assume longs as a *very* rough target
            val chunkIter: java.util.Iterator[KeyValuePair] = iter.nextChunk(chunkBuffer, DataWidth.VARIABLE, DataWidth.VARIABLE).getIterator
            while (chunkIter.hasNext) {
              val kvPair = chunkIter.next()
              buffer += unproject(kvPair.getKey, kvPair.getValue)(f)
            }
            val chunk = Vector(buffer: _*)
            k(elInput(chunk)) >>== (s => step(s, MO.MG.point(iter)))
          } else {
            _done
          } 

          s.fold(
            cont = k => {
              if (System.currentTimeMillis >= expiresAt) {
                iterateeT(iterF.flatMap(iter => MO.promote(IO(iter.close))) >> Monad[F].point(StepT.serr[X, Vector[E], F, A](new TimeoutException("Iteration expired"))))
              } else {
                iterateeT(iterF flatMap (next(_, k).value))
              }
            },
            done = (_, _) => _done,
            err = _ => _done
          )
        }

        step(_, MO.promote(iterIO))
      }
    }
  }

  def getAllPairs(expiresAt: Long) : EnumeratorP[X, Vector[(Identities, Seq[CValue])], IO] = new EnumeratorP[X, Vector[(Identities, Seq[CValue])], IO] {
    def apply[F[_]](implicit MO: F |>=| IO) = traverseIndex[(Identities, Seq[CValue]), F](expiresAt) {
      (id, b) => (id, b)
    }
  }

  def getAllColumnPairs(columnIndex: Int, expiresAt: Long) : EnumeratorP[X, Vector[(Identities, CValue)], IO] = new EnumeratorP[X, Vector[(Identities, CValue)], IO] {
    def apply[F[_]](implicit MO: F |>=| IO) = traverseIndex[(Identities, CValue), F](expiresAt) {
      (id, b) => (id, b(columnIndex))
    }
  }

  def getAllIds(expiresAt: Long) : EnumeratorP[X, Vector[Identities], IO] = new EnumeratorP[X, Vector[Identities], IO] {
    def apply[F[_]](implicit MO: F |>=| IO) = traverseIndex[Identities, F](expiresAt) {
      (id, b) => id
    }
  }

  def getAllValues(expiresAt: Long) : EnumeratorP[X, Vector[Seq[CValue]], IO] = new EnumeratorP[X, Vector[Seq[CValue]], IO] {
    def apply[F[_]](implicit MO: F |>=| IO) = traverseIndex[Seq[CValue], F](expiresAt) {
      (id, b) => b
    }
  }


  def traverseIndexRange[X, E, F[_]](range: Interval[Identities])(f: (Identities, Seq[CValue]) => E)(implicit MO : F |>=| IO): EnumeratorT[X, Vector[E], F] = {
    import MO._
    import MO.MG.bindSyntax._

    new EnumeratorT[X, Vector[E], F] { 
      def apply[A] = {
        val iterIO = IO(idIndexFile.iterator) map { iter =>
          range.start match {
            case Some(id) => iter.seek(id.as[Array[Byte]])
            case None => iter.seekToFirst()
          }

          iter
        }

        def step(s: StepT[X, Vector[E], F, A], iterF: F[DBIterator]): IterateeT[X, Vector[E], F, A] = {
          @inline def _done = iterateeT[X, Vector[E], F, A](iterF.flatMap(iter => MO.promote(IO(iter.close))) >> s.pointI.value)

          @inline def next(iter: DBIterator, k: Input[Vector[E]] => IterateeT[X, Vector[E], F, A]) = if (iter.hasNext) {
            val rawValues = iter.asScala.map(n => (n, n.getKey.as[Identities])).take(chunkSize)
            val chunk = Vector(range.end.map(end => rawValues.takeWhile(_._2 < end)).getOrElse(rawValues).map { case (n, ids) => unproject(n.getKey, n.getValue)(f) }.toSeq: _*)
            
            if (chunk.isEmpty) {
              _done
            } else {
              k(elInput(chunk)) >>== (s => step(s, MO.MG.point(iter)))
            }
//              val n = iter.next
//              val id = n.getKey.as[Identities]
//              range.end match {
//                case Some(end) if end <= id => _done
//                case _ => k(elInput(unproject(n.getKey, n.getValue)(f))) >>== step
//              }
          } else {
            _done
          } 

          s.fold(
            cont = k => iterateeT(iterF flatMap (next(_, k).value)),
            done = (_, _) => _done,
            err = _ => _done
          )
        }

        step(_, MO.promote(iterIO))
      }
    }
  }

  def getPairsByIdRange[X](range: Interval[Identities]): EnumeratorP[X, Vector[(Identities, Seq[CValue])], IO] = new EnumeratorP[X, Vector[(Identities, Seq[CValue])], IO] {
    def apply[F[_]](implicit MO: F |>=| IO) = traverseIndexRange[X, (Identities, Seq[CValue]), F](range) {
      (id, b) => (id, b)
    }
  }

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
