/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
package com.precog.yggdrasil
package leveldb

import iterable._
import com.precog.common._ 
import com.precog.util._ 
import com.precog.util.Bijection._

import org.iq80.leveldb._
import org.fusesource.leveldbjni.JniDBFactory
import org.fusesource.leveldbjni.DataWidth

import java.io._
import java.nio.ByteBuffer
import java.util.concurrent.TimeoutException

import com.weiglewilczek.slf4s.Logger
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import scalaz.{Ordering => _, _}
import scalaz.effect._
import scalaz.iteratee._
import scalaz.iteratee.Input._
import scalaz.syntax.plus._
import scalaz.syntax.monad._
import scalaz.syntax.applicativePlus._
import scalaz.syntax.bifunctor
import scalaz.syntax.show._
import scalaz.Scalaz._
import IterateeT._

trait LevelDBProjection extends LevelDBByteProjection {
  val baseDir: File
  val descriptor: ProjectionDescriptor

  val chunkSize = 32000 // bytes
  val maxOpenFiles = 25

  val logger = Logger("col:" + descriptor.shows)
  logger.debug("Opening column index files")

  override def toString = "LevelDBProjection(" + descriptor.columns + ")"

  private val createOptions = (new Options)
    .createIfMissing(true)
    .maxOpenFiles(maxOpenFiles)
    .blockSize(1024 * 1024) // Based on rudimentary benchmarking. Gains in the high single digit percents

  protected lazy val idIndexFile: DB = JniDBFactory.factory.open(new File(baseDir, "idIndex"), createOptions)

  private final val syncOptions = (new WriteOptions).sync(true)

  def close() = {
    logger.info("Closing column index files")
    idIndexFile.close()
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
    val (idBytes, valueBytes) = toBytes(id, v)

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

  protected def emptyJavaIterator = new java.util.Iterator[KeyValuePair] {
    def hasNext() = false
    def next() = throw new NoSuchElementException("Empty iterator")
    def remove() = throw new UnsupportedOperationException("Iterators cannot remove")
  }

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
//                    buffer += fromBytes(kvPair.getKey, kvPair.getValue)(f)
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
//            val chunk = Vector(range.end.map(end => rawValues.takeWhile(_._2 < end)).getOrElse(rawValues).map { case (n, ids) => fromBytes(n.getKey, n.getValue)(f) }.toSeq: _*)
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
////                case _ => k(elInput(fromBytes(n.getKey, n.getValue)(f))) >>== step
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
