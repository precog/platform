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
package memoization

import iterable._
import serialization._
import util._

import java.io._
import java.util.{PriorityQueue, Comparator, Arrays}
import java.util.zip._

import scala.annotation.tailrec
import scalaz._
import scalaz.effect._
import scalaz.iteratee._
import scalaz.syntax.monad._
import scalaz.syntax.semigroup._
import scalaz.syntax.std._
import Iteratee._

trait DiskMemoizationConfig {
  def memoizationBufferSize: Int
  def memoizationWorkDir: File
  def sortBufferSize: Int
  def sortWorkDir: File
}

trait DiskMemoizationComponent extends YggConfigComponent with MemoizationEnvironment { self =>
  type YggConfig <: DiskMemoizationConfig
  type Memoable[α] = Iterable[α]
  type MemoId = Int

  def withMemoizationContext[A](f: MemoContext => A) = {
    val ctx = new MemoContext { } 
    try {
      f(ctx)
    } finally {
      ctx.cache.purge()
    }
  }

  sealed trait MemoContext extends MemoizationContext[Iterable] { ctx => 
    type CacheKey[α] = MemoId
    type CacheValue[A] = Iterable[A]

    @volatile private var memoCache: KMap[CacheKey, CacheValue] = KMap.empty[CacheKey, CacheValue]
    @volatile private var files: Vector[File] = Vector()

    def memoize[A](dataset: Iterable[A], memoId: Int)(implicit serialization: IncrementalSerialization[A]): Iterable[A] = {
      def store(iter: Iterator[A]): CacheValue[A] = {
        @tailrec def buffer(i: Int, acc: Vector[A]): Vector[A] = {
          if (i < yggConfig.memoizationBufferSize && iter.hasNext) buffer(i+1, acc :+ iter.next)
          else acc
        }

        @tailrec def spill(writer: serialization.IncrementalWriter, out: DataOutputStream, iter: Iterator[A]): serialization.IncrementalWriter = {
          if (iter.hasNext) spill(writer.write(out, iter.next), out, iter)
          else writer
        }

        val initial = buffer(0, Vector()) 
        if (initial.length < yggConfig.memoizationBufferSize) {
          initial
        } else {
          val memoFile = new File(yggConfig.memoizationWorkDir, "memo." + memoId)
          using(serialization.oStream(memoFile)) { out =>
            spill(spill(serialization.writer, out, initial.iterator), out, iter).finish(out)
          }
          files :+= memoFile 

          new Iterable[A] { 
            def iterator = serialization.reader.read(serialization.iStream(memoFile), true)
          }
        }
      }

      // yup, we block the whole world. Yay.
      ctx.synchronized {
        memoCache.get[A](memoId) match {
          case Some(cacheValue) => 
            cacheValue

          case None => 
            val cacheValue = store(dataset.iterator)
            memoCache += (memoId -> cacheValue)
            cacheValue
        }
      }
    }

    def sort[A](values: Iterable[A], memoId: Int)(implicit buffering: Buffering[A], fs: SortSerialization[A]): Iterable[A] = {
      val buf = buffering.newBuffer(yggConfig.sortBufferSize)

      def sortFile(i: Int) = new File(yggConfig.sortWorkDir, "sort." + memoId + "." + i)

      def writeSortedChunk(chunkId: Int, limit: Int): File = {
        buf.sort(limit)
        val chunkFile = sortFile(chunkId)
        using(fs.oStream(chunkFile)) { fs.write(_, buf.buffer, limit) }
        chunkFile
      }

      @tailrec def writeChunked(files: Vector[File], v: Iterator[A]): Vector[File] = {
        if (v.hasNext) {
          val limit = buf.bufferChunk(v)
          writeChunked(files :+ writeSortedChunk(files.length, limit), v)
        } else {
          files
        }
      }

      def mergeIterator(toMerge: Vector[(Iterator[A], () => Unit)]): Iterator[A] = {
        class Cell(iter: Iterator[A]) {
          var value: A = iter.next
          def advance(): Unit = {
            value = if (iter.hasNext) iter.next else null.asInstanceOf[A]
            this
          }
        }

        val cellComparator = buffering.order.contramap((c: Cell) => c.value).toJavaComparator
        val (streams, closes) = toMerge.unzip
        
        // creating a priority queue of size 0 will cause an NPE
        if (streams.size == 0) {
          Iterator.empty
        } else {
          new Iterator[A] {
            private val heads: PriorityQueue[Cell] = new PriorityQueue[Cell](streams.size, cellComparator) 
            streams.foreach(i => heads.add(new Cell(i)))

            def hasNext = {
              if (heads.isEmpty) closes.foreach(_())
              !heads.isEmpty
            }

            def next = {
              assert(!heads.isEmpty) 
              val cell = heads.poll
              val result = cell.value
              cell.advance
              if (cell.value != null) heads.offer(cell)
              result
            }
          }
        }
      }

      ctx.synchronized {
        memoCache.get[A](memoId) match {
          case Some(cacheValue) => cacheValue
          case None =>
            // first buffer up to the limit of the iterator, so that we can stay in-memory if possible
            val limit = buf.bufferChunk(values.iterator)
            val cacheValue = 
              if (limit < buf.buffer.length) {
                // if we fit in memory, just sort then return an iterator over the buffer
                buf.sort(limit)
                new Iterable[A] {
                  def iterator = new Iterator[A] {
                    private var i = 0
                    def hasNext = i < limit
                    def next = {
                      val tmp = buf.buffer(i)
                      i += 1
                      tmp
                    }
                  }
                }
              } else {
                // dump the chunk to disk, then dump the remainder of the iterator to disk in chunks
                // and return an iterator over the full set of files
                val initialChunkFile = writeSortedChunk(0, limit)
                val chunkFiles = writeChunked(Vector(initialChunkFile), values.iterator)
                files ++= chunkFiles
                new Iterable[A] {
                  def iterator = mergeIterator {
                    chunkFiles flatMap { f =>
                      val stream = fs.iStream(f)
                      val iter = fs.reader(stream)
                      if (iter.hasNext) {
                        Some((iter, () => stream.close)) 
                      } else {
                        stream.close
                        None
                      }
                    }
                  }
                }
              }

            memoCache += (memoId -> cacheValue)
            cacheValue
        }
      }
    }

    object cache extends MemoCache {
      def expire(memoId: Int) = {
        ctx.synchronized { 
          memoCache.get[Any](memoId) foreach { cacheValue =>
            memoCache -= memoId
          }
        }
      }

      def purge() = {
        ctx.synchronized {
          for (f <- files) f.delete
          files = Vector()
          memoCache = KMap.empty[CacheKey, CacheValue]
        }
      }
    }
  }
}
