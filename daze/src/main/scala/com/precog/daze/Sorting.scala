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
package com.precog
package daze

import yggdrasil._
import yggdrasil.serialization._
import com.precog.common.VectorCase
import com.precog.util._

import scala.annotation.tailrec
import scalaz._
import scalaz.std.anyVal.longInstance

trait Buffering[A] {
  implicit val order: Order[A]
  def newBuffer(size: Int): Buffer

  trait Buffer {
    val buffer: Array[A]
    def sort(limit: Int): Unit
    def bufferChunk(v: Iterator[A]): Int = {
      @tailrec def insert(j: Int, iter: Iterator[A]): Int = {
        if (j < buffer.length && iter.hasNext) {
          buffer(j) = iter.next()
          insert(j + 1, iter)
        } else j
      }

      insert(0, v)
    }
  }
}

object Buffering {
  import java.util.Arrays

  implicit def refBuffering[A <: AnyRef](implicit ord: Order[A], cm: ClassManifest[A]): Buffering[A] = new Buffering[A] {
    implicit val order = ord
    private val javaOrder = order.toJavaComparator

    def newBuffer(size: Int) = new Buffer {
      val buffer = new Array[A](size)
      def sort(limit: Int) = Arrays.sort(buffer, 0, limit, javaOrder)
    }
  }

  implicit object longBuffering extends Buffering[Long] {
    implicit val order = longInstance
    def newBuffer(size: Int) = new Buffer {
      val buffer = new Array[Long](size)
      def sort(limit: Int) = Arrays.sort(buffer, 0, limit)
    }
  }
}


trait Sorting[Dataset[_], Resultset[_]] {
  def sort[A](values: Dataset[A], filePrefix: String, memoId: Int)(implicit buffering: Buffering[A], fs: SortSerialization[A]): Resultset[A]
}

class IteratorSorting(sortConfig: SortConfig) extends Sorting[Iterator, Iterable] {
  def sort[A](values: Iterator[A], filePrefix: String, memoId: Int)(implicit buffering: Buffering[A], fs: SortSerialization[A]): Iterable[A] = {
    import java.io.File
    import java.util.{PriorityQueue, Comparator, Arrays}

    val buf = buffering.newBuffer(sortConfig.sortBufferSize)

    def sortFile(i: Int) = new File(sortConfig.sortWorkDir, filePrefix + "." + memoId + "." + i)


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

    // first buffer up to the limit of the iterator, so that we can stay in-memory if possible
    val limit = buf.bufferChunk(values)
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
      val chunkFiles = writeChunked(Vector(initialChunkFile), values)
      new Iterable[A] {
        def iterator = 
          mergeIterator {
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
  }
}

// vim: set ts=4 sw=4 et:
