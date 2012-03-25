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

trait Sorting[Dataset[_], Resultset[_]] {
  def sort[E](values: Dataset[E], filePrefix: String, memoId: Int)(implicit order: Order[E], cm: ClassManifest[E], fs: SortSerialization[E]): Resultset[E]
}

class IteratorSorting(sortConfig: SortConfig) extends Sorting[Iterator, Iterable] {
  def sort[E](values: Iterator[E], filePrefix: String, memoId: Int)(implicit order: Order[E], cm: ClassManifest[E], fs: SortSerialization[E]): Iterable[E] = {
    import java.io.File
    import java.util.{PriorityQueue, Comparator, Arrays}

    val javaOrder = order.toJavaComparator

    def sortFile(i: Int) = new File(sortConfig.sortWorkDir, filePrefix + "." + i)

    val buffer = new Array[E](sortConfig.sortBufferSize)

    def writeSortedChunk(chunkId: Int, v: Iterator[E]): File = {
      @tailrec def insert(j: Int, iter: Iterator[E]): Int = {
        if (j < buffer.length && iter.hasNext) {
          buffer(j) = iter.next()
          insert(j + 1, iter)
        } else j
      }

      insert(0, v)

      Arrays.sort(buffer.asInstanceOf[Array[AnyRef]], javaOrder.asInstanceOf[Comparator[AnyRef]])
      val chunkFile = sortFile(chunkId)
      using(fs.oStream(chunkFile)) { fs.write(_, buffer) }
      chunkFile
    }

    @tailrec def writeChunked(files: Vector[File]): Vector[File] = {
      if (values.hasNext) writeChunked(files :+ writeSortedChunk(files.length, values))
      else files
    }

    def mergeIterator(toMerge: Vector[(Iterator[E], () => Unit)]): Iterator[E] = {
      class Cell(iter: Iterator[E]) {
        var value: E = iter.next
        def advance(): Unit = {
          value = if (iter.hasNext) iter.next else null.asInstanceOf[E]
          this
        }
      }

      val cellComparator = order.contramap((c: Cell) => c.value).toJavaComparator
      val (streams, closes) = toMerge.unzip
      
      // creating a priority queue of size 0 will cause an NPE
      if (streams.size == 0) {
        Iterator.empty
      } else {
        new Iterator[E] {
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

    val chunkFiles = writeChunked(Vector.empty[File])

    new Iterable[E] {
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

// vim: set ts=4 sw=4 et:
