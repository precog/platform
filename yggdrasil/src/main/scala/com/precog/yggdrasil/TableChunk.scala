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


type TableChunkSchema = Seq[(CPath, CType)]

trait ColumnBuffer {
  def ints: IntBuffer

  def bools: BoolBuffer

  def append(it: RowState, idx: Int): Unit

  def clear: Unit
  // ...
}

class ColumnBufferInt extends ColumnBuffer {
  val ints = new ArrayListInt

  def append(it: RowState, idx: Int): Unit = {
    ints.append(it.intAt(idx))
  }

  def clear = ints.clear
}

trait TableChunkBuffer extends TableChunk {
  def isEmpty: Boolean

  def iterator: RowIterator
  def last: RowIterator

  private[yggdrasil] val columns: Array[ColumnBuffer]
  var length : Int

  def clear: Unit
  
  def append(rowStates: RowState*): TableChunkBuffer
} 

private case class TableChunkBufferImpl(schema: TableChunkSchema) extends TableChunkBuffer {

  val columns = new Array[ColumnBuffer]
  var length = 0

  private var i = 0

  while (i < schema.length) {
    val ctype = schema(i)._2

    ctype match {
      case CInt => columns(i) = new ColumnBufferInt
      ...
    }

    ++i
  }

  def clear: Unit = {
    length = 0
    var i = 0
    while (i < columns.length) {
      columns(i).clear
      ++i
    }
  }

  def append(rowStates: RowState*): TableChunkBuffer = {
    length += 1
    var i, j = 0
    while (i < rowStates.length) {
      val it = rowStates(i)
      var k = 0
      while (k < it.schema.length) {
        columns(j).append(it, k)
        ++j
        ++k
      }
    }
  }
}

object TableChunkBuffer {
  val empty = new TableChunkBuffer {
    val isEmpty = true
  }

  def apply(schema: TableChunkSchema) = TableChunkBufferImpl(schema)
}

sealed trait CogroupState

case class ReadBoth(leftBuffer: TableChunkBuffer, rightBuffer: TableChunkBuffer) extends CogroupState
case class ReadLeft(leftBuffer: TableChunkBuffer, rightBuffer: TableChunkBuffer, rightRemain: Option[RowIterator]) extends CogroupState
case class ReadRight(leftBuffer: TableChunkBuffer, leftRemain: Option[RowIterator], rightBuffer: TableChunkBuffer) extends CogroupState
case object Exhausted extends CogroupState

trait TableChunk { self =>
  def schema: TableChunkSchema

  def iterator: RowIterator

  def isEmpty: Boolean

  // Runtime exception if types are different
  def concat(chunk: TableChunk): TableChunk
}

case class RowIterTableChunk(iter: RowIterator) extends TableChunk {

object TableChunk {
  def empty(schema1: TableChunkSchema): TableChunk = {
    def schema = schema1
    
    def iterator = new RowIteratorEmpty(schema)
    
    def isEmpty = true
  }

  def builder(schema: TableChunkSchema): TableChunkBuilder = new {
  }
}

trait Table {
  def iterator: Iterator[TableChunk]
}

object TableOps {
  def join[X, F[_]](implicit M: Monad[F], order: Order[RowIterator]): Enumeratee2T[X, TableChunk, TableChunk, TableChunk, F] =
    new Enumeratee2T[X, TableChunk, TableChunk, TableChunk, F] {
      type ContFunc[A] = Input[TableChunk] => IterateeT[X, TableChunk, F, A]

      def cartesian(left: TableChunkBuffer, right: TableChunkBuffer, result: TableChunkBuffer): TableChunkBuffer = {
        val leftIter = left.iterator
        
        var leftHasMore = true
        var rightHasMore = true

        while (leftHasMore) {
          rightHasMore = true
          val rightIter = right.iterator
          
          while (rightHasMore) {
            result.append(leftIter, rightIter)
            rightHasMore = rightIter.advance
          }

          leftHasMore = leftIter.advance
        }

        // Finished with this data, so reset the buffers
        left.clear
        right.clear 

        result
      }

      // This method is responsible for grouping two chunks up to the point where one or both is exhausted
      def innerGroup(resultBuffer: TableChunkBuffer, leftIter: RowIterator, leftBuffer: TableChunkBuffer, rightIter: RowIterator, rightBuffer: TableChunkBuffer, order: Order[RowIterator]): (TableChunk, CogroupState) = {
        var leftHasMore = true
        var rightHasMore = true

        // If we have existing spans, see if we can continue filling them
        if (! leftBuffer.isEmpty) {
          val leftCompareIter = leftBuffer.iterator

          while (rightHasMore && order.order(leftCompareIter, rightIter) == EQ) {
            rightBuffer.append(rightIter)
            rightHasMore = rightIter.advance
          }

          val rightCompareIter = rightBuffer.iterator

          while (leftHasMore && order.order(rightCompareIter, leftIter) == EQ) {
            leftBuffer.append(leftIter)
            leftHasMore = leftIter.advance
          }

          if (leftHasMore && rightHasMore) {
            // can do an immediate cartesian here because we found complete spans withing the two sides (hit no boundaries)
            cartesian(leftBuffer, rightBuffer, resultBuffer)
          }
        }

        while (leftHasMore && rightHasMore) {          
          order.order(leftIter, rightIter) match {
            case EQ => {
              // Read in as much of the span on either side as possible
              leftBuffer.append(leftIter)
              rightBuffer.append(rightIter)
              
              leftHasMore = leftIter.advance
              while (leftHasMore && order.order(leftIter, rightIter) == EQ) {
                leftBuffer.append(leftIter)
                leftHasMore = leftIter.advance
              }

              val leftBufIter = leftBuffer.iterator

              rightHasMore = rightIter.advance
              while (rightHasMore && order.order(leftBufIter, rightIter) == EQ) {
                rightBuffer.append(rightIter)
                rightHasMore = rightIter.advance
              }

              if (leftHasMore && rightHasMore) {
                // can do an immediate cartesian here because we found complete spans withing the two sides (hit no boundaries)
                cartesian(leftBuffer, rightBuffer, resultBuffer)
              }
            }
            case LT => leftHasMore = leftIter.advance
            case GT => rightHasMore = rightIter.advance
          }
        }

        /* At this point we've either run out of inputs on the left or right. Depending on where we have remaining input we
         * need to create a new CogroupState based on the buffers. */
        val newState = if (leftHasMore) {
          // we ran out on the right
          ReadRight(leftBuffer, Some(leftIter), rightBuffer)
        } else if (rightHasMore) {
          // we ran out on the left
          ReadLeft(leftBuffer, rightBuffer, Some(rightIter))
        } else {
          // Ran out on both sides (two spans)
          ReadBoth(leftBuffer, rightBuffer)
        }

        (resultBuffer, newState)
      }

      // Iterate on iterA, filling bufferA as long as it has elements equal to bufferB's elements, then process accordingly
      def runOneSide[A](contf: ContFunc[A], iterA: RowIterator, bufferA: TableChunkBuffer, bufferB: TableChunkBuffer, needsMoreInputState: CogroupState) = {
        val iterB = bufferB.iterator
        var existsA = true

        while (existsA && order.order(iterA, iterB) == LT) { existsA = iterA.advance }

        while (rightExists && order.order(iterA, iterB) == EQ) {
          bufferA.append(iterA)
          iterA = iterA.advance
        }

        if (existsA) {
          if (bufferA.isEmpty) {
            // Never found a span
            _done
          } else {
            // Found a terminal span
            _exhaustedCartesian
          }
        } else {
          // Ran out of right input, so ask for more
          iterateeT[X, TableChunk, IterateeM, StepM[A]](contf(emptyInput) >>== (step(_, needsMoreInputState, order).value))
        }
      }

       def process[A](s: StepM[A], contf: ContFunc[A], leftOpt: Option[TableChunk], leftBuffer: TableChunkBuffer, rightOpt: Option[TableChunk], rightBuffer: TableChunkBuffer, order: Order[RowIterator]): IterateeT[X, TableChunk, IterateeM, StepM[A]] = 
          val _done = done[X, TableChunk, IterateeM, StepM[A]](s, eofInput)
          def _exhaustedCartesian = {
            val results = TableChunkBuffer(leftBuffer.schema ++ rightBuffer.schema)
            iterateeT[X, TableChunk, IterateeM, StepM[A]](contf(elInput(cartesian(leftBuffer, rightBuffer, results)) >>== (step(_, Exhausted, order).value)))
          }

          (leftOpt, leftBuffer.isEmpty, rightOpt, rightBuffer.isEmpty) match {
            // We're finished when we hit EOF on a side and we don't have a span to process on that side
            case (None, true, _, _) => _done
            case (_, _, None, true) => _done

            // Next simplest case: EOF on both sides, but spans on both sides
            case (None, false, None, false) => _exhaustedCartesian

            // No spans on either side, just run the iterators to produce result + new state
            // EOF + span on one side, chunk on the other means check to see if it matches and fill its span if it does, then cross the spans if they're both non-empty
            case (None, false, Some(right), _) => runOneSide(contf, right.iterator, rightBuffer, leftBuffer, ReadRight(leftBuffer, None, rightBuffer))
            case (Some(left), _, None, false) => runOneSide(contf, left.iterator, leftBuffer, rightBuffer, ReadLeft(leftBuffer, rightBuffer, None))

            // In all other cases, run innerGroup to complete any existing spans and compute new state
            case (Some(left), _, Some(right), _) => {
              val resultBuffer = TableChunkBuffer(left.schema ++ right.schema)

              val (result, nextState) = innerGroup(resultBuffer, left.iterator,  leftBuffer, right.iterator, rightBuffer, order)

              iterateeT[X, TableChunk, IterateeM, StepM[A]](contf(if (result.isEmpty) emptyInput else elInput(result)) >>== (step(_, nextState, order).value)))
            }
          } 

      def step[A](s: StepM[A], state: CogroupState, order: Order[RowIterator]): IterateeT[X, TableChunk, IterateeM, StepM[A]] = {
        s.fold[IterateeT[X, TableChunk, IterateeM, StepM[A]]](
          cont = contf => {
            // Conditionally read from both sides based on state
            state match {
              case Exhausted => done[X, TableChunk, IterateeM, StepM[A]](s, eofInput)
              case ReadBoth(leftBuffer, rightBuffer) => {
                for {
                  leftOpt  <- head[X, TableChunk, IterateeM]
                  rightOpt <- head[X, TableChunk, F].liftI[TableChunk]
                  a        <- process(s, contf, leftOpt, leftBuffer, rightOpt, rightBuffer, order)
                } yield a
              }
              case ReadLeft(leftBuffer, rightBuffer, rightRemain) => {
                for {
                  leftOpt  <- head[X, TableChunk, IterateeM]
                  a        <- process(s, contf, leftOpt, leftBuffer, rightRemain, rightBuffer, order)
                } yield a                
              }
              case ReadRight(leftBuffer, leftRemain, rightBuffer) => {
                for {
                  rightOpt <- head[X, TableChunk, F].liftI[TableChunk]
                  a        <- process(s, contf, leftRemain, leftBuffer, rightOpt, rightBuffer, order)
                } yield a
              }
            }
          },
          done = (a, r) => done[X, TableChunk, IterateeM, StepM[A]](sdone(a, if (r.isEof) eofInput else emptyInput), if (r.isEof) eofInput else emptyInput),
          err  = x => err[X, TableChunk, IterateeM, StepM[A]](x)
        )
      }

      def apply[A] = {
        step[A](_, (ArrayBuffer(),ArrayBuffer()), jOrder, kOrder)
      }
    }
}

// vim: set ts=4 sw=4 et:
