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

import org.apache.commons.collections.primitives._

import java.nio._

import scalaz._
import scalaz.Scalaz._

import scalaz.Ordering._

import scalaz.iteratee._
import scalaz.iteratee.Input._
import scalaz.iteratee.IterateeT._
import scalaz.iteratee.StepT._

trait TableChunk { self =>
  import TableChunk.TableChunkSchema

  def schema: TableChunkSchema

  def iterator: RowIterator

  def isEmpty: Boolean
}

object TableChunk {
  type TableChunkSchema = Seq[(CPath, CType)]

  def empty(schema1: TableChunkSchema): TableChunk = new TableChunk {
    def schema = schema1
    
    def iterator = RowIterator.empty(schema)
    
    def isEmpty = true
  }
}

trait ColumnBuffer {
  def append(it: RowState, idx: Int): Unit

  def clear: Unit
}

class ColumnBufferInt extends ColumnBuffer {
  val ints = new ArrayIntList()

  def append(it: RowState, idx: Int): Unit = {
    ints.add(it.intAt(idx))
  }

  def intAt(idx: Int) = ints.get(idx)

  def clear = ints.clear
}

trait TableChunkBuffer extends TableChunk {
  import TableChunk.TableChunkSchema

  val schema: TableChunkSchema

  final val isEmpty: Boolean = length == 0

  def iterator: RowIterator

  private[yggdrasil] val columns: Array[ColumnBuffer]

  var length : Int

  def clear: Unit
  
  def append(rowStates: RowState*): Unit
} 

object TableChunkBuffer {
  import TableChunk.TableChunkSchema

  def apply(schema: TableChunkSchema): TableChunkBuffer = TableChunkBufferImpl(schema)
}

private case class TableChunkBufferImpl(final val idCount : Int, schema: Seq[(CPath, CType)]) extends TableChunkBuffer {
  final private val ids = new Array[ArrayLongList](idCount)
  final private val columns = new Array[ColumnBuffer](schema.length)
  var length = 0

  // Constructor setup
  private var i = 0

  while (i < schema.length) {
    val ctype = schema(i)._2

    ctype match {
      case CInt => columns(i) = new ColumnBufferInt
      case _    => sys.error("Not yet supported")
    }

    i += 1
  }
  // End constructor

  def iterator = new RowIterator {
    private final var currentPos = 0

    final def isValid = currentPos > 0 && currentPos < length
    final def advance(i: Int) = if (currentPos + i < length) { currentPos += i; true } else false

    final val idCount = ids.length
    final val valueCount = columns.length

  }

  def clear: Unit = {
    length = 0
    var i = 0
    while (i < columns.length) {
      columns(i).clear
      i += 1
    }
  }

  def append(rowStates: RowState*): Unit = {
    length += 1
    var stateIndex, idIndex, valueIndex = 0
    while (stateIndex < rowStates.length) {
      val it = rowStates(stateIndex)

      var i = 0
      while (i < it.idCount) {
        ids(idIndex).add(it.idAt(i))
        idIndex += 1
        i += 1
      }

      var j = 0
      while (j < it.valueCount) {
        columns(valueIndex).append(it, j)
        valueIndex += 1
        j += 1
      }
    }
  }
}



sealed trait CogroupState

case class ReadBoth(leftBuffer: TableChunkBuffer, rightBuffer: TableChunkBuffer) extends CogroupState
case class ReadLeft(leftBuffer: TableChunkBuffer, rightBuffer: TableChunkBuffer, rightRemain: Option[RowIterator]) extends CogroupState
case class ReadRight(leftBuffer: TableChunkBuffer, leftRemain: Option[RowIterator], rightBuffer: TableChunkBuffer) extends CogroupState
case object Exhausted extends CogroupState


trait Table {
  def iterator: Iterator[TableChunk]
}

object TableOps {
  import TableChunk.TableChunkSchema
  def join[X, F[_]](leftSchema: TableChunkSchema, rightSchema: TableChunkSchema)(implicit M: Monad[F], order: Order[RowIterator]): Enumeratee2T[X, TableChunk, TableChunk, TableChunk, F] =
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
            rightHasMore = rightIter.advance(1)
          }

          leftHasMore = leftIter.advance(1)
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
            rightHasMore = rightIter.advance(1)
          }

          val rightCompareIter = rightBuffer.iterator

          while (leftHasMore && order.order(rightCompareIter, leftIter) == EQ) {
            leftBuffer.append(leftIter)
            leftHasMore = leftIter.advance(1)
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
              
              leftHasMore = leftIter.advance(1)
              while (leftHasMore && order.order(leftIter, rightIter) == EQ) {
                leftBuffer.append(leftIter)
                leftHasMore = leftIter.advance(1)
              }

              val leftBufIter = leftBuffer.iterator

              rightHasMore = rightIter.advance(1)
              while (rightHasMore && order.order(leftBufIter, rightIter) == EQ) {
                rightBuffer.append(rightIter)
                rightHasMore = rightIter.advance(1)
              }

              if (leftHasMore && rightHasMore) {
                // can do an immediate cartesian here because we found complete spans withing the two sides (hit no boundaries)
                cartesian(leftBuffer, rightBuffer, resultBuffer)
              }
            }
            case LT => leftHasMore = leftIter.advance(1)
            case GT => rightHasMore = rightIter.advance(1)
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
      def runOneSide[A](contf: ContFunc[A], iterA: RowIterator, bufferA: TableChunkBuffer, bufferB: TableChunkBuffer, _done: IterateeT[X, TableChunk, IterateeM, StepM[A]], _exhaustedCartesian: IterateeT[X, TableChunk, IterateeM, StepM[A]], needsMoreInputState: CogroupState) = {
        val iterB = bufferB.iterator
        var existsA = true

        while (existsA && order.order(iterA, iterB) == LT) { existsA = iterA.advance(1) }

        while (existsA && order.order(iterA, iterB) == EQ) {
          bufferA.append(iterA)
          existsA = iterA.advance(1)
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

      def process[A](s: StepM[A], contf: ContFunc[A], leftOpt: Option[RowIterator], leftBuffer: TableChunkBuffer, rightOpt: Option[RowIterator], rightBuffer: TableChunkBuffer, order: Order[RowIterator]): IterateeT[X, TableChunk, IterateeM, StepM[A]] = {
        val _done = done[X, TableChunk, IterateeM, StepM[A]](s, eofInput)
        def _exhaustedCartesian = {
          val results = TableChunkBuffer(leftBuffer.schema ++ rightBuffer.schema)
          iterateeT[X, TableChunk, IterateeM, StepM[A]](contf(elInput(cartesian(leftBuffer, rightBuffer, results))) >>== (step(_, Exhausted, order).value))
        }

        (leftOpt, leftBuffer.isEmpty, rightOpt, rightBuffer.isEmpty) match {
          // We're finished when we hit EOF on a side and we don't have a span to process on that side
          case (None, true, _, _) => _done
          case (_, _, None, true) => _done

          // Next simplest case: EOF on both sides, but spans on both sides
          case (None, false, None, false) => _exhaustedCartesian

          // No spans on either side, just run the iterators to produce result + new state
          // EOF + span on one side, chunk on the other means check to see if it matches and fill its span if it does, then cross the spans if they're both non-empty
          case (None, false, Some(right), _) => runOneSide(contf, right, rightBuffer, leftBuffer, _done, _exhaustedCartesian, ReadRight(leftBuffer, None, rightBuffer))
          case (Some(left), _, None, false) => runOneSide(contf, left, leftBuffer, rightBuffer, _done, _exhaustedCartesian, ReadLeft(leftBuffer, rightBuffer, None))

          // In all other cases, run innerGroup to complete any existing spans and compute new state
          case (Some(left), _, Some(right), _) => {
            val resultBuffer = TableChunkBuffer(left.schema ++ right.schema)

            val (result, nextState) = innerGroup(resultBuffer, left,  leftBuffer, right, rightBuffer, order)

            iterateeT[X, TableChunk, IterateeM, StepM[A]](contf(if (result.isEmpty) emptyInput else elInput(result)) >>== (step(_, nextState, order).value))
          }
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
                  a        <- process(s, contf, leftOpt.map(_.iterator), leftBuffer, rightOpt.map(_.iterator), rightBuffer, order)
                } yield a
              }
              case ReadLeft(leftBuffer, rightBuffer, rightRemain) => {
                for {
                  leftOpt  <- head[X, TableChunk, IterateeM]
                  a        <- process(s, contf, leftOpt.map(_.iterator), leftBuffer, rightRemain, rightBuffer, order)
                } yield a                
              }
              case ReadRight(leftBuffer, leftRemain, rightBuffer) => {
                for {
                  rightOpt <- head[X, TableChunk, F].liftI[TableChunk]
                  a        <- process(s, contf, leftRemain, leftBuffer, rightOpt.map(_.iterator), rightBuffer, order)
                } yield a
              }
            }
          },
          done = (a, r) => done[X, TableChunk, IterateeM, StepM[A]](sdone(a, if (r.isEof) eofInput else emptyInput), if (r.isEof) eofInput else emptyInput),
          err  = x => err[X, TableChunk, IterateeM, StepM[A]](x)
        )
      }

      def apply[A] = {
        step[A](_, ReadBoth(TableChunkBuffer(leftSchema), TableChunkBuffer(rightSchema)), order)
      }
    }
}

// vim: set ts=4 sw=4 et:
