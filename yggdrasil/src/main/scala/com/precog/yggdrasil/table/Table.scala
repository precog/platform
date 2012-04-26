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
package table

import blueeyes.json.JsonAST._
import java.lang.ref.SoftReference

import scala.collection.mutable.ArrayBuffer
import scala.annotation.tailrec
import scalaz.Ordering._

class Table(val idCount: Int, val foci: Set[VColumnRef[_]], val slices: Iterable[Slice]) { self  =>
  def map(colId: VColumnId, nextRef: () => Long)(f: F1P[_, _]): Table = {
    val oldRef = VColumnRef(colId, f.accepts)
    val newId  = DynColumnId(nextRef())
    val newRef = VColumnRef(newId, f.returns)
    new Table(idCount, foci - oldRef + newRef, slices map { slice => slice.map(colId, newId)(f.toF1) })
  }

  def normalize: Table = new Table(idCount, foci, slices.filterNot(_.isEmpty))

  def cogroup(other: Table, prefixLength: Int)(merge: CogroupMerge): Table = {
    sealed trait CogroupState
    case object StepLeftCheckRight extends CogroupState
    case object StepLeftDoneRight extends CogroupState
    case object StepRightCheckLeft extends CogroupState
    case object StepRightDoneLeft extends CogroupState
    case object Cartesian extends CogroupState
    case object Done extends CogroupState

    new Table(
      idCount,
      foci ++ other.foci,
      new Iterable[Slice] {
        def iterator = new Iterator[Slice] {
          private val leftIter = self.slices.iterator
          private val rightIter = other.slices.iterator
          
          private var leftSlice = if (leftIter.hasNext) leftIter.next else null.asInstanceOf[Slice]
          private var rightSlice = if (rightIter.hasNext) rightIter.next else null.asInstanceOf[Slice]

          private var maxSliceSize = 
            if (leftSlice == null) { 
              if (rightSlice == null) 0 else rightSlice.size 
            } else { 
              if (rightSlice == null) leftSlice.size else leftSlice.size max rightSlice.size
            }

          private var leftIdx = 0 
          private var rightIdx = 0
          private var firstRightEq: Int = -1
          private var nextRight: Int = -1

          private var leftBuffer: ArrayBuffer[Int] = new ArrayBuffer[Int]()
          private var rightBuffer: ArrayBuffer[Int] = new ArrayBuffer[Int]()

          def bufferRemap(buf: ArrayBuffer[Int]): PartialFunction[Int, Int] = {
            case i if (i < buf.size) && buf(i) != -1 => buf(i)
          }

          private var state: CogroupState =
            if (leftSlice == null) {
              if (rightSlice == null) Done else StepRightDoneLeft
            } else {
              if (rightSlice == null) StepLeftDoneRight else StepLeftCheckRight
            }

          private var curSlice = precomputeNext()

          def hasNext: Boolean = curSlice != null
          
          def next: Slice = {
            val tmp = curSlice
            curSlice = precomputeNext()
            tmp
          }

          @tailrec private def precomputeNext(): Slice = {
            @inline 
            def nonEmptyLeftSlice = leftIdx < leftSlice.size

            @inline 
            def emptyLeftSlice = leftIdx >= leftSlice.size
            
            @inline 
            def nonEmptyRightSlice = rightIdx < rightSlice.size

            @inline 
            def emptyRightSlice = rightIdx >= rightSlice.size

            @inline 
            def compareIdentities = leftSlice.compareIdentityPrefix(rightSlice, prefixLength, leftIdx, rightIdx) 

            state match {
              case StepLeftCheckRight => 
                if (nonEmptyLeftSlice) {
                  compareIdentities match {
                    case LT => 
                      bufferAdvanceLeft()

                    case GT =>
                      bufferAdvanceRight()
                      state = StepRightCheckLeft

                    case EQ =>
                      bufferBoth()
                      firstRightEq = rightIdx
                      state = Cartesian
                  }

                  precomputeNext()
                } else {
                  val result = emitSliceOnOverflow(true, false, StepRightDoneLeft)
                  if (result != null) result else precomputeNext()
                }

              case StepLeftDoneRight =>
                if (nonEmptyLeftSlice) {
                  bufferAdvanceLeft()
                  precomputeNext()
                } else {
                  val result = emitSliceOnOverflow(true, false, Done)
                  if (result != null) result else precomputeNext()
                }

              case StepRightCheckLeft => 
                if (nonEmptyRightSlice) {
                  compareIdentities match {
                    case LT => 
                      bufferAdvanceLeft()
                      state = StepLeftCheckRight

                    case GT =>
                      bufferAdvanceRight()

                    case EQ =>
                      bufferBoth()
                      firstRightEq = rightIdx
                      state = Cartesian
                  }

                  precomputeNext()
                } else {
                  val result = emitSliceOnOverflow(false, true, StepLeftDoneRight)
                  if (result != null) result else precomputeNext()
                }

              case StepRightDoneLeft =>
                if (nonEmptyRightSlice) {
                  bufferAdvanceRight()
                  precomputeNext()
                } else {
                  val result = emitSliceOnOverflow(false, true, Done)
                  if (result != null) result else precomputeNext()
                }

              case Cartesian =>
                @inline 
                def ensureRightSliceNonEmpty(): Boolean = {
                  while (rightIdx >= rightSlice.size && rightIter.hasNext) rightSlice = (rightSlice append rightIter.next)
                  nonEmptyRightSlice
                }

                @inline 
                def ensureLeftSliceNonEmpty(): Boolean = {
                  while (leftIdx >= leftSlice.size && leftIter.hasNext) leftSlice = (leftSlice append leftIter.next)
                  nonEmptyLeftSlice
                }

                rightIdx += 1
                if (ensureRightSliceNonEmpty()) {
                  compareIdentities match {
                    case LT => 
                      nextRight = rightIdx
                      rightIdx = firstRightEq
                      leftIdx += 1

                      if (ensureLeftSliceNonEmpty()) {
                        compareIdentities match {
                          case LT => sys.error("Inputs on the left not sorted")
                          case GT => 
                            state = StepLeftCheckRight
                            rightIdx = nextRight
                            val result = emitSliceOnOverflow(false, false, null)
                            if (result != null) result else precomputeNext()
                          
                          case EQ => 
                            bufferBoth()
                            precomputeNext()
                        }
                      } else {
                        state = StepRightDoneLeft
                        rightIdx = nextRight
                        val result = emitSliceOnOverflow(false, false, null)
                        if (result != null) result else precomputeNext()
                      }

                    case GT => 
                      sys.error("Inputs on the right not sorted")

                    case EQ => 
                      bufferBoth()
                      precomputeNext()
                  }
                } else {
                  rightIdx = firstRightEq
                  leftIdx += 1
                  if (ensureLeftSliceNonEmpty()) {
                    compareIdentities match {
                      case LT => sys.error("Inputs on the left not sorted")
                      case GT => 
                        state = StepLeftDoneRight
                        val result = emitSliceOnOverflow(false, false, null)
                        if (result != null) result else precomputeNext()
                          
                      case EQ => 
                        bufferBoth()
                        precomputeNext()
                    }
                  } else {
                    state = Done
                    emitSlice()
                  }
                }

              case Done => null.asInstanceOf[Slice]
            }
          }

          private def bufferAdvanceLeft(): Unit = {
            leftBuffer += leftIdx
            rightBuffer += -1
            leftIdx += 1
          }

          private def bufferAdvanceRight(): Unit = {
            leftBuffer += -1
            rightBuffer += rightIdx
            rightIdx += 1
          }

          private def bufferBoth(): Unit = {
            leftBuffer += leftIdx
            rightBuffer += rightIdx
          }

          private def emitSliceOnOverflow(advanceLeft: Boolean, advanceRight: Boolean, advancingState: CogroupState): Slice = {
            if (leftBuffer.size >= maxSliceSize) {
              val result = emitSlice()

              if (advanceLeft) {
                if (leftIter.hasNext) {
                  leftSlice = leftIter.next
                  leftIdx = 0
                } else {
                  state = advancingState
                }
              } else if (advanceRight) {
                if (rightIter.hasNext) {
                  rightSlice = rightIter.next
                  rightIdx = 0
                } else {
                  state = advancingState
                }
              }

              result
            } else {
              if (advanceLeft) {
                if (leftIter.hasNext) {
                  leftSlice = leftSlice append leftIter.next
                  null
                } else {
                  state = advancingState
                  if (state == Done) emitSlice() else null
                }
              } else if (advanceRight) {
                if (rightIter.hasNext) {
                  rightSlice = rightSlice append rightIter.next
                  null
                } else {
                  state = advancingState
                  if (state == Done) emitSlice() else null
                }
              } else {
                null
              } 
            }
          }

          private def emitSlice(): Slice = {
            val result = new Slice {
              private val remappedLeft  = leftSlice.remap(bufferRemap(leftBuffer))
              private val remappedRight = rightSlice.remap(bufferRemap(rightBuffer))

              val idCount = self.idCount + other.idCount
              val size = leftBuffer.size
              val identities = {
                val (li, lr) = remappedLeft.identities.splitAt(prefixLength)
                val (ri, rr) = remappedRight.identities.splitAt(prefixLength)
                val sharedPrefix = (li zip ri) map {
                  case (c1, c2) => new Column[Long] {
                    val returns = CLong
                    def isDefinedAt(row: Int) = c1.isDefinedAt(row) || c2.isDefinedAt(row)
                    def apply(row: Int) = {
                      if (c1.isDefinedAt(row)) {
                        c1(row) // identities must be equal, or c2 must be undefined
                      } else {
                        if (c2.isDefinedAt(row)) c2(row) else sys.error("impossible")
                      }
                    }
                  }
                }

                sharedPrefix ++ lr ++ rr
              }

              val columns = remappedRight.columns.foldLeft(remappedLeft.columns) {
                case (acc, (rref, rcol)) => 
                  val mergef = merge(rref).get

                  acc.get(rref) match {
                    case None =>
                      acc + (rref -> rcol)

                    case Some(lcol) => acc + (rref -> (rref.ctype match {
                      case CBoolean => new Column[Boolean] {
                        private val lc = lcol.asInstanceOf[Column[Boolean]]
                        private val rc = rcol.asInstanceOf[Column[Boolean]]
                        private val mf = mergef.asInstanceOf[F2P[Boolean, Boolean, Boolean]]

                        val returns = CBoolean
                        
                        def isDefinedAt(row: Int) = lc.isDefinedAt(row) || rc.isDefinedAt(row)
                        
                        def apply(row: Int): Boolean = {
                          if (lc.isDefinedAt(row)) {
                            if (rc.isDefinedAt(row)) {
                              mf(lc(row), rc(row))
                            } else {
                              rc(row)        
                            }
                          } else {
                            lc(row)
                          }
                        }
                      }

                      case CLong    => new Column[Long] {
                        private val lc = lcol.asInstanceOf[Column[Long]]
                        private val rc = rcol.asInstanceOf[Column[Long]]
                        private val mf = mergef.asInstanceOf[F2P[Long, Long, Long]]

                        val returns = CLong

                        def isDefinedAt(row: Int) = lc.isDefinedAt(row) || rc.isDefinedAt(row)

                        def apply(row: Int): Long = {
                          if (lc.isDefinedAt(row)) {
                            if (rc.isDefinedAt(row)) {
                              mf(lc(row), rc(row))
                            } else {
                              rc(row)        
                            }
                          } else {
                            lc(row)
                          }
                        }
                      }

                      case CDouble  => new Column[Double] {
                        private val lc = lcol.asInstanceOf[Column[Double]]
                        private val rc = rcol.asInstanceOf[Column[Double]]
                        private val mf = mergef.asInstanceOf[F2P[Double, Double, Double]]

                        val returns = CDouble

                        def isDefinedAt(row: Int) = lc.isDefinedAt(row) || rc.isDefinedAt(row)

                        def apply(row: Int): Double = {
                          if (lc.isDefinedAt(row)) {
                            if (rc.isDefinedAt(row)) {
                              mf(lc(row), rc(row))
                            } else {
                              rc(row)        
                            }
                          } else {
                            lc(row)
                          }
                        }
                      }

                      case ctype    => new Column[ctype.CA] {
                        private val lc = lcol.asInstanceOf[Column[ctype.CA]]
                        private val rc = rcol.asInstanceOf[Column[ctype.CA]]
                        private val mf = mergef.asInstanceOf[F2P[ctype.CA, ctype.CA, ctype.CA]]

                        val returns: CType { type CA = ctype.CA } = ctype

                        def isDefinedAt(row: Int) = lc.isDefinedAt(row) || rc.isDefinedAt(row)

                        def apply(row: Int): ctype.CA = {
                          if (lc.isDefinedAt(row)) {
                            if (rc.isDefinedAt(row)) {
                              mf(lc(row), rc(row))
                            } else {
                              rc(row)        
                            }
                          } else {
                            lc(row)
                          }
                        }
                      }
                    }))
                  }
              }
            }

            leftSlice = leftSlice.split(leftIdx)._2
            rightSlice = rightSlice.split(rightIdx)._2
            leftIdx = 0
            rightIdx = 0
            leftBuffer = new ArrayBuffer[Int]()
            rightBuffer = new ArrayBuffer[Int]()
            result
          }
        }
      }
    )
  }

  def retain(refs: Set[ColumnRef]) = self.slices map { _.retain(refs) }

  def toJson: Iterable[JValue] = {
    new Iterable[JValue] {
      def iterator = new Iterator[JValue] {
        private val iter = self.slices.iterator
        private var slice = if (iter.hasNext) iter.next else null.asInstanceOf[Slice]
        private var idx = 0

        def hasNext = (slice != null && idx < slice.size) || iter.hasNext

        @tailrec def next() = {
          if (slice == null) {
            sys.error("next() called past end of iterator")
          } else if (idx < slice.size) {
            val result = slice.toJson(idx)
            idx += 1
            result
          } else {
            slice = if (iter.hasNext) iter.next else null.asInstanceOf[Slice]
            idx = 0
            next() //recursive call is okay because hasNext must have returned true to get here
          }
        }
      }
    }
  }
}
// vim: set ts=4 sw=4 et:
