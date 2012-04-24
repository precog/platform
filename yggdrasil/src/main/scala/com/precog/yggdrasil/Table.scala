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

  def cogroup(other: Table)(merge: CogroupMerge): Table = {
    sealed trait CogroupState
    case object StepLeftCheckRight extends CogroupState
    case object StepLeftDoneRight extends CogroupState
    case object StepRightCheckLeft extends CogroupState
    case object StepRightDoneLeft extends CogroupState
    case object LastEqual extends CogroupState
    case object RunLeft extends CogroupState
    case object Done extends CogroupState
    case object Cartesian extends CogroupState

    new Table(
      idCount,
      foci ++ other.foci,
      new Iterable[Slice] {
        def iterator = new Iterator[Slice] {
          private val prefixLength = self.idCount min other.idCount
          private val leftIter = self.slices.iterator
          private val rightIter = other.slices.iterator
          
          private var leftSlice = if (leftIter.hasNext) leftIter.next else null.asInstanceOf[Slice]
          private var rightSlice = if (rightIter.hasNext) rightIter.next else null.asInstanceOf[Slice]

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
            state match {
              case StepLeftCheckRight => 
                if (leftIdx < leftSlice.size) {
                  leftSlice.compareIdentityPrefix(rightSlice, prefixLength, leftIdx, rightIdx) match {
                    case LT => 
                      leftBuffer += leftIdx
                      rightBuffer += -1

                      leftIdx += 1

                    case GT =>
                      state = StepRightCheckLeft

                    case EQ =>
                      leftBuffer += leftIdx
                      rightBuffer += rightIdx
                      state = LastEqual
                  }

                  precomputeNext()
                } else {
                  val result = emit()
                  if (leftIter.hasNext) {
                    leftSlice = leftIter.next 
                    leftIdx = 0
                  } else {
                    state = StepRightDoneLeft
                  }
                  result
                }

              case LastEqual =>
                firstRightEq = rightIdx
                rightIdx += 1

                if (rightIdx < rightSlice.size) {
                  leftSlice.compareIdentityPrefix(rightSlice, prefixLength, leftIdx, rightIdx) match {
                    case LT => 
                      leftIdx += 1
                      state = StepLeftCheckRight

                    case GT =>
                      sys.error("Inputs on the right not sorted.")
                      
                    case EQ => 
                      leftBuffer += leftIdx
                      rightBuffer += rightIdx
                      rightIdx += 1
                      state = Cartesian
                  }
                } else {
                  if (rightIter.hasNext) {
                    rightSlice = (rightSlice append rightIter.next)
                  } else {
                    state = StepLeftDoneRight
                    leftIdx += 1
                  }
                }

                precomputeNext()

              case Cartesian =>
                leftSlice.compareIdentityPrefix(rightSlice, prefixLength, leftIdx, rightIdx) match {
                  case LT => 
                    nextRight = rightIdx
                    rightIdx = firstRightEq
                    leftIdx += 1
                    
                    if (leftIdx >= leftSlice.size && leftIter.hasNext) {
                      leftSlice = (leftSlice append leftIter.next)
                    
                      leftSlice.compareIdentityPrefix(rightSlice, prefixLength, leftIdx, rightIdx) match {
                        case LT => sys.error("Inputs on the left not sorted")
                        case GT => 
                          rightIdx = nextRight
                          state = StepLeftCheckRight
                          if (leftBuffer.size > leftSlice.size) emit() else precomputeNext()
                        
                        case EQ => 
                          leftBuffer += leftIdx
                          rightBuffer += rightIdx
                          rightIdx += 1
                          precomputeNext()
                      }
                    } else {
                      state = StepRightDoneLeft
                      if (leftBuffer.size > leftSlice.size) emit() else precomputeNext()
                    }

                  case GT =>
                    sys.error("Inputs on the right not sorted.")
                    
                  case EQ => 
                    leftBuffer += leftIdx
                    rightBuffer += rightIdx
                    rightIdx += 1

                    if (rightIdx >= rightSlice.size && rightIter.hasNext) {
                      rightSlice = rightSlice append rightIter.next
                    }

                    precomputeNext()
                }
              }
          }

          private def emit(): Slice = {
            val result = new Slice {
              private val remappedLeft  = leftSlice.remap(bufferRemap(leftBuffer))
              private val remappedRight = rightSlice.remap(bufferRemap(rightBuffer))

              val idCount = self.idCount + other.idCount
              val size = leftBuffer.size
              val identities = remappedLeft.identities ++ remappedRight.identities

              val columns = remappedRight.columns.foldLeft(remappedLeft.columns) {
                case (acc, (rref, rcol)) => 
                  val mergef = merge(rref).get
                  acc.get(rref) match {
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

  def toJson: Iterable[JValue] = {
    new Iterable[JValue] {
      def iterator = new Iterator[JValue] {
        private val iter = self.slices.iterator
        private var slice = if (iter.hasNext) iter.next else null.asInstanceOf[Slice]
        private var idx = 0
        private var _next: JValue = precomputeNext()

        def hasNext = _next != null

        def next = {
          val tmp = _next
          _next = precomputeNext()
          tmp
        }

        @tailrec private def precomputeNext(): JValue = {
          if (slice == null) {
            null.asInstanceOf[JValue]
          } else if (idx < slice.size) {
            val result = slice.columns.foldLeft[JValue](JNull) {
              case (jv, (ref @ VColumnRef(NamedColumnId(_, selector), ctype), col)) if (col.isDefinedAt(idx)) => 
                jv.set(selector, ctype.jvalueFor(col(idx)))

              case (jv, _) => jv
            }

            idx += 1
            result
          } else {
            slice = if (iter.hasNext) iter.next else null.asInstanceOf[Slice]
            idx = 0
            precomputeNext()
          }
        }
      }
    }
  }
}
// vim: set ts=4 sw=4 et:
