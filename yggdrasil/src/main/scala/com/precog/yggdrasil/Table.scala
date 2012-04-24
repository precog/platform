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
    case object Cartesian extends CogroupState
    case object Done extends CogroupState

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
            if (state != Done) println((state, leftSlice.toString(leftIdx), rightSlice.toString(rightIdx)))
            state match {
              case StepLeftCheckRight => 
                if (leftIdx < leftSlice.size) {
                  leftSlice.compareIdentityPrefix(rightSlice, prefixLength, leftIdx, rightIdx) match {
                    case LT => 
                      bufferLeft()
                      leftIdx += 1

                    case GT =>
                      bufferRight()
                      rightIdx += 1
                      state = StepRightCheckLeft

                    case EQ =>
                      bufferBoth()
                      state = LastEqual
                  }

                  precomputeNext()
                } else {
                  val result = emitSlice()
                  if (leftIter.hasNext) {
                    leftSlice = leftIter.next 
                    leftIdx = 0
                  } else {
                    state = StepRightDoneLeft
                  }

                  result
                }

              case StepLeftDoneRight =>
                if (leftIdx < leftSlice.size) {
                  bufferLeft()
                  leftIdx += 1
                  precomputeNext()
                } else {
                  val result = emitSlice()
                  if (leftIter.hasNext) {
                    leftSlice = leftIter.next 
                    leftIdx = 0
                  } else {
                    state = Done
                  }

                  result
                }

              case StepRightCheckLeft => 
                if (rightIdx < rightSlice.size) {
                  leftSlice.compareIdentityPrefix(rightSlice, prefixLength, leftIdx, rightIdx) match {
                    case LT => 
                      bufferLeft()
                      leftIdx += 1
                      state = StepLeftCheckRight

                    case GT =>
                      bufferRight()
                      rightIdx += 1

                    case EQ =>
                      bufferBoth()
                      state = LastEqual
                  }

                  precomputeNext()
                } else {
                  val result = emitSlice()
                  if (rightIter.hasNext) {
                    rightSlice = rightIter.next 
                    rightIdx = 0
                  } else {
                    state = StepLeftDoneRight
                  }

                  result
                }

              case StepRightDoneLeft =>
                if (rightIdx < rightSlice.size) {
                  bufferRight()
                  rightIdx += 1
                  precomputeNext()
                } else {
                  val result = emitSlice()
                  if (rightIter.hasNext) {
                    rightSlice = rightIter.next 
                    rightIdx = 0
                  } else {
                    state = Done
                  }

                  result
                }

              case LastEqual =>
                firstRightEq = rightIdx
                rightIdx += 1

                if (rightIdx < rightSlice.size) {
                  leftSlice.compareIdentityPrefix(rightSlice, prefixLength, leftIdx, rightIdx) match {
                    case LT => 
                      // there is potentially a run on the left; when we reach the end of the run we need
                      // to readvance the right
                      leftIdx += 1
                      rightIdx -= 1
                      state = RunLeft

                    case GT =>
                      sys.error("Inputs on the right not sorted.")
                      
                    case EQ => 
                      bufferBoth()
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

              case RunLeft =>
                if (leftIdx < leftSlice.size) {
                  leftSlice.compareIdentityPrefix(rightSlice, prefixLength, leftIdx, rightIdx) match {
                    case LT => 
                      sys.error("Inputs on the left not sorted") 

                    case GT => 
                      rightIdx += 1
                      state = StepLeftCheckRight

                    case EQ => 
                      bufferBoth()
                      leftIdx += 1
                  }
                } else {
                  if (leftIter.hasNext) {
                    leftIdx = 0
                    leftSlice = leftIter.next
                  } else {
                    state = StepRightDoneLeft
                  }
                } 

                precomputeNext()

              case Cartesian =>
                rightIdx += 1
                while (rightIdx >= rightSlice.size && rightIter.hasNext) rightSlice = (rightSlice append rightIter.next)
                if (rightIdx >= rightSlice.size) {
                  rightIdx = firstRightEq
                  leftIdx += 1
                  while (leftIdx >= leftSlice.size && leftIter.hasNext) leftSlice = (leftSlice append leftIter.next)
                  if (leftIdx >= leftSlice.size) {
                    state = Done
                  }
                }

                if (state != Done) {
                  leftSlice.compareIdentityPrefix(rightSlice, prefixLength, leftIdx, rightIdx) match {
                    case LT => 
                      nextRight = rightIdx
                      rightIdx = firstRightEq
                      leftIdx += 1
                      while (leftIdx >= leftSlice.size && leftIter.hasNext) leftSlice = (leftSlice append leftIter.next)
                      
                      if (leftIdx < leftSlice.size) {
                        leftSlice.compareIdentityPrefix(rightSlice, prefixLength, leftIdx, rightIdx) match {
                          case LT => sys.error("Inputs on the left not sorted")
                          case GT => 
                            if (leftBuffer.size > leftSlice.size) {
                              val result = emitSlice()
                              rightIdx = nextRight
                              state = StepLeftCheckRight
                              result
                            } else {
                              rightIdx = nextRight
                              state = StepLeftCheckRight
                              precomputeNext()
                            }
                          
                          case EQ => 
                            bufferBoth()
                            precomputeNext()
                        }
                      } else {
                        if (leftBuffer.size > leftSlice.size) {
                          val result = emitSlice() 
                          rightIdx = nextRight
                          state = StepRightDoneLeft
                          result
                        } else {
                          rightIdx = nextRight
                          state = StepRightDoneLeft
                          precomputeNext()
                        }
                      }

                    case GT =>
                      sys.error("Inputs on the right not sorted.")
                      
                    case EQ => 
                      bufferBoth()
                      precomputeNext()
                  }
                } else {
                  precomputeNext()
                }

              case Done => emitSlice()
            }
          }

          private def bufferLeft(): Unit = {
            println("Buffering left")
            leftBuffer += leftIdx
            rightBuffer += -1
          }

          private def bufferRight(): Unit = {
            println("Buffering right")
            leftBuffer += -1
            rightBuffer += rightIdx
          }

          private def bufferBoth(): Unit = {
            println("Buffering equal")
            leftBuffer += leftIdx
            rightBuffer += rightIdx
          }

          private def emitSlice(): Slice = {
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
