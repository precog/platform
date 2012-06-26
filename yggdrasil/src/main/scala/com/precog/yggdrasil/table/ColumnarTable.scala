package com.precog.yggdrasil
package table

import com.precog.common.VectorCase

import blueeyes.json._
import blueeyes.json.JsonAST._
import org.apache.commons.collections.primitives.ArrayIntList

import java.lang.ref.SoftReference

import scala.collection.mutable.ArrayBuffer
import scala.annotation.tailrec

import scalaz._
import scalaz.Ordering._

trait ColumnarTableModule extends TableModule {
  import trans._

  type F1 = CF1
  type F2 = CF2

  def liftF1(f: CValue => CValue): CF1 = sys.error("todo")

  class Table(val focus: Set[ColumnRef], val slices: Iterable[Slice]) extends TableLike { self  =>
    
    /**
     * Folds over the table to produce a single value (stored in a singleton table).
     */
    def reduce(scanner: Scanner[_, _, _]): Table = sys.error("todo")
    
    /**
     * Performs a one-pass transformation of the keys and values in the table.
     * If the key transform is not identity, the resulting table will have
     * unknown sort order.
     */
    def transform(spec: TransSpec1): Table = sys.error("todo")
    
    /**
     * Cogroups this table with another table, using equality on the specified
     * transformation on rows of the table.
     */
    def cogroup(leftKey: TransSpec1, rightKey: TransSpec1, that: Table)(left: TransSpec1, right: TransSpec1, both: TransSpec2): Table = sys.error("todo")
    
    /**
     * Performs a full cartesian cross on this table with the specified table,
     * applying the specified transformation to merge the two tables into
     * a single table.
     */
    def cross(that: Table)(spec: TransSpec2): Table = sys.error("todo")
    
    /**
     * Sorts the KV table by ascending or descending order of a transformation
     * applied to the rows.
     */
    def sort(sortKey: TransSpec1, sortOrder: SortOrder): Table = sys.error("todo")
    
    // Does this have to be fully known at every point in time?
    def schema: JType = sys.error("todo")
    
    def drop(n: Int): Table = sys.error("todo")
    
    def take(n: Int): Table = sys.error("todo")
    
    def takeRight(n: Int): Table = sys.error("todo")

    def normalize: Table = new Table(focus, slices.filterNot(_.isEmpty))

  /*
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
        focus ++ other.focus,
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

            private var leftBuffer = new ArrayIntList(maxSliceSize)
            private var rightBuffer = new ArrayIntList(maxSliceSize)

            private var state: CogroupState =
              if (leftSlice == null) {
                if (rightSlice == null) Done else StepRightDoneLeft
              } else {
                if (rightSlice == null) StepLeftDoneRight else StepLeftCheckRight
              }

            private var curSlice = precomputeNext()

            def hasNext: Boolean = curSlice ne null
            
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
                    if (result ne null) result else precomputeNext()
                  }

                case StepLeftDoneRight =>
                  if (nonEmptyLeftSlice) {
                    bufferAdvanceLeft()
                    precomputeNext()
                  } else {
                    val result = emitSliceOnOverflow(true, false, Done)
                    if (result ne null) result else precomputeNext()
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
                    if (result ne null) result else precomputeNext()
                  }

                case StepRightDoneLeft =>
                  if (nonEmptyRightSlice) {
                    bufferAdvanceRight()
                    precomputeNext()
                  } else {
                    val result = emitSliceOnOverflow(false, true, Done)
                    if (result ne null) result else precomputeNext()
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
                              if (result ne null) result else precomputeNext()
                            
                            case EQ => 
                              bufferBoth()
                              precomputeNext()
                          }
                        } else {
                          state = StepRightDoneLeft
                          rightIdx = nextRight
                          val result = emitSliceOnOverflow(false, false, null)
                          if (result ne null) result else precomputeNext()
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
                          if (result ne null) result else precomputeNext()
                            
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

            @inline private def bufferAdvanceLeft(): Unit = {
              leftBuffer.add(leftIdx)
              rightBuffer.add(-1)
              leftIdx += 1
            }

            @inline private def bufferAdvanceRight(): Unit = {
              leftBuffer.add(-1)
              rightBuffer.add(rightIdx)
              rightIdx += 1
            }

            @inline private def bufferBoth(): Unit = {
              leftBuffer.add(leftIdx)
              rightBuffer.add(rightIdx)
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
                private val remappedLeft  = leftSlice.remap(F1P.bufferRemap(leftBuffer))
                private val remappedRight = rightSlice.remap(F1P.bufferRemap(rightBuffer))

                val idCount = self.idCount + other.idCount
                val size = leftBuffer.size

                // merge identity columns
                val identities = {
                  val (li, lr) = remappedLeft.identities.splitAt(prefixLength)
                  val (ri, rr) = remappedRight.identities.splitAt(prefixLength)
                  val sharedPrefix = (li zip ri) map {
                    case (c1, c2) => new Column[Long] {
                      val returns = CLong
                      def isDefinedAt(row: Int) = c1.isDefinedAt(row) || c2.isDefinedAt(row)
                      def apply(row: Int) = {
                        if (c1.isDefinedAt(row)) c1(row) // identities must be equal, or c2 must be undefined
                        else if (c2.isDefinedAt(row)) c2(row) 
                        else sys.error("impossible")
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
                                lc(row)        
                              }
                            } else {
                              rc(row)
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
              leftBuffer = new ArrayIntList(maxSliceSize)
              rightBuffer = new ArrayIntList(maxSliceSize)
              result
            }
          }
        }
      )
    }
    */

    def toJson: Iterable[JValue] = toEvents.map(_._2)

    def toEvents: Iterable[(Identities, JValue)] = {
      toEvents { (slice: Slice, row: Int) => slice.toJson(row) }
    }

    def toValidatedEvents: Iterable[(Identities, ValidationNEL[Throwable, JValue])] = {
      toEvents { (slice: Slice, row: Int) => slice.toValidatedJson(row) }
    }

    private def toEvents[A](f: (Slice, Int) => A): Iterable[(Identities, A)] = {
      sys.error("todo")
      /*
      new Iterable[(Identities, A)] {
        def iterator = new Iterator[(Identities, A)] {
          private val iter = self.normalize.slices.iterator
          private var slice = if (iter.hasNext) iter.next else null.asInstanceOf[Slice]
          private var idx = 0
          private var _next = precomputeNext()

          def hasNext = _next ne null

          def next() = {
            val tmp = _next
            _next = precomputeNext()
            tmp
          }
         
          @tailrec def precomputeNext(): (Identities, A) = {
            if (slice == null) {
              null.asInstanceOf[(Identities, A)]
            } else if (idx < slice.size) {
              if (slice.identities.isEmpty || slice.identities.exists(_.isDefinedAt(idx))) {
                val result = (VectorCase(slice.identities map { c => if (c.isDefinedAt(idx)) c(idx) else -1L }: _*), f(slice, idx))
                idx += 1
                result
              } else {
                idx += 1
                precomputeNext()
              }
            } else {
              slice = if (iter.hasNext) iter.next else null.asInstanceOf[Slice]
              idx = 0
              precomputeNext() //recursive call is okay because hasNext must have returned true to get here
            }
          }
        }
      } */
    }
  }
}
// vim: set ts=4 sw=4 et:
