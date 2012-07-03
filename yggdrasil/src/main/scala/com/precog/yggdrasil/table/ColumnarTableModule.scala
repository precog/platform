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

import com.precog.common.VectorCase

import blueeyes.json._
import blueeyes.json.JsonAST._
import org.apache.commons.collections.primitives.ArrayIntList

import java.lang.ref.SoftReference

import scala.collection.BitSet
import scala.annotation.tailrec

import scalaz._
import scalaz.Ordering._
import scalaz.std.function._
import scalaz.syntax.arrow._

trait ColumnarTableModule extends TableModule {
  import trans._
  import trans.constants._
  import Schema._

  type F1 = CF1
  type F2 = CF2
  type Scanner = CScanner
  type Reducer[α] = CReducer[α]
  type RowId = Int

  def ops: TableOps = sys.error("todo")

  implicit def liftF1(f: F1) = new F1Like {
    def compose(f1: F1) = f compose f1
    def andThen(f1: F1) = f andThen f1
  }

  implicit def liftF2(f: F2) = new F2Like {
    def applyl(cv: CValue) = new CF1(f(Column.const(cv), _))
    def applyr(cv: CValue) = new CF1(f(_, Column.const(cv)))

    def andThen(f1: F1) = new CF2((c1, c2) => f(c1, c2) flatMap f1)
  }

  case class SliceTransform[A](initial: A, f: (A, Slice) => (A, Slice)) {
    def andThen[B](t: SliceTransform[B]): SliceTransform[(A, B)] = {
      SliceTransform(
        (initial, t.initial),
        { case ((a, b), s) => 
            val (a0, sa) = f(a, s) 
            val (b0, sb) = t.f(b, sa)
            ((a0, b0), sb)
        }
      )
    }

    def zip[B](t: SliceTransform[B])(combine: (Slice, Slice) => Slice): SliceTransform[(A, B)] = {
      SliceTransform(
        (initial, t.initial),
        { case ((a, b), s) =>
            val (a0, sa) = f(a, s)
            val (b0, sb) = t.f(b, s)
            assert(sa.size == sb.size)
            ((a0, b0), combine(sa, sb))
        }
      )
    }
  }

  object SliceTransform {
    def identity[A](initial: A) = SliceTransform(initial, (a: A, s: Slice) => (a, s))
  }

  class Table(val slices: Iterable[Slice]) extends TableLike { self  =>
    /**
     * Folds over the table to produce a single value (stored in a singleton table).
     */
    def reduce[A: Monoid](reducer: Reducer[A]): A = sys.error("todo")

    private def map0(f: Slice => Slice): SliceTransform[Unit] = SliceTransform[Unit]((), Function.untupled(f.second[Unit]))

    private def transform0[A](sliceTransform: SliceTransform[A]): Table = {
      new Table(
        new Iterable[Slice] {
          def iterator: Iterator[Slice] = {
            val baseIter = slices.iterator
            new Iterator[Slice] {
              private var state: A = sliceTransform.initial
              private var next0: Slice = precomputeNext()

              private def precomputeNext(): Slice = {
                if (baseIter.hasNext) {
                  val s = baseIter.next
                  val (nextState, s0) = sliceTransform.f(state, s)
                  state = nextState
                  s0
                } else {
                  null.asInstanceOf[Slice]
                }
              }

              def hasNext: Boolean = next0 != null
              def next: Slice = {
                val tmp = next0
                next0  = precomputeNext()
                tmp
              }
            }
          }
        }
      )
    }

    // No transform defined herein may reduce the size of a slice. Be it known!
    private def composeSliceTransform(spec: TransSpec1): SliceTransform[_] = {
      spec match {
        case Leaf(_) => SliceTransform.identity[Unit](())

        case Map1(source, f) => 
          composeSliceTransform(source) andThen {
             map0 { _ mapColumns f }
          }

        case Filter(source, predicate) => 
          composeSliceTransform(source).zip(composeSliceTransform(predicate)) { (s, filter) => 
            if (s.columns.isEmpty) {
              s
            } else {
              assert(filter.columns.nonEmpty)
              val definedAt = filter.columns.values.foldLeft(BitSet(0 until s.size: _*)) { (acc, col) =>
                cf.util.isSatisfied(col).map(_.definedAt(0, s.size) & acc).getOrElse(BitSet.empty) 
              }

              s mapColumns { cf.util.filter(0, s.size, definedAt) }
            }
          }
          // match the source table

        case DerefObjectStatic(source, field) =>
          composeSliceTransform(source) andThen {
            map0 { _ deref field }
          }

        case DerefArrayStatic(source, element) =>
          composeSliceTransform(source) andThen {
            map0 { _ deref element }
          }

        case Map2(left, right, f) =>
          val l0 = composeSliceTransform(left)
          val r0 = composeSliceTransform(right)

          l0.zip(r0) { (sl, sr) =>
            new Slice {
              val size = sl.size
              val columns: Map[ColumnRef, Column] = 
                (for {
                  cl <- sl.valueColumns
                  cr <- sr.valueColumns
                  col <- f(cl, cr) // TODO: Unify columns of the same result type
                } yield {
                  (ColumnRef(JPath.Identity, col.tpe), col)
                })(collection.breakOut)
            }
          }

        case Equal(left, right) =>
          val l0 = composeSliceTransform(left)
          val r0 = composeSliceTransform(right)

          l0.zip(r0) { (sl, sr) =>
            new Slice {
              val size = sl.size
              val columns: Map[ColumnRef, Column] = 
                if (sl.columns.keySet == sr.columns.keySet) {
                  val eqResult = sl.columns.keySet.foldLeft(Option.empty[Column]) { (acc, colRef) => 
                    val colsEq = cf.std.Eq(sl.columns(colRef), sr.columns(colRef))
                    acc flatMap { accCol => colsEq flatMap { cf.std.And(accCol, _) } } orElse colsEq
                  }

                  eqResult map { ColumnRef(JPath.Identity, CBoolean) -> _ } toMap
                } else {
                  Map(ColumnRef(JPath.Identity, CBoolean) -> new EmptyColumn[BoolColumn] with BoolColumn)
                }
            }
          }

        case WrapStatic(source, field) =>
          composeSliceTransform(source) andThen {
            map0 { _ wrap JPathField(field) }
          }

        case ObjectConcat(left, right) =>
          val l0 = composeSliceTransform(left)
          val r0 = composeSliceTransform(right)

          l0.zip(r0) { (sl, sr) =>
            new Slice {
              val size = sl.size
              val columns = 
                // left side first in the seq so that the right side wins
                (sl.columns.toSeq ++ sr.columns).foldLeft(Map.empty[ColumnRef, Column]) {
                  case (acc, (ref, col)) if ref.selector.head.exists(_.isInstanceOf[JPathField]) => acc + (ref -> col)
                  case (acc, _) => acc
                }
            }
          }

        case Typed(source, tpe) =>
          composeSliceTransform(source) andThen {
            map0 { _ typed tpe }
          }
      }
    }
    
    /**
     * Performs a one-pass transformation of the keys and values in the table.
     * If the key transform is not identity, the resulting table will have
     * unknown sort order.
     */
    def transform(spec: TransSpec1): Table = {
      transform0(composeSliceTransform(spec))
    }
    
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

    def normalize: Table = new Table(slices.filterNot(_.isEmpty))

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

    def toJson: Iterable[JValue] = {
      toEvents { (slice: Slice, row: RowId) => slice.toJson(row) }
    }

    private def toEvents[A](f: (Slice, RowId) => A): Iterable[A] = {
      new Iterable[A] {
        def iterator = {
          val normalized = self.normalize.slices.iterator

          new Iterator[A] {
            private var slice = if (normalized.hasNext) normalized.next else null.asInstanceOf[Slice]
            private var idx = 0
            private var next0: A = precomputeNext()

            def hasNext = next0 != null

            def next() = {
              val tmp = next0
              next0 = precomputeNext()
              tmp
            }
           
            @tailrec def precomputeNext(): A = {
              if (slice == null) {
                null.asInstanceOf[A]
              } else if (idx < slice.size) {
                val result = f(slice, idx)
                idx += 1
                result
              } else {
                slice = if (normalized.hasNext) normalized.next else null.asInstanceOf[Slice]
                idx = 0
                precomputeNext() //recursive call is okay because hasNext must have returned true to get here
              }
            }
          }
        }
      }
    }
  }
}
// vim: set ts=4 sw=4 et:
