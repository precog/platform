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

import com.precog.common.{Path, VectorCase}
import com.precog.bytecode.JType
import com.precog.yggdrasil.jdbm3._

import blueeyes.bkka.AkkaTypeClasses
import blueeyes.json._
import blueeyes.json.JsonAST._
import org.apache.commons.collections.primitives.ArrayIntList
import org.joda.time.DateTime
import com.google.common.io.Files
import com.weiglewilczek.slf4s.Logging

import org.apache.jdbm.DBMaker
import java.io.File
import java.util.SortedMap

import scala.collection.BitSet
import scala.collection.Set
import scala.annotation.tailrec

import scalaz._
import scalaz.Ordering._
import scalaz.std.function._
import scalaz.std.list._
import scalaz.std.tuple._
import scalaz.std.iterable._
import scalaz.syntax.arrow._
import scalaz.syntax.traverse._

trait ColumnarTableModule[M[+_]] extends TableModule[M] {
  import trans._
  import trans.constants._

  type F1 = CF1
  type F2 = CF2
  type Scanner = CScanner
  type Reducer[α] = CReducer[α]
  type RowId = Int
  type Table <: ColumnarTable

  def newScratchDir(): File = Files.createTempDir()
  def jdbmCommitInterval: Long = 200000l

  object ops extends TableOps {
    def empty: Table = table(StreamT.empty[M, Slice])
    
    def constBoolean(v: Set[CBoolean]): Table = {
      val column = ArrayBoolColumn(v.map(_.value).toArray)
      table(Slice(Map(ColumnRef(JPath.Identity, CBoolean) -> column), v.size) :: StreamT.empty[M, Slice])
    }

    def constLong(v: Set[CLong]): Table = {
      val column = ArrayLongColumn(v.map(_.value).toArray)
      table(Slice(Map(ColumnRef(JPath.Identity, CLong) -> column), v.size) :: StreamT.empty[M, Slice])
    }

    def constDouble(v: Set[CDouble]): Table = {
      val column = ArrayDoubleColumn(v.map(_.value).toArray)
      table(Slice(Map(ColumnRef(JPath.Identity, CDouble) -> column), v.size) :: StreamT.empty[M, Slice])
    }

    def constDecimal(v: Set[CNum]): Table = {
      val column = ArrayNumColumn(v.map(_.value).toArray)
      table(Slice(Map(ColumnRef(JPath.Identity, CNum) -> column), v.size) :: StreamT.empty[M, Slice])
    }

    def constString(v: Set[CString]): Table = {
      val column = ArrayStrColumn(v.map(_.value).toArray)
      table(Slice(Map(ColumnRef(JPath.Identity, CString) -> column), v.size) :: StreamT.empty[M, Slice])
    }

    def constDate(v: Set[CDate]): Table =  {
      val column = ArrayDateColumn(v.map(_.value).toArray)
      table(Slice(Map(ColumnRef(JPath.Identity, CDate) -> column), v.size) :: StreamT.empty[M, Slice])
    }

    def constNull: Table = 
      table(Slice(Map(ColumnRef(JPath.Identity, CNull) -> new InfiniteColumn with NullColumn), 1) :: StreamT.empty[M, Slice])

    def constEmptyObject: Table = 
      table(Slice(Map(ColumnRef(JPath.Identity, CEmptyObject) -> new InfiniteColumn with EmptyObjectColumn), 1) :: StreamT.empty[M, Slice])

    def constEmptyArray: Table = 
      table(Slice(Map(ColumnRef(JPath.Identity, CEmptyArray) -> new InfiniteColumn with EmptyArrayColumn), 1) :: StreamT.empty[M, Slice])
  }
  
  object grouper extends Grouper {
    import trans._
    
    def merge[A: scalaz.Equal](grouping: GroupingSpec[A])(body: (Table, A => Table) => M[Table]): M[Table] =
      sys.error("todo")
  }

  implicit def liftF1(f: F1) = new F1Like {
    def compose(f1: F1) = f compose f1
    def andThen(f1: F1) = f andThen f1
  }

  implicit def liftF2(f: F2) = new F2Like {
    def applyl(cv: CValue) = new CF1(f(Column.const(cv), _))
    def applyr(cv: CValue) = new CF1(f(_, Column.const(cv)))

    def andThen(f1: F1) = new CF2((c1, c2) => f(c1, c2) flatMap f1.apply)
  }

  private case class SliceTransform[A](initial: A, f: (A, Slice) => (A, Slice)) {
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

    def map(mapFunc: Slice => Slice): SliceTransform[A] = {
      SliceTransform(
        initial,
        { case (a, s) =>
            val (a0, sa) = f(a, s)
            (a0, mapFunc(sa))
        }
      )
    }
  }

  private object SliceTransform {
    def identity[A](initial: A) = SliceTransform(initial, (a: A, s: Slice) => (a, s))
  }

  def table(slices: StreamT[M, Slice]): Table

  abstract class ColumnarTable(val slices: StreamT[M, Slice]) extends TableLike with Logging { self: Table =>
    /**
     * Folds over the table to produce a single value (stored in a singleton table).
     */
    def reduce[A](reducer: Reducer[A])(implicit monoid: Monoid[A]): M[A] = {  
      (slices map { s => reducer.reduce(s.logicalColumns, 0 until s.size) }).foldLeft(monoid.zero)((a, b) => monoid.append(a, b))
    }

    def compact(spec: TransSpec1): Table = sys.error("todo")

    private def map0(f: Slice => Slice): SliceTransform[Unit] = SliceTransform[Unit]((), Function.untupled(f.second[Unit]))

    private def transform0[A](sliceTransform: SliceTransform[A]): Table = {
      def stream(state: A, slices: StreamT[M, Slice]): StreamT[M, Slice] = StreamT(
        for {
          head <- slices.uncons
        } yield {
          head map { case (s, sx) =>
            val (nextState, s0) = sliceTransform.f(state, s)
            StreamT.Yield(s0, stream(nextState, sx))
          } getOrElse {
            StreamT.Done
          }
        }
      )

      table(stream(sliceTransform.initial, slices))
    }

    // No transform defined herein may reduce the size of a slice. Be it known!
    private def composeSliceTransform(spec: TransSpec1): SliceTransform[_] = {
      spec match {
        case Leaf(_) => SliceTransform.identity[Unit](())

        case Map1(source, f) => 
          composeSliceTransform(source) andThen {
             map0 { _ mapColumns f }
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

        case Filter(source, predicate) => 
          composeSliceTransform(source).zip(composeSliceTransform(predicate)) { (s: Slice, filter: Slice) => 
            assert(filter.size == s.size)

            if (s.columns.isEmpty) {
              s
            } else {
              val definedAt: BitSet = filter.columns.values.foldLeft(BitSet.empty) { (acc, col) =>
                col match {
                  case c: BoolColumn => {
                    cf.util.isSatisfied(col).map(_.definedAt(0, s.size) ++ acc).getOrElse(BitSet.empty) 
                  }
                  case _ => acc
                }
              }

              s filterColumns { cf.util.filter(0, s.size, definedAt) }
            }
          }

        case Equal(left, right) =>
          val l0 = composeSliceTransform(left)
          val r0 = composeSliceTransform(right)

          l0.zip(r0) { (sl, sr) =>
            new Slice {
              val size = sl.size
              val columns: Map[ColumnRef, Column] = {
                // 'excluded' is the set of columns that do not exist on both sides of the equality comparison
                // if, for a given row, any of these columns' isDefinedAt returns true, then
                // the result is defined for that row, and its value is false. If isDefinedAt
                // returns false for all columns, then the result (true, false, or undefined) 
                // must be determined by comparing the remaining columns pairwise.

                // In the following fold, we compute all paired columns, and the columns on the left that
                // have no counterpart on the right.
                val (paired, excludedLeft) = sl.columns.foldLeft((Map.empty[JPath, Column], Set.empty[Column])) {
                  case ((paired, excluded), (ref @ ColumnRef(selector, CLong | CDouble | CNum), col)) => 
                    val numEq = for {
                                  ctype <- CLong :: CDouble :: CNum :: Nil
                                  col0  <- sr.columns.get(ColumnRef(selector, ctype)) 
                                  boolc <- cf.std.Eq(col, col0)
                                } yield boolc

                    if (numEq.isEmpty) {
                      (paired, excluded + col)
                    } else {
                      val resultCol = new BoolColumn {
                        def isDefinedAt(row: Int) = {
                          numEq exists { _.isDefinedAt(row) }
                        }
                        def apply(row: Int) = {
                          numEq exists { 
                            case col: BoolColumn => col.isDefinedAt(row) && col(row) 
                            case _ => sys.error("Unreachable code - only boolean columns can be derived from equality.")
                          }
                        }
                      }

                      (paired + (selector -> paired.get(selector).flatMap(cf.std.And(_, resultCol)).getOrElse(resultCol)), excluded)
                    }

                  case ((paired, excluded), (ref, col)) =>
                    sr.columns.get(ref) flatMap { col0 =>
                      cf.std.Eq(col, col0) map { boolc =>
                        // todo: This line contains something that might be an error case going to none, but I can't see through it
                        // well enough to know for sure. Please review.
                        (paired + (ref.selector -> paired.get(ref.selector).flatMap(cf.std.And(_, boolc)).getOrElse(boolc)), excluded)
                      }
                    } getOrElse {
                      (paired, excluded + col)
                    }
                }

                val excluded = excludedLeft ++ sr.columns.collect({
                  case (ColumnRef(selector, CLong | CDouble | CNum), col) 
                    if !(CLong :: CDouble :: CNum :: Nil).exists(ctype => sl.columns.contains(ColumnRef(selector, ctype))) => col

                  case (ref, col) if !sl.columns.contains(ref) => col
                })

                val allColumns = sl.columns ++ sr.columns
                
                val resultCol = new MemoBoolColumn(
                  new BoolColumn {
                    def isDefinedAt(row: Int): Boolean = {
                      allColumns exists { case (_, c) => c.isDefinedAt(row) } 
                    }

                    def apply(row: Int): Boolean = {
                      !(
                        // if any excluded column exists for the row, unequal
                        excluded.exists(_.isDefinedAt(row)) || 
                         // if any paired column compares unequal, unequal
                        paired.exists({ case (_, equal: BoolColumn) => equal.isDefinedAt(row) && !equal(row) })
                      )
                    }
                  }
                )
                
                Map(ColumnRef(JPath.Identity, CBoolean) -> resultCol)
              }
            }
          }

        case EqualLiteral(source, value, invert) => {
          val id = System.currentTimeMillis
          import cf.std.Eq

          val sourceSlice = composeSliceTransform(source)

          def comp: (BoolColumn => BoolColumn) = {
            (b: BoolColumn) => new BoolColumn {
              def isDefinedAt(row: Int) = b.isDefinedAt(row)
              def apply(row: Int) = !b(row)
            }
          }

          def boolId: (BoolColumn => BoolColumn) = {
            (b: BoolColumn) => b
          }

          val transform: (BoolColumn => BoolColumn)  = if (invert) comp else boolId

          sourceSlice map { ss =>
            new Slice {
              val size = ss.size
              val columns = {
                val (comparable, other) = ss.columns.toList.partition {
                  case (ref @ ColumnRef(JPath.Identity, tpe), col) if CType.canCompare(CType.of(value),tpe) => true
                  case _ => false
                }

                (comparable.flatMap { case (ref, col) => Eq.partialRight(value)(col).map { col => (ref, col.asInstanceOf[BoolColumn]) } } ++
                 other.map { case (ref, col) => (ref.copy(selector = JPath.Identity), new Map1Column(col) with BoolColumn { def apply(row: Int) = false }) }).map {
                  case (ref, col) => (ref, transform(col)) 
                }.groupBy { 
                  case (ref, col) => ref 
                }.map {
                  case (ref, columns) => (ref, new BoolColumn {
                    def isDefinedAt(row: Int) = columns.exists { case (_, col) => col.isDefinedAt(row) }
                    def apply(row: Int)       = columns.exists { case (_, col) => col(row) }
                  })
                }
              }
            }
          }
        }

        case WrapObject(source, field) =>
          composeSliceTransform(source) andThen {
            map0 { _ wrap JPathField(field) }
          }

        case WrapArray(source) =>
          composeSliceTransform(source) andThen {
            map0 { _ wrap JPathIndex(0) }
          }

        case ObjectConcat(left, right) =>
          val l0 = composeSliceTransform(left)
          val r0 = composeSliceTransform(right)

          l0.zip(r0) { (sl, sr) =>
            new Slice {
              val size = sl.size
              val columns = {
                val logicalFilters = sr.columns.groupBy(_._1.selector) mapValues { cols => new BoolColumn {
                  def isDefinedAt(row: Int) = cols.exists(_._2.isDefinedAt(row))
                  def apply(row: Int) = !isDefinedAt(row)
                }}

                val remapped = sl.columns map {
                  case (ref @ ColumnRef(jpath, ctype), col) => (ref, logicalFilters.get(jpath).flatMap(c => cf.util.FilterComplement(c)(col)).getOrElse(col))
                }

                remapped ++ sr.columns
              }
            }
          }

        case ArrayConcat(left, right) =>
          val l0 = composeSliceTransform(left)
          val r0 = composeSliceTransform(right)

          l0.zip(r0) { (sl, sr) =>
            def assertDense(paths: Set[JPath]) = assert {
              (paths collect { case JPath(JPathIndex(i), _ @ _*) => i }).toList.sorted.zipWithIndex forall { case (a, b) => a == b }
            }

            assertDense(sl.columns.keySet.map(_.selector))
            assertDense(sr.columns.keySet.map(_.selector))

            new Slice {
              val size = sl.size
              val columns: Map[ColumnRef, Column] = {
                val (indices, lcols) = sl.columns.toList map { case t @ (ColumnRef(JPath(JPathIndex(i), xs @ _*), _), _) => (i, t) } unzip
                val maxIndex = indices.reduceLeftOption(_ max _).map(_ + 1).getOrElse(0)
                val rcols = sr.columns map { case (ColumnRef(JPath(JPathIndex(j), xs @ _*), ctype), col) => (ColumnRef(JPath(JPathIndex(j + maxIndex) +: xs : _*), ctype), col) }
                lcols.toMap ++ rcols
              }
            }
          }

        case ObjectDelete(source, mask) => 
          composeSliceTransform(source) andThen {
            map0 { _ deleteFields mask }
          }

        case Typed(source, tpe) =>
          composeSliceTransform(source) andThen {
            map0 { _ typed tpe }
          }

        case Scan(source, scanner) => 
          composeSliceTransform(source) andThen {
            SliceTransform[scanner.A](
              scanner.init,
              (state: scanner.A, slice: Slice) => {
                assert(slice.columns.size <= 1)
                slice.columns.headOption flatMap {
                  case (ColumnRef(selector, ctype), col) =>
                    val (nextState, nextCol) = scanner.scan(state, col, 0 until slice.size)
                    nextCol map { c =>
                      ( nextState, 
                        new Slice { 
                          val size = slice.size; 
                          val columns = Map(ColumnRef(selector, c.tpe) -> c)
                        }
                      )
                    }
                } getOrElse {
                  (state, slice)
                } 
              }
            )
          }

        case DerefObjectStatic(source, field) =>
          composeSliceTransform(source) andThen {
            map0 { _ deref field }
          }

        case DerefObjectDynamic(source, ref) =>
          composeSliceTransform(source).zip(composeSliceTransform(ref)) { (slice, derefBy) => 
            assert(derefBy.columns.size <= 1)
            derefBy.columns.headOption collect {
              case (ColumnRef(JPath.Identity, CString), c: StrColumn) => 
                new DerefSlice(slice, { case row: Int if c.isDefinedAt(row) => JPathField(c(row)) })
            } getOrElse {
              slice
            }
          }

        case DerefArrayStatic(source, element) =>
          composeSliceTransform(source) andThen {
            map0 { _ deref element }
          }

        case DerefArrayDynamic(source, ref) =>
          composeSliceTransform(source).zip(composeSliceTransform(ref)) { (slice, derefBy) => 
            assert(derefBy.columns.size <= 1)
            derefBy.columns.headOption collect {
              case (ColumnRef(JPath.Identity, CLong), c: LongColumn) => 
                new DerefSlice(slice, { case row: Int if c.isDefinedAt(row) => JPathIndex(c(row).toInt) })

              case (ColumnRef(JPath.Identity, CDouble), c: DoubleColumn) => 
                new DerefSlice(slice, { case row: Int if c.isDefinedAt(row) => JPathIndex(c(row).toInt) })

              case (ColumnRef(JPath.Identity, CNum), c: NumColumn) => 
                new DerefSlice(slice, { case row: Int if c.isDefinedAt(row) => JPathIndex(c(row).toInt) })
            } getOrElse {
              slice
            }
          }

        case ArraySwap(source, index) =>
          composeSliceTransform(source) andThen {
            map0 { _ arraySwap index }
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
        
    // Does this have to be fully known at every point in time?
    def schema: JType = sys.error("todo")
    
    def drop(n: Long): Table = sys.error("todo")
    
    def take(n: Long): Table = sys.error("todo")
    
    def takeRight(n: Long): Table = sys.error("todo")

    def normalize: Table = table(slices.filter(!_.isEmpty))

  /*
    def cogroup(other: Table, prefixLength: Int)(merge: CogroupMerge): Table = {
      sealed trait CogroupState
      case object StepLeftCheckRight extends CogroupState
      case object StepLeftDoneRight extends CogroupState
      case object StepRightCheckLeft extends CogroupState
      case object StepRightDoneLeft extends CogroupState
      case object Cartesian extends CogroupState
      case object Done extends CogroupState

      table(
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

    def toStrings: M[Iterable[String]] = {
      toEvents { (slice, row) => slice.toString(row) }
    }
    
    def toJson: M[Iterable[JValue]] = {
      toEvents { (slice, row) => slice.toJson(row) }
    }

    private def toEvents[A](f: (Slice, RowId) => A): M[Iterable[A]] = {
      for (stream <- self.normalize.slices.toStream) yield {
        for (slice <- stream; i <- 0 until slice.size) yield f(slice, i) 
      }
    }
  }
}
// vim: set ts=4 sw=4 et:
