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
import com.precog.yggdrasil.util._

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
import scala.collection.immutable.NumericRange

import scalaz._
import scalaz.Ordering._
import scalaz.std.function._
import scalaz.std.list._
import scalaz.std.tuple._
import scalaz.std.iterable._
import scalaz.std.option._
import scalaz.syntax.arrow._
import scalaz.syntax.monad._
import scalaz.syntax.traverse._
import scalaz.syntax.std.boolean._

trait ColumnarTableModule[M[+_]] extends TableModule[M] with IdSourceScannerModule[M] {
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

  private case class SliceTransform1[A](initial: A, f: (A, Slice) => (A, Slice)) {
    def apply(s: Slice) = f(initial, s)

    def andThen[B](t: SliceTransform1[B]): SliceTransform1[(A, B)] = {
      SliceTransform1(
        (initial, t.initial),
        { case ((a, b), s) => 
            val (a0, sa) = f(a, s) 
            val (b0, sb) = t.f(b, sa)
            ((a0, b0), sb)
        }
      )
    }

    def zip[B](t: SliceTransform1[B])(combine: (Slice, Slice) => Slice): SliceTransform1[(A, B)] = {
      SliceTransform1(
        (initial, t.initial),
        { case ((a, b), s) =>
            val (a0, sa) = f(a, s)
            val (b0, sb) = t.f(b, s)
            assert(sa.size == sb.size)
            ((a0, b0), combine(sa, sb))
        }
      )
    }

    def map(mapFunc: Slice => Slice): SliceTransform1[A] = {
      SliceTransform1(
        initial,
        { case (a, s) =>
            val (a0, sa) = f(a, s)
            (a0, mapFunc(sa))
        }
      )
    }
  }

  private object SliceTransform1 {
    def identity[A](initial: A) = SliceTransform1(initial, (a: A, s: Slice) => (a, s))
  }

  private case class SliceTransform2[A](initial: A, f: (A, Slice, Slice) => (A, Slice), source: Option[TransSpec[SourceType]] = None) {
    def apply(s1: Slice, s2: Slice) = f(initial, s1, s2)

    def andThen[B](t: SliceTransform1[B]): SliceTransform2[(A, B)] = {
      SliceTransform2(
        (initial, t.initial),
        { case ((a, b), sl, sr) => 
            val (a0, sa) = f(a, sl, sr) 
            val (b0, sb) = t.f(b, sa)
            ((a0, b0), sb)
        }
      )
    }

    def zip[B](t: SliceTransform2[B])(combine: (Slice, Slice) => Slice): SliceTransform2[(A, B)] = {
      SliceTransform2(
        (initial, t.initial),
        { case ((a, b), sl, sr) =>
            val (a0, sa) = f(a, sl, sr)
            val (b0, sb) = t.f(b, sl, sr)
            assert(sa.size == sb.size) 
            ((a0, b0), combine(sa, sb))
        }
      )
    }

    def map(mapFunc: Slice => Slice): SliceTransform2[A] = {
      SliceTransform2(
        initial,
        { case (a, sl, sr) =>
            val (a0, s0) = f(a, sl, sr)
            (a0, mapFunc(s0))
        }
      )
    }

    def parallel: SliceTransform1[A] = SliceTransform1[A](initial, (a: A, s: Slice) => f(a,s,s))

    def withSource(ts: TransSpec[SourceType]) = copy(source = Some(ts))
  }

  private object SliceTransform2 {
    def left[A](initial: A)  = SliceTransform2[A](initial, (a: A, sl: Slice, sr: Slice) => (a, sl))
    def right[A](initial: A) = SliceTransform2[A](initial, (a: A, sl: Slice, sr: Slice) => (a, sr))
  }

  def table(slices: StreamT[M, Slice]): Table

  abstract class ColumnarTable(val slices: StreamT[M, Slice]) extends TableLike with Logging { self: Table =>
    /**
     * Folds over the table to produce a single value (stored in a singleton table).
     */
    def reduce[A](reducer: Reducer[A])(implicit monoid: Monoid[A]): M[A] = {  
      (slices map { s => reducer.reduce(s.logicalColumns, 0 until s.size) }).foldLeft(monoid.zero)((a, b) => monoid.append(a, b))
    }

    def compact(spec: TransSpec1): Table = {
      table(transformStream((SliceTransform1.identity(()) zip composeSliceTransform(spec))(_ compact _), slices)).normalize
    }

    private def map0(f: Slice => Slice): SliceTransform1[Unit] = SliceTransform1[Unit]((), Function.untupled(f.second[Unit]))

    private def transformStream[A](sliceTransform: SliceTransform1[A], slices: StreamT[M, Slice]): StreamT[M, Slice] = {
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

      stream(sliceTransform.initial, slices)
    }

    private def composeSliceTransform(spec: TransSpec1): SliceTransform1[_] = {
      composeSliceTransform2(spec).parallel
    }

    // No transform defined herein may reduce the size of a slice. Be it known!
    private def composeSliceTransform2(spec: TransSpec[SourceType]): SliceTransform2[_] = {
      val result = spec match {
        case Leaf(source) if source == Source || source == SourceLeft => SliceTransform2.left(())
        case Leaf(source) if source == SourceRight => SliceTransform2.right(())

        case Map1(source, f) => 
          composeSliceTransform2(source) map {
            _ mapColumns f 
          }

        case Map2(left, right, f) =>
          val l0 = composeSliceTransform2(left)
          val r0 = composeSliceTransform2(right)

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
          composeSliceTransform2(source).zip(composeSliceTransform2(predicate)) { (s: Slice, filter: Slice) => 
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
          val l0 = composeSliceTransform2(left)
          val r0 = composeSliceTransform2(right)

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

          val sourceSlice = composeSliceTransform2(source)

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
        
        case ConstLiteral(value, target) =>
          composeSliceTransform2(target) map { _.definedConst(value) }

        case WrapObject(source, field) =>
          composeSliceTransform2(source) map {
            _ wrap JPathField(field) 
          }

        case WrapArray(source) =>
          composeSliceTransform2(source) map {
            _ wrap JPathIndex(0) 
          }

        case ObjectConcat(left, right) =>
          val l0 = composeSliceTransform2(left)
          val r0 = composeSliceTransform2(right)

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
          val l0 = composeSliceTransform2(left)
          val r0 = composeSliceTransform2(right)

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
          composeSliceTransform2(source) map {
            _ deleteFields mask 
          }

        case Typed(source, tpe) =>
          composeSliceTransform2(source) map {
            _ typed tpe
          }

        case Scan(source, scanner) => 
          composeSliceTransform2(source) andThen {
            SliceTransform1[scanner.A](
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
          composeSliceTransform2(source) map {
            _ deref field
          }

        case DerefObjectDynamic(source, ref) =>
          val l0 = composeSliceTransform2(source)
          val r0 = composeSliceTransform2(ref)

          l0.zip(r0) { (slice, derefBy) => 
            assert(derefBy.columns.size <= 1)
            derefBy.columns.headOption collect {
              case (ColumnRef(JPath.Identity, CString), c: StrColumn) => 
                new DerefSlice(slice, { case row: Int if c.isDefinedAt(row) => JPathField(c(row)) })
            } getOrElse {
              slice
            }
          }

        case DerefArrayStatic(source, element) =>
          composeSliceTransform2(source) map {
            _ deref element
          }

        case DerefArrayDynamic(source, ref) =>
          val l0 = composeSliceTransform2(source)
          val r0 = composeSliceTransform2(ref)

          l0.zip(r0) { (slice, derefBy) => 
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
          composeSliceTransform2(source) map {
            _ arraySwap index 
          }
      }
      
      result.withSource(spec)
    }
    
    /**
     * Performs a one-pass transformation of the keys and values in the table.
     * If the key transform is not identity, the resulting table will have
     * unknown sort order.
     */
    def transform(spec: TransSpec1): Table = {
      table(transformStream(composeSliceTransform(spec), slices))
    }

    
    /**
     * Cogroups this table with another table, using equality on the specified
     * transformation on rows of the table.
     */
    def cogroup(leftKey: TransSpec1, rightKey: TransSpec1, that: Table)(leftResultTrans: TransSpec1, rightResultTrans: TransSpec1, bothResultTrans: TransSpec2): Table = {
      class IndexBuffers(lInitialSize: Int, rInitialSize: Int) {
        val lbuf = new ArrayIntList(lInitialSize)
        val rbuf = new ArrayIntList(rInitialSize)
        val leqbuf = new ArrayIntList(lInitialSize max rInitialSize)
        val reqbuf = new ArrayIntList(lInitialSize max rInitialSize)

        @inline def advanceLeft(lpos: Int): Unit = {
          lbuf.add(lpos)
          rbuf.add(-1)
          leqbuf.add(-1)
          reqbuf.add(-1)
        }

        @inline def advanceRight(rpos: Int): Unit = {
          lbuf.add(-1)
          rbuf.add(rpos)
          leqbuf.add(-1)
          reqbuf.add(-1)
        }

        @inline def advanceBoth(lpos: Int, rpos: Int): Unit = {
          lbuf.add(-1)
          rbuf.add(-1)
          leqbuf.add(lpos)
          reqbuf.add(rpos)
        }

        def cogrouped[LR, RR, BR](lslice: Slice, 
                                  rslice: Slice, 
                                  leftTransform:  SliceTransform1[LR], 
                                  rightTransform: SliceTransform1[RR], 
                                  bothTransform:  SliceTransform2[BR]): (Slice, LR, RR, BR) = {

          val remappedLeft = lslice.remap(lbuf)
          val remappedRight = rslice.remap(rbuf)

          val remappedLeq = lslice.remap(leqbuf)
          val remappedReq = rslice.remap(reqbuf)

          val (ls0, lx) = leftTransform(remappedLeft)
          val (rs0, rx) = rightTransform(remappedRight)
          val (bs0, bx) = bothTransform(remappedLeq, remappedReq)

          assert(lx.size == rx.size && rx.size == bx.size)
          val resultSlice = lx zip rx zip bx

          (resultSlice, ls0, rs0, bs0)
        }

        override def toString = {
          "left: " + lbuf.toArray.mkString("[", ",", "]") + "\n" + 
          "right: " + rbuf.toArray.mkString("[", ",", "]") + "\n" + 
          "both: " + (leqbuf.toArray zip reqbuf.toArray).mkString("[", ",", "]")
        }
      }

      sealed trait NextStep
      case class SplitLeft(lpos: Int) extends NextStep
      case class SplitRight(rpos: Int) extends NextStep
      case class AppendLeft(lpos: Int, rpos: Int, rightCartesian: Option[(Int, Option[Int])]) extends NextStep
      case class AppendRight(lpos: Int, rpos: Int, rightCartesian: Option[(Int, Option[Int])]) extends NextStep

      def cogroup0[LK, RK, LR, RR, BR](stlk: SliceTransform1[LK], strk: SliceTransform1[RK], stlr: SliceTransform1[LR], strr: SliceTransform1[RR], stbr: SliceTransform2[BR]) = {
        case class SlicePosition[K](
          /** The position in the current slice. This will only be nonzero when the slice has been appended
            * to as a result of a cartesian crossing the slice boundary */
          pos: Int, 
          /** Present if not in a final right or left run. A pair of a key slice that is parallel to the 
            * current data slice, and the value that is needed as input to sltk or srtk to produce the next key. */
          keyState: K,
          key: Slice, 
          /** The current slice to be operated upon. */
          data: Slice, 
          /** The remainder of the stream to be operated upon. */
          tail: StreamT[M, Slice])

        sealed trait CogroupState
        case class EndLeft(lr: LR, lhead: Slice, ltail: StreamT[M, Slice]) extends CogroupState
        case class Cogroup(lr: LR, rr: RR, br: BR, left: SlicePosition[LK], right: SlicePosition[RK], rightReset: Option[(Int, Option[Int])]) extends CogroupState
        case class EndRight(rr: RR, rhead: Slice, rtail: StreamT[M, Slice]) extends CogroupState
        case object CogroupDone extends CogroupState

        val Reset = -1

        // step is the continuation function fed to uncons. It is called once for each emitted slice
        def step(state: CogroupState): M[Option[(Slice, CogroupState)]] = {
          // step0 is the inner monadic recursion needed to cross slice boundaries within the emission of a slice
          def step0(lr: LR, rr: RR, br: BR, leftPosition: SlicePosition[LK], rightPosition: SlicePosition[RK], rightReset: Option[(Int, Option[Int])])
                   (ibufs: IndexBuffers = new IndexBuffers(leftPosition.key.size, rightPosition.key.size)): M[Option[(Slice, CogroupState)]] = {
            val SlicePosition(lpos0, lkstate, lkey, lhead, ltail) = leftPosition
            val SlicePosition(rpos0, rkstate, rkey, rhead, rtail) = rightPosition

            val comparator = Slice.rowComparatorFor(lkey, rkey) { slice => 
              // since we've used the key transforms, and since transforms are contracturally
              // forbidden from changing slice size, we can just use all
              slice.columns.keys.toList.sorted
            }

            // the inner tight loop; this will recur while we're within the bounds of
            // a pair of slices. Any operation that must cross slice boundaries
            // must exit this inner loop and recur through the outer monadic loop
            // xrstart is an int with sentinel value for effieiency, but is Option at the slice level.
            @inline @tailrec def buildRemappings(lpos: Int, rpos: Int, xrstart: Int, xrend: Int, endRight: Boolean): NextStep = {
              if (xrstart != -1) {
                // We're currently in a cartesian. 
                if (lpos < lhead.size && rpos < rhead.size) {
                  comparator.compare(lpos, rpos) match {
                    case LT => 
                      buildRemappings(lpos + 1, xrstart, xrstart, rpos, endRight)
                    case GT => 
                      // catch input-out-of-order errors early
                      if (xrend == -1) sys.error("Inputs are not sorted; value on the left exceeded value on the right at the end of equal span.")
                      buildRemappings(lpos, xrend, Reset, Reset, endRight)
                    case EQ => 
                      ibufs.advanceBoth(lpos, rpos)
                      buildRemappings(lpos, rpos + 1, xrstart, xrend, endRight)
                  }
                } else if (lpos < lhead.size) {
                  if (endRight) {
                    // we know there won't be another slice on the RHS, so just keep going to exhaust the left
                    buildRemappings(lpos + 1, xrstart, xrstart, rpos, endRight)
                  } else {
                    // right slice is exhausted, so we need to append to that slice from the right tail
                    // then continue in the cartesian
                    AppendRight(lpos, rpos, Some((xrstart, (xrend != -1).option(xrend))))
                  }
                } else if (rpos < rhead.size) {
                  // left slice is exhausted, so we need to append to that slice from the left tail
                  // then continue in the cartesian
                  AppendLeft(lpos, rpos, Some((xrstart, (xrend != -1).option(xrend))))
                } else {
                  sys.error("This state should be unreachable, since we only increment one side at a time.")
                }
              } else {
                // not currently in a cartesian, hence we can simply proceed.
                if (lpos < lhead.size && rpos < rhead.size) {
                  comparator.compare(lpos, rpos) match {
                    case LT => 
                      ibufs.advanceLeft(lpos)
                      buildRemappings(lpos + 1, rpos, Reset, Reset, endRight)
                    case GT => 
                      ibufs.advanceRight(rpos)
                      buildRemappings(lpos, rpos + 1, Reset, Reset, endRight)
                    case EQ =>
                      ibufs.advanceBoth(lpos, rpos)
                      buildRemappings(lpos, rpos + 1, rpos, Reset, endRight)
                  }
                } else if (lpos < lhead.size) {
                  // right side is exhausted, so we should just split the left and emit 
                  SplitLeft(lpos)
                } else if (rpos < rhead.size) {
                  // left side is exhausted, so we should just split the right and emit
                  SplitRight(rpos)
                } else {
                  sys.error("This state should be unreachable, since we only increment one side at a time.")
                }
              }
            }

            def continue(nextStep: NextStep): M[Option[(Slice, CogroupState)]] = nextStep match {
              case SplitLeft(lpos) =>
                val (lpref, lsuf) = lhead.split(lpos + 1)
                val (_, lksuf) = lkey.split(lpos + 1)
                val (completeSlice, lr0, rr0, br0) = ibufs.cogrouped(lpref, rhead, 
                                                                     SliceTransform1[LR](lr, stlr.f),
                                                                     SliceTransform1[RR](rr, strr.f),
                                                                     SliceTransform2[BR](br, stbr.f))

                rtail.uncons map {
                  case Some((nextRightHead, nextRightTail)) => 
                    val (rkstate0, rkey0) = strk.f(rkstate, nextRightHead)
                    val nextState = Cogroup(lr0, rr0, br0, 
                                            SlicePosition(0, lkstate,  lksuf, lsuf, ltail),
                                            SlicePosition(0, rkstate0, rkey0, nextRightHead, nextRightTail), None) 

                    Some(completeSlice -> nextState)

                  case None => 
                    val nextState = EndLeft(lr0, lsuf, ltail)
                    Some(completeSlice -> nextState)
                }

              case SplitRight(rpos) => 
                val (rpref, rsuf) = rhead.split(rpos + 1)
                val (_, rksuf) = rkey.split(rpos + 1)
                val (completeSlice, lr0, rr0, br0) = ibufs.cogrouped(lhead, rpref, 
                                                                     SliceTransform1[LR](lr, stlr.f),
                                                                     SliceTransform1[RR](rr, strr.f),
                                                                     SliceTransform2[BR](br, stbr.f))

                ltail.uncons map {
                  case Some((nextLeftHead, nextLeftTail)) =>
                    val (lkstate0, lkey0) = stlk.f(lkstate, nextLeftHead)
                    val nextState = Cogroup(lr0, rr0, br0,
                                            SlicePosition(0, lkstate0, lkey0, nextLeftHead, nextLeftTail),
                                            SlicePosition(0, rkstate,  rksuf, rsuf, rtail), None)

                    Some(completeSlice -> nextState)

                  case None =>
                    val nextState = EndRight(rr0, rsuf, rtail)
                    Some(completeSlice -> nextState)
                }

              case AppendLeft(lpos, rpos, rightReset) => 
                ltail.uncons flatMap {
                  case Some((nextLeftHead, nextLeftTail)) =>
                    val (lkstate0, lkey0) = stlk.f(lkstate, nextLeftHead)
                    step0(lr, rr, br,
                          SlicePosition(lpos, lkstate0, lkey append lkey0, lhead append nextLeftHead, nextLeftTail),
                          SlicePosition(rpos, rkstate, rkey, rhead, rtail), 
                          rightReset)(ibufs)

                  case None => 
                    rightReset.flatMap(_._2) map { rend =>
                      // We've found an actual end to the cartesian on the right, and have run out of 
                      // data inside the cartesian on the left, so we have to split the right, emit,
                      // and then end right
                      val (rpref, rsuf) = rhead.split(rend)
                      val (completeSlice, lr0, rr0, br0) = ibufs.cogrouped(lhead, rpref,
                                                                           SliceTransform1[LR](lr, stlr.f),
                                                                           SliceTransform1[RR](rr, strr.f),
                                                                           SliceTransform2[BR](br, stbr.f))

                      val nextState = EndRight(rr0, rsuf, rtail)
                      M.point(Some((completeSlice -> nextState)))
                    } getOrElse {
                      // the end of the cartesian must be found on the right before trying to find the end
                      // on the left, so if we're here then the right must be 
                      val (completeSlice, lr0, rr0, br0) = ibufs.cogrouped(lhead, rhead,
                                                                           SliceTransform1[LR](lr, stlr.f),
                                                                           SliceTransform1[RR](rr, strr.f),
                                                                           SliceTransform2[BR](br, stbr.f))
                      M.point(Some(completeSlice -> CogroupDone))
                    }
                }

              case AppendRight(lpos, rpos, rightReset) => 
                rtail.uncons flatMap {
                  case Some((nextRightHead, nextRightTail)) =>
                    val (rkstate0, rkey0) = strk.f(rkstate, nextRightHead)
                    step0(lr, rr, br, 
                          SlicePosition(lpos, lkstate, lkey, lhead, ltail), 
                          SlicePosition(rpos, rkstate0, rkey append rkey0, rhead append nextRightHead, nextRightTail),
                          rightReset)(ibufs)

                  case None =>
                    // run out the left hand side, since the right will never advance
                    continue(buildRemappings(lpos, rpos, rightReset.map(_._1).getOrElse(Reset), rightReset.flatMap(_._2).getOrElse(Reset), true))
                }
            }

            continue(buildRemappings(lpos0, rpos0, rightReset.map(_._1).getOrElse(Reset), rightReset.flatMap(_._2).getOrElse(Reset), false))
          } // end of step0 

          state match {
            case EndLeft(lr, data, tail) =>
              val (lr0, leftResult) = stlr.f(lr, data)
              tail.uncons map { unconsed =>
                Some(leftResult -> (unconsed map { case (nhead, ntail) => EndLeft(lr0, nhead, ntail) } getOrElse CogroupDone))
              }

            case Cogroup(lr, rr, br, left, right, rightReset) =>
              step0(lr, rr, br, left, right, rightReset)()

            case EndRight(rr, data, tail) =>
              val (rr0, rightResult) = strr.f(rr, data)
              tail.uncons map { unconsed =>
                Some(rightResult -> (unconsed map { case (nhead, ntail) => EndRight(rr0, nhead, ntail) } getOrElse CogroupDone))
              }

            case CogroupDone => M.point(None)
          }
        } // end of step

        val initialState = for {
          leftUnconsed  <- self.slices.uncons
          rightUnconsed <- that.slices.uncons
        } yield {
          val cogroup = for {
            (leftHead, leftTail)   <- leftUnconsed
            (rightHead, rightTail) <- rightUnconsed
          } yield {
            val (lkstate, lkey) = stlk(leftHead)
            val (rkstate, rkey) = strk(rightHead)
            Cogroup(stlr.initial, strr.initial, stbr.initial, 
                    SlicePosition(0, lkstate, lkey, leftHead,  leftTail), 
                    SlicePosition(0, rkstate, rkey, rightHead, rightTail), None)
          } 
          
          cogroup orElse {
            leftUnconsed map {
              case (head, tail) => EndLeft(stlr.initial, head, tail)
            }
          } orElse {
            rightUnconsed map {
              case (head, tail) => EndRight(strr.initial, head, tail)
            }
          }
        }

        table(StreamT.wrapEffect(initialState map { state => StreamT.unfoldM[M, Slice, CogroupState](state getOrElse CogroupDone)(step) }))
      }

      cogroup0(composeSliceTransform(leftKey), 
               composeSliceTransform(rightKey), 
               composeSliceTransform(leftResultTrans), 
               composeSliceTransform(rightResultTrans), 
               composeSliceTransform2(bothResultTrans))
    }

    /**
     * Performs a full cartesian cross on this table with the specified table,
     * applying the specified transformation to merge the two tables into
     * a single table.
     */
    def cross(that: Table)(spec: TransSpec2): Table = {
      def cross0[A](transform: SliceTransform2[A]): M[StreamT[M, Slice]] = {
        case class CrossState(a: A, position: Int, tail: StreamT[M, Slice])

        def crossLeftSingle(lhead: Slice, right: StreamT[M, Slice]): StreamT[M, Slice] = {
          def step(state: CrossState): M[Option[(Slice, CrossState)]] = {
            if (state.position < lhead.size) {
              state.tail.uncons flatMap {
                case Some((rhead, rtail0)) =>
                  val lslice = new Slice {
                    val size = rhead.size
                    val columns = lhead.columns.mapValues { cf.util.Remap({ case _ => state.position })(_).get }
                  }

                  val (a0, resultSlice) = transform.f(state.a, lslice, rhead)
                  M.point(Some((resultSlice, CrossState(a0, state.position, rtail0))))
                  
                case None => 
                  step(CrossState(state.a, state.position + 1, right))
              }
            } else {
              M.point(None)
            }
          }

          StreamT.unfoldM(CrossState(transform.initial, 0, right))(step _)
        }
        
        def crossRightSingle(left: StreamT[M, Slice], rhead: Slice): StreamT[M, Slice] = {
          def step(state: CrossState): M[Option[(Slice, CrossState)]] = {
            state.tail.uncons map {
              case Some((lhead, ltail0)) =>
                val lslice = new Slice {
                  val size = rhead.size * lhead.size
                  val columns = lhead.columns.mapValues { cf.util.Remap({ case i => i / rhead.size })(_).get }
                }

                val rslice = new Slice {
                  val size = rhead.size * lhead.size
                  val columns = rhead.columns.mapValues { cf.util.Remap({ case i => i % rhead.size })(_).get }
                }

                val (a0, resultSlice) = transform.f(state.a, lslice, rslice)
                Some((resultSlice, CrossState(a0, state.position, ltail0)))
                
              case None => None
            }
          }

          StreamT.unfoldM(CrossState(transform.initial, 0, left))(step _)
        }

        def crossBoth(ltail: StreamT[M, Slice], rtail: StreamT[M, Slice]): StreamT[M, Slice] = {
          ltail.flatMap(crossLeftSingle(_ :Slice, rtail))
        }

        this.slices.uncons flatMap {
          case Some((lhead, ltail)) =>
            that.slices.uncons flatMap {
              case Some((rhead, rtail)) =>
                for {
                  lempty <- ltail.isEmpty //TODO: Scalaz result here is negated from what it should be!
                  rempty <- rtail.isEmpty
                } yield {
                  if (lempty) {
                    // left side is a small set, so restart it in memory
                    crossLeftSingle(lhead, rhead :: rtail)
                  } else if (rempty) {
                    // right side is a small set, so restart it in memory
                    crossRightSingle(lhead :: ltail, rhead)
                  } else {
                    // both large sets, so just walk the left restarting the right.
                    crossBoth(this.slices, that.slices)
                  }
                }

              case None => M.point(StreamT.empty[M, Slice])
            }

          case None => M.point(StreamT.empty[M, Slice])
        }
      }

      table(StreamT(cross0(composeSliceTransform2(spec)) map { tail => StreamT.Skip(tail) }))
    }
    
    /**
     * Yields a new table with distinct rows. Assumes this table is sorted.
     */
    def distinct(spec: TransSpec1): Table = {
      def distinct0[T](id: SliceTransform1[Option[Slice]], filter: SliceTransform1[T]): Table = {
        def stream(state: (Option[Slice], T), slices: StreamT[M, Slice]): StreamT[M, Slice] = StreamT(
          for {
            head <- slices.uncons
          } yield
            head map { case (s, sx) =>
              val (prevFilter, cur) = id.f(state._1, s)
              val (nextT, curFilter) = filter.f(state._2, s)
              
              val next = cur.distinct(prevFilter, curFilter)
              
              StreamT.Yield(next, stream((if(next.size > 0) Some(curFilter) else prevFilter, nextT), sx))
            } getOrElse {
              StreamT.Done
            }
        )
        
        table(stream((id.initial, filter.initial), slices))
      }

      distinct0(SliceTransform1.identity(None : Option[Slice]), composeSliceTransform(spec))
    }
    
    def takeRange(startIndex: Long, numberToTake: Long): Table = {  //in slice.takeRange, need to numberToTake to not be larger than the slice. 
      def loop(s: Stream[Slice], readSoFar: Long): Stream[Slice] = s match {
        case h #:: rest if (readSoFar + h.size) < startIndex => loop(rest, readSoFar + h.size)
        case rest if readSoFar < startIndex + 1 => {
          inner(rest, 0, (startIndex - readSoFar).toInt)
        }
        case _ => Stream.empty[Slice]
      }

      def inner(s: Stream[Slice], takenSoFar: Long, sliceStartIndex: Int): Stream[Slice] = s match {
        case h #:: rest if takenSoFar < numberToTake && h.size > numberToTake - takenSoFar => {
          val needed = h.takeRange(sliceStartIndex, (numberToTake - takenSoFar).toInt)
          println("sliceStartIndex: %s and     numberToTake: %s and   takenSoFar: %s and    needed: %s".format(sliceStartIndex, numberToTake, takenSoFar, needed))
          needed #:: Stream.empty[Slice]
        }
        case h #:: rest if takenSoFar < numberToTake =>
          h #:: inner(rest, takenSoFar + h.size, 0)
        case _ => Stream.empty[Slice]
      }

      table(StreamT.fromStream(slices.toStream.map(loop(_, 0))))
    }

    def normalize: Table = table(slices.filter(!_.isEmpty))

    def toStrings: M[Iterable[String]] = {
      toEvents { (slice, row) => slice.toString(row) }
    }
    
    def toJson: M[Iterable[JValue]] = {
      toEvents { (slice, row) => slice.toJson(row) }
    }

    private def toEvents[A](f: (Slice, RowId) => Option[A]): M[Iterable[A]] = {
      for (stream <- self.compact(Leaf(Source)).slices.toStream) yield {
        (for (slice <- stream; i <- 0 until slice.size) yield f(slice, i)).flatten 
      }
    }
  }
}
// vim: set ts=4 sw=4 et:
