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
import scala.annotation.tailrec

import scalaz._
import scalaz.Ordering._
import scalaz.std.function._
import scalaz.std.list._
import scalaz.std.tuple._
//import scalaz.std.iterable._
import scalaz.std.option._
import scalaz.std.map._
import scalaz.std.set._
import scalaz.std.stream._
import scalaz.syntax.arrow._
import scalaz.syntax.monad._
import scalaz.syntax.traverse._
import scalaz.syntax.std.boolean._

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
    import scala.collection.Set
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
  
  implicit def liftF1(f: F1) = new F1Like {
    def compose(f1: F1) = f compose f1
    def andThen(f1: F1) = f andThen f1
  }

  implicit def liftF2(f: F2) = new F2Like {
    def applyl(cv: CValue) = new CF1(f(Column.const(cv), _))
    def applyr(cv: CValue) = new CF1(f(_, Column.const(cv)))

    def andThen(f1: F1) = new CF2((c1, c2) => f(c1, c2) flatMap f1.apply)
  }

  protected object SliceTransform {
    def identity[A](initial: A) = SliceTransform1[A](initial, (a: A, s: Slice) => (a, s))
    def left[A](initial: A)  = SliceTransform2[A](initial, (a: A, sl: Slice, sr: Slice) => (a, sl))
    def right[A](initial: A) = SliceTransform2[A](initial, (a: A, sl: Slice, sr: Slice) => (a, sr))
  }

  protected case class SliceTransform1[A](initial: A, f: (A, Slice) => (A, Slice)) {
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

  protected case class SliceTransform2[A](initial: A, f: (A, Slice, Slice) => (A, Slice), source: Option[TransSpec[SourceType]] = None) {
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

  def table(slices: StreamT[M, Slice]): Table

  def align(tables: (Table, TransSpec1)*): Seq[Table] = sys.error("todo")

  abstract class ColumnarTable(val slices: StreamT[M, Slice]) extends TableLike with Logging { self: Table =>
    /**
     * Folds over the table to produce a single value (stored in a singleton table).
     */
    def reduce[A](reducer: Reducer[A])(implicit monoid: Monoid[A]): M[A] = {  
      (slices map { s => reducer.reduce(s.logicalColumns, 0 until s.size) }).foldLeft(monoid.zero)((a, b) => monoid.append(a, b))
    }

    def compact(spec: TransSpec1): Table = {
      table(transformStream((SliceTransform.identity(()) zip composeSliceTransform(spec))(_ compact _), slices)).normalize
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

    protected def composeSliceTransform(spec: TransSpec1): SliceTransform1[_] = {
      composeSliceTransform2(spec).parallel
    }

    // No transform defined herein may reduce the size of a slice. Be it known!
    protected def composeSliceTransform2(spec: TransSpec[SourceType]): SliceTransform2[_] = {
      val result = spec match {
        case Leaf(source) if source == Source || source == SourceLeft => SliceTransform.left(())
        case Leaf(source) if source == SourceRight => SliceTransform.right(())

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
          composeSliceTransform2(target) map {
            _ filterColumns cf.util.DefinedConst(value)
          }

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

            val compare = Slice.rowComparator(lkey, rkey) { slice => 
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
                  compare(lpos, rpos) match {
                    case LT => 
                      buildRemappings(lpos + 1, xrstart, xrstart, rpos, endRight)
                    case GT => 
                      // this will miss catching input-out-of-order errors, but we know that the right must
                      // be advanced fully within the 'EQ' state before we get here, so we can just
                      // increment the right by 1
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
                  compare(lpos, rpos) match {
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

      distinct0(SliceTransform.identity(None : Option[Slice]), composeSliceTransform(spec))
    }

    def drop(n: Long): Table = sys.error("todo")
    
    def take(n: Long): Table = sys.error("todo")
    
    def takeRight(n: Long): Table = sys.error("todo")

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

  object grouper extends Grouper {
    import trans._
    type TicVar = JPathField

    case class MergeAlignment(left: MergeSpec, right: MergeSpec, keys: Seq[TicVar])
    
    sealed trait MergeSpec
    case class SortMergeSpec(target: MergeSpec, sortBy: TransSpec1) extends MergeSpec
    case class SourceMergeSpec(binding: Binding) extends MergeSpec
    case class LeftAlignMergeSpec(alignment: MergeAlignment) extends MergeSpec
    case class RightAlignMergeSpec(alignment: MergeAlignment) extends MergeSpec
    case class IntersectMergeSpec(mergeSpecs: Set[MergeSpec]) extends MergeSpec
    case class NodeMergeSpec(ordering: Seq[TicVar], toAlign: Set[MergeSpec]) extends MergeSpec
    case class CrossMergeSpec(left: MergeSpec, right: MergeSpec) extends MergeSpec
    
    // The GroupKeySpec for a binding is comprised only of conjunctions that refer only
    // to members of the source table. The targetTrans defines a transformation of the
    // table to be used as the value output after keys have been derived. 
    case class Binding(source: Table, idTrans: TransSpec1, targetTrans: TransSpec1, groupKeySpec: GroupKeySpec)
    case class Universe(bindings: List[Binding]) {
      import Universe._

      def composeMergeSpec: MergeSpec = {
        val clusters: Map[MergeNode, List[Binding]] = bindings groupBy { 
          case Binding(_, _, _, groupKeySpec) => MergeNode(sources(groupKeySpec).map(_.key).toSet) 
        }

        val adjacencyList: List[MergeEdge] = (for { 
          l <- clusters.keys
          r <- clusters.keys
          if l != r
          sharedKey = l.keys intersect r.keys
          if sharedKey.nonEmpty
        } yield {
          MergeEdge(l, r, sharedKey)
        })(collection.breakOut)

        val intersectionSubtrees: Set[MergeTree] = findSpanningForest(clusters.keySet map { node => MergeTree(Set(node)) }, adjacencyList)

        val allMergeSpecs = for (subtree <- intersectionSubtrees) yield {
          val inbound: Map[MergeNode, Set[MergeEdge]] = subtree.edges.foldLeft(subtree.nodes.map((_, Set.empty[MergeEdge])).toMap) {
            case (acc, edge @ MergeEdge(a, b, _)) => 
              val aInbound = acc(a) + edge
              val bInbound = acc(b) + edge
              acc + (a -> aInbound) + (b -> bInbound)
          }

          buildMerges(clusters, inbound)
        }
          
        allMergeSpecs reduceLeft (CrossMergeSpec.apply _)
      }
    }
    
    object Universe {
      // a universe is a conjunction of binding clauses, which must contain no disjunctions
      def sources(spec: GroupKeySpec): Seq[GroupKeySpecSource] = (spec: @unchecked) match {
        case GroupKeySpecAnd(left, right) => sources(left) ++ sources(right)
        case src: GroupKeySpecSource => Vector(src)
      }

      // the GroupKeySpec passed to deriveTransSpecs must be either a source or a conjunction; all disjunctions
      // have been factored out by this point
      def deriveTransSpec(conjunction: GroupKeySpec, keyOrder: Seq[TicVar] = Nil): TransSpec1 = {
        val keyMap = keyOrder.zipWithIndex.toMap

        // [['a, value], ['b, value2]]
        val keySpecs = sources(conjunction).sortBy(src => keyMap.getOrElse(src.key, Int.MaxValue)) map { src => 
          WrapArray(ArrayConcat(WrapArray(ConstLiteral(CString(src.key.name), src.spec)), WrapArray(src.spec))) : TransSpec1
        }
        
        keySpecs reduce { ArrayConcat(_, _) }
      }

      // MergeTrees describe intersections as edges in a graph, where the nodes correspond
      // to sets of bindings
      case class MergeNode(keys: Set[TicVar])
      case class MergeEdge(a: MergeNode, b: MergeNode, sharedKey: Set[TicVar]) {
        def nodes = Set(a, b)
        def keys = a.keys ++ b.keys
      }

      // A maximal spanning tree for a merge graph, where the edge weights correspond
      // to the size of the shared keyset for that edge. We use hte maximal weights
      // since the larger the set of shared keys, the fewer constraints are imposed
      // making it more likely that a sorting for those shared keys can be reused.
      case class MergeTree(nodes: Set[MergeNode], edges: Set[MergeEdge] = Set()) {
        def join(other: MergeTree, edge: MergeEdge) = MergeTree(nodes ++ other.nodes, edges ++ other.edges + edge)
      } 

      // An implementation of Kruskal's algorithm for finding a maximal spanning forest
      // for a set of merge trees
      def findSpanningForest(trees: Set[MergeTree], edges: List[MergeEdge]): Set[MergeTree] = {
        def find0(trees: Set[MergeTree], edges: List[MergeEdge]): Set[MergeTree] = {
          if (edges.isEmpty) {
            trees 
          } else {
            val edge0 = edges.head

            val newTrees = trees.find(t => t.nodes.contains(edge0.a)) map { t0 =>
              if (t0.nodes.contains(edge0.b)) {
                trees
              } else {
                trees.find(t => t.nodes.contains(edge0.b)) map { t1 =>
                  (trees - t0 - t1) + t0.join(t1, edge0)
                } getOrElse {
                  (trees - t0) + MergeTree(t0.nodes + edge0.b, t0.edges + edge0)
                }
              }
            } orElse {
              trees.find(t => t.nodes.contains(edge0.b)) map { t0 => 
                (trees - t0) + MergeTree(t0.nodes + edge0.a, t0.edges + edge0)
              }
            } getOrElse {
              trees + MergeTree(Set(edge0.a, edge0.b), Set(edge0))
            }

            find0(newTrees, edges.tail)
          }
        }
        
        find0(trees, edges.sortBy(-_.sharedKey.size))
      }

      case class BindingConstraint(ordering: Seq[Set[TicVar]]) {
        @tailrec final def computeCompatible(keys: Set[TicVar], tail: Seq[Set[TicVar]] = ordering, acc: Seq[Set[TicVar]] = Vector()): Option[BindingConstraint] = {
          if (tail.isEmpty) {
            // no more constraints are imposed by the existing constraint sequence, so we can just tack on the remaining key set
            // and be done
            Some(BindingConstraint(acc :+ keys))
          } else {
            val remainder = keys diff tail.head
            val intersection = tail.head intersect keys
            if (remainder.isEmpty && intersection.nonEmpty) {
              // the remaining keys are entirely contained in the head of the tail, so we have to split the head
              Some(BindingConstraint((acc :+ intersection :+ (tail.head diff keys)) ++ tail.tail))
            } else if ((tail.head diff keys).isEmpty && intersection.nonEmpty) { 
              // the head of the tail is entirely contained in the key set, so recurse on the remainder
              computeCompatible(remainder, tail.tail, acc :+ tail.head)
            } else {
              None
            }
          }
        }


        // Fix this binding constraint into a sort order. Any non-singleton TicVar sets will simply
        // be convered into an arbitrary sequence
        lazy val fixed = ordering.flatten
      }

      object BindingConstraints {
        /**
         * Given the set of input constraints, find a _minimal_ set of compatible BindingConstraints that
         * covers that set.
         */
        def minimize(constraints: Set[BindingConstraint]): Set[BindingConstraint] = {
          @tailrec
          def reduce(unreduced: Set[BindingConstraint], minimized: Set[BindingConstraint]): Set[BindingConstraint] = {
            if (unreduced.isEmpty) {
              minimized
            } else {
              // Find the first constraint in the tail that can be reduced with the head
              unreduced.tail.iterator.map { c => (c, replacementFor(c, unreduced.head)) } find { _._2.isDefined } match {
                // We have a reduction, so re-run, replacing the two reduced constraints with the newly compute one
                case Some((other, Some(reduced))) => reduce(unreduced -- Set(other, unreduced.head) + reduced, minimized)
                // No reduction possible, so head is part of the minimized set
                case _ => reduce(unreduced.tail, minimized + unreduced.head)
              }
            }
          }

          reduce(constraints, Set())
        }

        // Choose a set of strict orderings for the tic variables involved in the specified binding constraints.
        // If a hint is specified, then the hinted sequence must be represented in the resulting set of constraints 
        // by a sequence that is a prefix of the hint. We use a prefix because the graph is traversed in order
        // from largest number of tic variables per node to least, so as we choose a set of fixed constraints
        // to pass in the downward traversal, we must be sure that the constraints at the current node are
        // respected.
        def fix(constraints: Set[BindingConstraint], hint: Option[Seq[TicVar]] = None): Set[Seq[TicVar]] = {
          // Minimize the constraints, possibly including the hint, then flatten the minimal constraints into Seqs
          minimize(hint.map { s => constraints + BindingConstraint(s.map(Set(_))) }.getOrElse(constraints)).map(_.ordering.flatten)
        }

        def select(preferred: Set[Seq[TicVar]], dispreferred: Set[Seq[TicVar]], among: Set[BindingConstraint]): Seq[TicVar] = {
          sys.error("todo")
        }

        /**
         * Compute a new constraint that can replace both input constraints
         */
        def replacementFor(a: BindingConstraint, b: BindingConstraint): Option[BindingConstraint] = {
          @tailrec
          def alignConstraints(left: Seq[Set[TicVar]], right: Seq[Set[TicVar]], computed: Seq[Set[TicVar]] = Seq()): Option[BindingConstraint] = {
            if (left.isEmpty) {
              // left is a prefix or equal to the shifted right, so we can use computed :: right as our common constraint
              Some(BindingConstraint(computed ++ right))
            } else {
              val intersection = left.head & right.head
              val diff = right.head diff left.head
              if (intersection == left.head) {
                // If left's head is a subset of right's, we can split right's head, use the subset as the next part
                // of our computed sequence, then push the unused portion back onto right for another round of alignment
                val newRight = if (diff.nonEmpty) diff +: right.tail else right.tail
                alignConstraints(left.tail, newRight, computed :+ intersection)
              } else {
                // left is not a subset, so these constraints can't be aligned
                None
              }
            }
          }

          alignConstraints(a.ordering, b.ordering) orElse alignConstraints(b.ordering, a.ordering)
        }
      }

      // A depth-first traversal of merge edges that results in a set of binding constraints for each node.
      // These binding constraints may be underconstrained (i.e. an element of the constraint sequence
      // may have arity > 1, so a second pass from the top down will be necessary to determine a fixed
      // ordering that then gets propagated downward.
      def findUnderconstrained(node: MergeNode, graph: Map[MergeNode, Set[MergeEdge]]): Map[MergeNode, Set[BindingConstraint]] = {
        if (graph.get(node).forall(_.isEmpty)) {
          Map(node -> Set(BindingConstraint(Seq(node.keys))))
        } else {
          val outbound = graph(node)

          // subset the graph to only include edges that do not involve the current node
          val graph0: Map[MergeNode, Set[MergeEdge]] = (graph - node) map { 
            {(_: Set[MergeEdge]).filterNot({case MergeEdge(a, b, _) => a == node || b == node})}.second 
          }

          // for all outbound edges, visit the remote node collecting sets of BindingConstraints
          // associated with the edge that produced the set
          val edgeConstraints = outbound map {
            case MergeEdge(`node`, other, _) => (other, findUnderconstrained(other, graph0))
            case MergeEdge(other, `node`, _) => (other, findUnderconstrained(other, graph0))
          }

          // compute all compatible BindingConstraints between this node's key set and the constraints
          // associated with each edge
          val nodeConstraints: Set[BindingConstraint] = edgeConstraints flatMap {
            case (other, constraints) => 
              // the computeCompatible call here should never return None; if it does, then the 
              // edge should have been pruned from the graph in the first place.
              constraints(other) map { 
                _ computeCompatible node.keys getOrElse sys.error("Edge from " + node + " to " + other + " is bad; incompatible constraint found.") 
              }
          }

          //Now, we need to compute the minimal disjoint set of constraints from the node constraints.
          //Note that the edge information is irrelevant; we can always find the edge constraints because
          //they are exactly the same as the constraints on the node at the other end of the edge.
          edgeConstraints.map(_._2).suml + (node -> BindingConstraints.minimize(nodeConstraints))
        }
      }

      // Each MergeNode corresponds to a set of MergeSpecs, each of which is keyed by the 
      // sequence of TicVar that define the tic variables. It occurs to me that TicVar
      // isn't actually the right type for the ticvar.
      //         
      // Composition rules:
      // 1. Relating tables sharing a source is intersection on identity
      // 2. Relating tables with disjoint sources is alignment on group keys
      // 3. Relating tables with disjoint group keys is a cross
      // 
      def buildMerges(clusters: Map[MergeNode, List[Binding]], graph: Map[MergeNode, Set[MergeEdge]]) = {
        /**
         * General algorithm:
         *  1) Compute alignment on keys between current node and remote node for each source in current node.
         *     This may produce multiple "versions" of a single source, filtered according to different key orderings.
         *  2) In any case where multiple versions of a source have been generated, produce a single source
         *     via identity intersection.
         *  3) Now, sort each source by the sort ordering specified by the inbound edge and align with one another
         *     according to group key value. This will produce a set of tables that are minimized with respect to one another
         *     and to the child nodes. However, these tables are not yet globally minimized within the subgraph.
         *  4) Since all sources within the node are now aligned to one another, any one of them can be used as
         *     the alignment target for the parent node (if any.) 
         *  5) What to return? In the simplest case (a leaf node) this will simply be a product containing
         *     a source table, a transspec defining the group key columns (which also gives the group sorting key),
         *     and a transspec defining the value columns. 
         *     In the case that a node is defined by more than one source, then we must return all of those sources
         *     aligned by group key on a common sequence of ticvars. Once aligned, any one of them can be used
         *     to provid filtering for the next node outward. 
         *     Returning a Map[MergeNode, KeyFilter] gives us a view of the entire tree, which will allow
         *     us to traverse again downward to perform the final right-alignment. That is, we left-align
         *     going up (removing elements from the derived sets higher in the tree) and then right-align
         *     to remove elements from the derived sets going back down to the leaves, at which point we're done.
         *
         *     To think of what we need to return in very simple terms, each node in the tree acts as a 
         *     supervisor for the nodes closer to the leaves. As a supervisor, the node collects and
         *     refines feedback, passes it upward, and then disseminates the information it has received
         *     from above when a final decision about what group keys to include has been reached.
         *
         *     Alignment evaluation should actually start top-down, then proceed bottom-up to obtain the final 
         *     set since alignments higher in the tree will result in greater amounts of data reduction, since
         *     more constraints are in play.
         */
        def buildMergeSpec(node: MergeNode, candidates: Set[Seq[TicVar]], graph: Map[MergeNode, Set[MergeEdge]], underConstrained: Map[MergeNode, Set[BindingConstraint]]): NodeMergeSpec = {
          // TODO: Factor our common traversal structure, for now it's copypasta
          if (graph.get(node).forall(_.isEmpty)) {
            // no outbound edges will exist, so we can just pick arbitrarily from the set of candidates;
            // we only need one ordering for values within a leaf node.
            val ordering = candidates.head
            NodeMergeSpec(
              ordering,
              (clusters(node) map { binding =>
                SortMergeSpec(SourceMergeSpec(binding), deriveTransSpec(binding.groupKeySpec, ordering))
              })(collection.breakOut)
            )
          } else {
            val outbound = graph(node)

            // subset the graph to only include edges that do not involve the current node
            val graph0: Map[MergeNode, Set[MergeEdge]] = (graph - node) map { 
              ((_: Set[MergeEdge]).filterNot({case MergeEdge(a, b, _) => a == node || b == node})).second 
            }

            def mergeEdge(constraints: Set[Seq[TicVar]], alignments: Set[MergeSpec], other: MergeNode): (Set[Seq[TicVar]], Set[MergeSpec]) = {
              // choose a single ordering that will be used to align tables of this node
              // with tables of that node
              val edgeConstraint: Seq[TicVar] = BindingConstraints.select(constraints, candidates, underConstrained(other))

              // Fix any underconstrained orderings for the remote node, ensuring that the ordering
              // we plan to use for the internode alignment is among the orderings that the remote set will
              // ultimately be sorted by.
              val otherFixed = BindingConstraints.fix(underConstrained(other), Some(edgeConstraint))

              val otherNodeSpec = buildMergeSpec(other, otherFixed, graph0, underConstrained)

              val alignSpec: List[MergeSpec] = clusters(node) map { binding =>
                val sourceSort = SortMergeSpec(
                  SourceMergeSpec(binding),
                  deriveTransSpec(binding.groupKeySpec, edgeConstraint)
                )
                
                LeftAlignMergeSpec(MergeAlignment(sourceSort, otherNodeSpec, edgeConstraint))
              }

              (constraints + edgeConstraint, alignments ++ alignSpec)
            }
                
            val (edgeConstraints, edgeAlignments) = outbound.foldLeft((Set.empty[Seq[TicVar]], Set.empty[MergeSpec])) {
              case ((constraints, alignments), MergeEdge(`node`, other, _)) => mergeEdge(constraints, alignments, other)
              case ((constraints, alignments), MergeEdge(other, `node`, _)) => mergeEdge(constraints, alignments, other)
            }

            val groupedIntersections = edgeAlignments groupBy {
              case LeftAlignMergeSpec(MergeAlignment(SortMergeSpec(source, _), _, _)) => source
            } 
            
            val intersections: Set[MergeSpec] = (groupedIntersections map {
              case (_, specs) => if (specs.size > 1) IntersectMergeSpec(specs) else specs.head
            })(collection.breakOut)

            NodeMergeSpec(edgeConstraints.head, intersections)
          }
        }

        val largestNode = (graph.toList maxBy { case (_, edges) => edges.size })._1
        val underConstrained = findUnderconstrained(largestNode, graph)

        // the initial set of fixed constraints; not all of these will eventually be used, but each 
        // edge visited will choose a member of this set to include in the final set of orderings
        // for this node
        val fixedConstraints = BindingConstraints.fix(underConstrained(largestNode))
        buildMergeSpec(largestNode, fixedConstraints, graph, underConstrained)
      }
    }

    def findBindingUniverses[GroupId](grouping: GroupingSpec[GroupId]): Seq[Universe] = {
      @inline def find0(v: Vector[(GroupingSource[GroupId], Vector[GroupKeySpec])]): Stream[Universe] = {
        val protoUniverses = (v map { case (src, specs) => specs map { (src, _) } toStream } toList).sequence 
        
        protoUniverses map { proto =>
          Universe(proto map { case (src, spec) => Binding(src.table, src.idTrans, src.targetTrans, spec) })
        }
      }

      import GroupKeySpec.{dnf, toVector}
      find0(grouping.sources map { source => (source, ((dnf _) andThen (toVector _)) apply source.groupKeySpec) })
    }

    def merge[GroupId: scalaz.Equal](grouping: GroupingSpec[GroupId])(body: (Table, GroupId => Table) => M[Table]): M[Table] = {
      def evaluateMergeSpecs(specs: MergeSpec*): M[Table] = {
        sys.error("todo")
      }

      val universes = findBindingUniverses(grouping)
      evaluateMergeSpecs(universes map { _.composeMergeSpec }: _*)
    }
  }

}
// vim: set ts=4 sw=4 et:
