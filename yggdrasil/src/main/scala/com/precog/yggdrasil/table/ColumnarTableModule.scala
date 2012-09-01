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
  type TableCompanion = ColumnarTableCompanion

  def newScratchDir(): File = Files.createTempDir()
  def jdbmCommitInterval: Long = 200000l

  object ops extends ColumnarTableCompanion

  trait ColumnarTableCompanion extends TableCompanionLike {
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

    def align(sources: (Table, trans.TransSpec1)*): M[Seq[Table]] = sys.error("todo")
    def intersect(sources: (Table, trans.TransSpec1)*): M[Table] = sys.error("todo")
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

        case ObjectConcat(objects @ _*) =>
          objects.map(composeSliceTransform2).reduceLeft { (l0, r0) =>
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
          }

        case ArrayConcat(elements @ _*) =>
          // array concats cannot reduce the number of columns except by eliminating empty columns
          elements.map(composeSliceTransform2).reduceLeft { (tacc, t2) => 
            tacc.zip(t2) { (sliceAcc, s2) =>
              new Slice {
                val size = sliceAcc.size
                val columns: Map[ColumnRef, Column] = {
                  val accCols = sliceAcc.columns collect { case (ref @ ColumnRef(JPath(JPathIndex(i), _*), ctype), col) => (i, ref, col) }
                  val s2cols = s2.columns collect { case (ref @ ColumnRef(JPath(JPathIndex(i), xs @ _*), ctype), col) => (i, xs, ref, col) }

                  if (accCols.isEmpty && s2cols.isEmpty) {
                    val intersectedEmptyColumn = for {
                      accEmpty <- (sliceAcc.columns collect { case (ColumnRef(JPath.Identity, CEmptyArray), col) => col }).headOption
                      s2Empty  <- (s2.columns       collect { case (ColumnRef(JPath.Identity, CEmptyArray), col) => col }).headOption
                    } yield {
                      (ColumnRef(JPath.Identity, CEmptyArray) -> new IntersectColumn(accEmpty, s2Empty) with EmptyArrayColumn)
                    } 
                    
                    intersectedEmptyColumn.toMap
                  } else if ((accCols.isEmpty && !sliceAcc.columns.keys.exists(_.ctype == CEmptyArray)) || 
                             (s2cols.isEmpty && !s2.columns.keys.exists(_.ctype == CEmptyArray))) {
                    Map.empty[ColumnRef, Column]
                  } else {
                    val maxId = accCols.map(_._1).max
                    val newCols = (accCols map { case (_, ref, col) => ref -> col }) ++ 
                                  (s2cols  map { case (i, xs, ref, col) => ColumnRef(JPath(JPathIndex(i + maxId + 1) :: xs.toList), ref.ctype) -> col })

                    newCols.toMap
                  }
                }
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

  trait Grouper extends GrouperLike {
    import trans._
    type TicVar = JPathField

    case class MergeAlignment(left: MergeSpec, right: MergeSpec, keys: Seq[TicVar])
    
    sealed trait MergeSpec
    case class SourceMergeSpec(binding: Binding, groupKeyTransSpec: TransSpec1, order: Seq[TicVar]) extends MergeSpec
    case class LeftAlignMergeSpec(alignment: MergeAlignment) extends MergeSpec
    case class IntersectMergeSpec(mergeSpecs: Set[MergeSpec]) extends MergeSpec
    case class NodeMergeSpec(ordering: Seq[TicVar], toAlign: Set[MergeSpec]) extends MergeSpec
    case class CrossMergeSpec(left: MergeSpec, right: MergeSpec) extends MergeSpec
    
    // The GroupKeySpec for a binding is comprised only of conjunctions that refer only
    // to members of the source table. The targetTrans defines a transformation of the
    // table to be used as the value output after keys have been derived. 
    // while Binding as the same general structure as GroupingSource, we keep it as a seperate type because
    // of the constraint that the groupKeySpec must be a conjunction, or just a single source clause. Reusing
    // the same type would be confusing
    case class Binding(source: Table, idTrans: TransSpec1, targetTrans: Option[TransSpec1], groupId: GroupId, groupKeySpec: GroupKeySpec) 

    // MergeTrees describe intersections as edges in a graph, where the nodes correspond
    // to sets of bindings
    case class MergeNode(keys: Set[TicVar], binding: Binding)
    object MergeNode {
      def apply(binding: Binding): MergeNode = MergeNode(Universe.sources(binding.groupKeySpec).map(_.key).toSet, binding)
    }

    /**
     * Represents an adjaceny based on a common subset of TicVars
     */
    class MergeEdge private[MergeEdge](val a: MergeNode, val b: MergeNode) {
      /** The common subset of ticvars shared by both nodes */
      val sharedKeys = a.keys & b.keys

      /** The set of nodes joined by this edge */
      val nodes = Set(a, b)
      def touches(node: MergeNode) = nodes.contains(node)

      /** The total set of keys joined by this edge (for alignment) */
      val keys: Set[TicVar] = a.keys ++ b.keys

      def joins(x: MergeNode, y: MergeNode) = (x == a && y == b) || (x == b && y == a)

      // Overrrides for set equality
      override def equals(other: Any) = other match {
        case e: MergeEdge => e.nodes == this.nodes
        case _ => false
      }
      override def hashCode() = nodes.hashCode()
      override def toString() = "MergeEdge(%s, %s)".format(a, b)
    }

    object MergeEdge {
      def apply(a: MergeNode, b: MergeNode) = new MergeEdge(a, b)
      def unapply(n: MergeEdge): Option[(MergeNode, MergeNode)] = Some((n.a, n.b))
    }

    // A maximal spanning tree for a merge graph, where the edge weights correspond
    // to the size of the shared keyset for that edge. We use hte maximal weights
    // since the larger the set of shared keys, the fewer constraints are imposed
    // making it more likely that a sorting for those shared keys can be reused.
    case class MergeGraph(nodes: Set[MergeNode], edges: Set[MergeEdge] = Set()) {
      def join(other: MergeGraph, edge: MergeEdge) = MergeGraph(nodes ++ other.nodes, edges ++ other.edges + edge)

      val edgesFor: Map[MergeNode, Set[MergeEdge]] = edges.foldLeft(nodes.map((_, Set.empty[MergeEdge])).toMap) {
        case (acc, edge @ MergeEdge(a, b)) => 
          val aInbound = acc(a) + edge
          val bInbound = acc(b) + edge
          acc + (a -> aInbound) + (b -> bInbound)
      }

      def adjacent(a: MergeNode, b: MergeNode) = {
        edges.find { e => (e.a == a && e.b == a) || (e.a == b && e.b == a) }.isDefined
      }

      val rootNode = (edgesFor.toList maxBy { case (_, edges) => edges.size })._1
      
      /*
      // A depth-first traversal of merge edges that results in a set of binding constraints for each node.
      // These binding constraints may be underconstrained (i.e. an element of the constraint sequence
      // may have arity > 1, so a second pass from the top down will be necessary to determine a fixed
      // ordering that then gets propagated downward.
      lazy val underconstrained: Map[MergeNode, Set[OrderingConstraint]] = {
        def find0(node: MergeNode, visited: Set[MergeNode], graph: Map[MergeNode, Set[MergeEdge]], acc: Map[MergeNode, Set[OrderingConstraint]]): Map[MergeNode, Set[OrderingConstraint]] = {

          if (graph.get(node).forall(_.isEmpty)) {
            acc += (node -> Set(OrderingConstraint(Seq(node.keys))))
          } else {
            val outbound = graph(node)

            // for all outbound edges, visit the remote node collecting sets of OrderingConstraints
            // associated with the edge that produced the set
            val remoteNodes: Set[MergeNode] = outbound map {
              case MergeEdge(`node`, other) => other
              case MergeEdge(other, `node`) => other
            }

            (remoteNodes diff visited).map(find0(other, visited + node, graph, acc))
            
            
            // compute all compatible OrderingConstraints between this node's key set and the constraints
            // associated with each edge
            val nodeConstraints: Set[OrderingConstraint] = edgeConstraints flatMap {
              case (other, constraints) => 
                // the computeCompatible call here should never return None; if it does, then the 
                // edge should have been pruned from the graph in the first place.
                constraints(other) map { 
                  _ computeCompatible node.keys 
                }
            }

            //Now, we need to compute the minimal disjoint set of constraints from the node constraints.
            //Note that the edge information is irrelevant; we can always find the edge constraints because
            //they are exactly the same as the constraints on the node at the other end of the edge.
            edgeConstraints.map(_._2).suml + (node -> OrderingConstraints.minimize(nodeConstraints))
          }
        }

        find0(rootNode, edgesFor)
      } 
      */
    }

    case class Universe(bindings: List[Binding]) {
      import Universe._

      def spanningGraphs: Set[MergeGraph] = {
        val clusters: Map[MergeNode, List[Binding]] = bindings groupBy { 
          case binding @ Binding(_, _, _, _, groupKeySpec) => MergeNode(sources(groupKeySpec).map(_.key).toSet, binding) 
        }

        findSpanningGraphs(edgeMap(clusters.keySet))
      }
    }

    case class OrderingConstraint(ordering: Seq[Set[TicVar]]) {
      /*
      //
      // ordering: [{'a, 'b, 'c}]
      //           [{'a, 'b}, {'c}]
      //           [{'b}, {'a}, {'c}]
      //
      // keys: {'a, 'b}
      //
      final def computeCompatible(keys: Set[TicVar]): OrderingConstraint = {
        @tailrec def computeCompatible0(keys: Set[TicVar], tail: Seq[Set[TicVar]], acc: Seq[Set[TicVar]]): OrderingConstraint = {
          if (tail.isEmpty) {
            // no more constraints are imposed by the existing constraint sequence, so we can just tack on the remaining key set
            // and be done
            OrderingConstraint(acc :+ keys)
          } else {
            val remainder = keys diff tail.head
            val intersection = tail.head intersect keys
            if (remainder.isEmpty && intersection.nonEmpty) {
              // no need to retain the rest of the tail, since the keys that form the restriction
              // are all that count
              OrderingConstraint(acc :+ intersection)
            } else if ((tail.head diff keys).isEmpty && intersection.nonEmpty) { 
              // the head of the tail is entirely contained in the key set, so recurse on the remainder
              computeCompatible(remainder, tail.tail, acc :+ tail.head)
            } else {
              OrderingConstraint(acc :+ remainder)
            } 
          }
        }

        computeCompatible0(keys, ordering, Vector())
      }
      */


      // Fix this binding constraint into a sort order. Any non-singleton TicVar sets will simply
      // be convered into an arbitrary sequence
      lazy val fixed = ordering.flatten
    }

    object OrderingConstraints {
      /**
       * Compute a new constraint that can replace both input constraints
       */
      def replacementFor(a: OrderingConstraint, b: OrderingConstraint): Option[OrderingConstraint] = {
        @tailrec
        def alignConstraints(left: Seq[Set[TicVar]], right: Seq[Set[TicVar]], computed: Seq[Set[TicVar]] = Seq()): Option[OrderingConstraint] = {
          if (left.isEmpty) {
            // left is a prefix or equal to the shifted right, so we can use computed :: right as our common constraint
            Some(OrderingConstraint(computed ++ right))
          } else if (right.isEmpty) {
            Some(OrderingConstraint(computed ++ left))
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

      /**
       * Given the set of input constraints, find a _minimal_ set of compatible OrderingConstraints that
       * covers that set.
       */
      def minimize(constraints: Set[OrderingConstraint]): Set[OrderingConstraint] = {
        @tailrec
        def reduce(unreduced: Set[OrderingConstraint], minimized: Set[OrderingConstraint]): Set[OrderingConstraint] = {
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
      /*
      def fix(constraints: Set[OrderingConstraint], hint: Option[Seq[TicVar]] = None): Set[Seq[TicVar]] = {
        val original = minimize(constraints)
        // Minimize the constraints, possibly including the hint, then flatten the minimal constraints into Seqs
        val newMin = minimize(hint.map { s => original + OrderingConstraint(s.map(Set(_))) }.getOrElse(original))
        
        if (newMin.size <= original.size) {
          newMin.map(_.ordering.flatten) 
        } else {
          sys.error("Hint could not be respected during fix. This indicates a bug in the grouping algorithm.")
        }
      }

      def select(candidates: Set[Seq[TicVar]], among: Set[OrderingConstraint]): Option[Seq[TicVar]] = {
        def isCompatible(seq: Seq[TicVar], constraint: Seq[Set[TicVar]]): Boolean = {
          seq.isEmpty || 
          ( constraint.nonEmpty && 
            constraint.head.contains(seq.head) && 
            {
              val remainder = (constraint.head - seq.head)
              isCompatible(seq.tail, if (remainder.nonEmpty) remainder +: constraint.tail else constraint.tail) 
            } )
        }

        (for (seq <- candidates; constraint <- among if isCompatible(seq, constraint.ordering)) yield seq).headOption
      }
      */
    }

    object Universe {
      def allEdges(nodes: collection.Set[MergeNode]): collection.Set[MergeEdge] = {
        for {
          l <- nodes
          r <- nodes
          if l != r
          sharedKey = l.keys intersect r.keys
          if sharedKey.nonEmpty
        } yield {
          MergeEdge(l, r)
        }
      }

      def edgeMap(nodes: collection.Set[MergeNode]): Map[MergeNode, Set[MergeEdge]] = {
        allEdges(nodes).foldLeft(nodes.map(n => n -> Set.empty[MergeEdge]).toMap) { 
          case (acc, edge @ MergeEdge(a, b)) => acc + (a -> (acc.getOrElse(a, Set()) + edge)) + (b -> (acc.getOrElse(b, Set()) + edge))
        } 
      }

      // a universe is a conjunction of binding clauses, which must contain no disjunctions
      def sources(spec: GroupKeySpec): Seq[GroupKeySpecSource] = (spec: @unchecked) match {
        case GroupKeySpecAnd(left, right) => sources(left) ++ sources(right)
        case src: GroupKeySpecSource => Vector(src)
      }

      // the GroupKeySpec passed to deriveTransSpecs must be either a source or a conjunction; all disjunctions
      // have been factored out by this point
      def deriveKeyTransSpec(conjunction: GroupKeySpec, keyOrder: Seq[TicVar] = Nil): TransSpec1 = {
        val keyMap = keyOrder.zipWithIndex.toMap

        // [avalue, bvalue]
        val keySpecs = sources(conjunction).sortBy(src => keyMap.getOrElse(src.key, Int.MaxValue)).zipWithIndex map { 
          case (src, idx) => WrapObject(src.spec, idx.toString)
        }
        
        ObjectConcat(keySpecs: _*)
      }

      // An implementation of our algorithm for finding a minimally connected set of graphs
      def findSpanningGraphs(outbound: Map[MergeNode, Set[MergeEdge]]): Set[MergeGraph] = {
        def isConnected(from: MergeNode, to: MergeNode, outbound: Map[MergeNode, Set[MergeEdge]], constraintSet: Set[TicVar]): Boolean = {
          outbound.getOrElse(from, Set()).exists {
            case edge @ MergeEdge(a, b) => 
              a == to || b == to ||
              {
                val other = if (a == from) b else a
                // the other node's keys must be a superset of the constraint set we're concerned with in order to traverse it.
                ((other.keys & constraintSet) == constraintSet) && {
                  val pruned = outbound mapValues { _ - edge }
                  isConnected(other,to, pruned, constraintSet)
                }
              }
          }
        }

        def find0(outbound: Map[MergeNode, Set[MergeEdge]], edges: Set[MergeEdge]): Map[MergeNode, Set[MergeEdge]] = {
          if (edges.isEmpty) {
            outbound
          } else {
            val edge = edges.head

            // node we're searching from
            val fromNode = edge.a
            val toNode = edge.b

            val pruned = outbound mapValues { _ - edge }

            find0(if (isConnected(fromNode, toNode, pruned, edge.keys)) pruned else outbound, edges.tail)
          }
        }

        def partition(in: Map[MergeNode, Set[MergeEdge]]): Set[MergeGraph] = {
          in.values.flatten.foldLeft(in.keySet map { k => MergeGraph(Set(k)) }) {
            case (acc, edge @ MergeEdge(a, b)) => 
              val g1 = acc.find(_.nodes.contains(a)).get
              val g2 = acc.find(_.nodes.contains(b)).get

              val resultGraph = g1.join(g2, edge)
              acc - g1 - g2 + resultGraph
          }
        }

        partition(find0(outbound, outbound.values.flatten.toSet))
      }


/*

      // Each MergeNode corresponds to a set of MergeSpecs, each of which is keyed by the 
      // sequence of TicVar that define the tic variables. It occurs to me that TicVar
      // isn't actually the right type for the ticvar.
      //         
      // Composition rules:
      // 1. Relating tables sharing a source is intersection on identity
      // 2. Relating tables with disjoint sources is alignment on group keys
      // 3. Relating tables with disjoint group keys is a cross
      // 
      def buildMerges(mergeGraph: MergeGraph) = {
        // 
        // General algorithm:
        //  1) Compute alignment on keys between current node and remote node for each source in current node.
        //     This may produce multiple "versions" of a single source, filtered according to different key orderings.
        //  2) In any case where multiple versions of a source have been generated, produce a single source
        //     via identity intersection.
        //  3) Now, sort each source by the sort ordering specified by the edgesFor edge and align with one another
        //     according to group key value. This will produce a set of tables that are minimized with respect to one another
        //     and to the child nodes. However, these tables are not yet globally minimized within the subgraph.
        //  4) Since all sources within the node are now aligned to one another, any one of them can be used as
        //     the alignment target for the parent node (if any.) 
        //  5) What to return? In the simplest case (a leaf node) this will simply be a product containing
        //     a source table, a transspec defining the group key columns (which also gives the group sorting key),
        //     and a transspec defining the value columns. 
        //     In the case that a node is defined by more than one source, then we must return all of those sources
        //     aligned by group key on a common sequence of ticvars. Once aligned, any one of them can be used
        //     to provid filtering for the next node outward. 
        //     Returning a Map[MergeNode, KeyFilter] gives us a view of the entire tree, which will allow
        //     us to traverse again downward to perform the final right-alignment. That is, we left-align
        //     going up (removing elements from the derived sets higher in the tree) and then right-align
        //     to remove elements from the derived sets going back down to the leaves, at which point we're done.
        // 
        //     To think of what we need to return in very simple terms, each node in the tree acts as a 
        //     supervisor for the nodes closer to the leaves. As a supervisor, the node collects and
        //     refines feedback, passes it upward, and then disseminates the information it has received
        //     from above when a final decision about what group keys to include has been reached.
        // 
        //     Alignment evaluation should actually start top-down, then proceed bottom-up to obtain the final 
        //     set since alignments higher in the tree will result in greater amounts of data reduction, since
        //     more constraints are in play.
        // 
        def buildMergeSpec(node: MergeNode, candidates: Set[Seq[TicVar]], adjacencies: Map[MergeNode, Set[MergeEdge]], underConstrained: Map[MergeNode, Set[OrderingConstraint]]): NodeMergeSpec = {
          // TODO: Factor our common traversal structure, for now it's copypasta
          if (adjacencies.get(node).forall(_.isEmpty)) {
            // no outbound edges will exist, so we can just pick arbitrarily from the set of candidates;
            // we only need one ordering for values within a leaf node.
            val ordering = candidates.head
            NodeMergeSpec(
              ordering,
              SourceMergeSpec(node.binding, deriveKeyTransSpec(node.binding.groupKeySpec, ordering), ordering)
            )
          } else {
            val outbound = adjacencies(node)

            // subset the adjacencies to only include edges that do not involve the current node
            val graph0: Map[MergeNode, Set[MergeEdge]] = (adjacencies - node) map { 
              ((_: Set[MergeEdge]).filterNot({case MergeEdge(a, b, _) => a == node || b == node})).second 
            }

            def mergeEdge(constraints: Set[Seq[TicVar]], alignments: Set[MergeSpec], other: MergeNode): (Set[Seq[TicVar]], Set[MergeSpec]) = {
              // choose a single ordering that will be used to align tables of this node
              // with tables of that node. The other node *must* return a MergeSpec with
              // keys in this order so that we have a natural alignment
              val edgeConstraint: Seq[TicVar] = OrderingConstraints.select(constraints, candidates, underConstrained(other))

              // Fix any underconstrained orderings for the remote node, ensuring that the ordering
              // we plan to use for the internode alignment is among the orderings that the remote set will
              // ultimately be sorted by.
              val otherFixed = OrderingConstraints.fix(underConstrained(other), Some(edgeConstraint))

              val otherNodeSpec = buildMergeSpec(other, otherFixed, graph0, underConstrained)

              val alignSpec: List[MergeSpec] = clusters(node) map { binding =>
                val groupKeyTransSpec = deriveKeyTransSpec(binding.groupKeySpec, edgeConstraint)
                val sourceSort = SourceMergeSpec(binding, groupKeyTransSpec, edgeConstraint)
                
                LeftAlignMergeSpec(MergeAlignment(sourceSort, otherNodeSpec, edgeConstraint))
              }

              (constraints + edgeConstraint, alignments ++ alignSpec)
            }
                
            val (edgeConstraints, edgeAlignments) = outbound.foldLeft((Set.empty[Seq[TicVar]], Set.empty[MergeSpec])) {
              case ((constraints, alignments), MergeEdge(`node`, other, _)) => mergeEdge(constraints, alignments, other)
              case ((constraints, alignments), MergeEdge(other, `node`, _)) => mergeEdge(constraints, alignments, other)
            }

            val groupedIntersections = edgeAlignments groupBy {
              case LeftAlignMergeSpec(MergeAlignment(source,  _, _)) => source
            } 
            
            val intersections: Set[MergeSpec] = (groupedIntersections map {
              case (_, specs) => if (specs.size > 1) IntersectMergeSpec(specs) else specs.head
            })(collection.breakOut)

            NodeMergeSpec(edgeConstraints.head, intersections)
          }
        }

        buildMergeSpec(mergeGraph.rootNode, fixedConstraints, mergeGraph.edgesFor, mergeGraph.underconstrained)
      }
      */
    }

    def findBindingUniverses(grouping: GroupingSpec): Seq[Universe] = {
      @inline def find0(v: Vector[(GroupingSource, Vector[GroupKeySpec])]): Stream[Universe] = {
        val protoUniverses = (v map { case (src, specs) => specs map { (src, _) } toStream } toList).sequence 
        
        protoUniverses map { proto =>
          Universe(proto map { case (src, spec) => Binding(src.table, src.idTrans, src.targetTrans, src.groupId, spec) })
        }
      }

      import GroupKeySpec.{dnf, toVector}
      find0(grouping.sources map { source => (source, ((dnf _) andThen (toVector _)) apply source.groupKeySpec) })
    }

    case class NodeSubset(node: MergeNode, table: Table, idTrans: TransSpec1, targetTrans: Option[TransSpec1], groupKeyTrans: TransSpec1, groupKeyOrder: Seq[TicVar]) {
      def alignedGroupKeyTrans(targetOrder: Seq[TicVar]): TransSpec1 = {
        if (groupKeyOrder == targetOrder) {
          groupKeyTrans
        } else {
          ObjectConcat(
            targetOrder map { ticvar => 
              WrapObject(
                DerefObjectStatic(groupKeyTrans, JPathField(groupKeyOrder.indexOf(ticvar).toString)), 
                targetOrder.indexOf(ticvar).toString)
          }: _*)
        }
      }
    }


    def findRequiredSorts(spanningGraph: MergeGraph): Map[MergeNode, Set[Seq[TicVar]]] = {
      findRequiredSorts(spanningGraph, spanningGraph.nodes.toList)
    }

    private[table] def findRequiredSorts(spanningGraph: MergeGraph, nodeList: List[MergeNode]): Map[MergeNode, Set[Seq[TicVar]]] = {
      import OrderingConstraints.minimize
      def inPrefix(seq: Seq[TicVar], keys: Set[TicVar], acc: Seq[TicVar] = Vector()): Option[Seq[TicVar]] = {
        if (keys.isEmpty) Some(acc) else {
          seq.headOption.flatMap { a => 
            if (keys.contains(a)) inPrefix(seq.tail, keys - a, acc :+ a) else None
          }
        }
      }

      def fix(nodes: List[MergeNode], underconstrained: Map[MergeNode, Set[OrderingConstraint]]): Map[MergeNode, Set[OrderingConstraint]] = {
        if (nodes.isEmpty) underconstrained else {
          val node = nodes.head
          val fixed = minimize(underconstrained(node)).map(_.ordering.flatten)
          val newUnderconstrained = spanningGraph.edgesFor(node).foldLeft(underconstrained) {
            case (acc, edge @ MergeEdge(a, b)) => 
              val other = if (a == node) b else a
              val edgeConstraint: Seq[TicVar] = 
                fixed.view.map(inPrefix(_, edge.sharedKeys)).collect({ case Some(seq) => seq }).head

              acc + (other -> (acc.getOrElse(other, Set()) + OrderingConstraint(edgeConstraint.map(Set(_)))))
          }

          fix(nodes.tail, newUnderconstrained)
        }
      }

      if (spanningGraph.edges.isEmpty) {
        spanningGraph.nodes.map(_ -> Set.empty[Seq[TicVar]]).toMap
      } else {
        val unconstrained = spanningGraph.edges.foldLeft(Map.empty[MergeNode, Set[OrderingConstraint]]) {
          case (acc, edge @ MergeEdge(a, b)) =>
            val edgeConstraint = OrderingConstraint(Seq(edge.sharedKeys))
            val aConstraints = acc.getOrElse(a, Set()) + edgeConstraint
            val bConstraints = acc.getOrElse(b, Set()) + edgeConstraint

            acc + (a -> aConstraints) + (b -> bConstraints)
        }
        
        fix(nodeList, unconstrained).mapValues(s => minimize(s).map(_.fixed))
      }
    }

    def alignOnEdges(spanningGraph: MergeGraph): M[Map[GroupId, Set[NodeSubset]]] = {
      import OrderingConstraints._

      // Compute required sort orders based on graph traversal
      val requiredSorts: Map[MergeNode, Set[Seq[TicVar]]] = findRequiredSorts(spanningGraph)

      val sortPairs: M[Stream[(MergeNode, Map[Seq[TicVar], NodeSubset])]] = requiredSorts.map {
        case (node, orders) => 
          val nodeSubsetsM: M[Set[(Seq[TicVar], NodeSubset)]] = orders.map { ticvars => 
            val sorted: M[Table] = node.binding.source.sort(Universe.deriveKeyTransSpec(node.binding.groupKeySpec, ticvars))
            sorted.map { sortedTable => 
              ticvars ->
              NodeSubset(node,
                         sortedTable,
                         TransSpec.deepMap(node.binding.idTrans) { case Leaf(_) => TransSpec1.DerefArray1 },
                         node.binding.targetTrans.map(t => TransSpec.deepMap(t) { case Leaf(_) => TransSpec1.DerefArray1 }),
                         TransSpec1.DerefArray0,
                         ticvars)
            }
          }.sequence

          nodeSubsetsM map { orders => node -> orders.toMap }
      }.toStream.sequence
      
      for {
        sorts <- sortPairs.map(_.toMap)
        groupedSubsets <- {
          val edgeAlignments = spanningGraph.edges flatMap {
            case MergeEdge(a, b) =>
              // Find the compatible sortings for this edge's endpoints
              val common: Set[(NodeSubset, NodeSubset)] = for {
                aVars <- sorts(a).keySet
                bVars <- sorts(b).keySet
                if aVars.startsWith(bVars) || bVars.startsWith(aVars)
              } yield {
                (sorts(a)(aVars), sorts(b)(bVars))
              }

              common map {
                case (aSorted, bSorted) => 
                  val alignedM = ops.align((aSorted.table, aSorted.groupKeyTrans), 
                                           (bSorted.table, bSorted.groupKeyTrans))
                  
                  for (alignedPair <- alignedM) yield {
                    alignedPair.toList match {
                      case aAligned :: bAligned :: Nil => List(
                        NodeSubset(a, aAligned, aSorted.idTrans, aSorted.targetTrans, aSorted.groupKeyTrans, aSorted.groupKeyOrder),
                        NodeSubset(b, bAligned, bSorted.idTrans, bSorted.targetTrans, bSorted.groupKeyTrans, bSorted.groupKeyOrder)
                    )
                  }
              }
            }
          }

          edgeAlignments.sequence
        }
      } yield {
        groupedSubsets.flatten.groupBy(_.node.binding.groupId)
      }
    }

    type ConnectedSubgraph = Set[NodeSubset]

    def intersect(set: Set[NodeSubset]): M[NodeSubset] = {
      sys.error("todo")
    }

    case class BorgResult(table: Table, groupKeyTrans: TransSpec1, idTrans: Map[GroupId, TransSpec1], rowTrans: Map[GroupId, TransSpec1])
    /* Take the distinctiveness of each node (in terms of group keys) and add it to the uber-cogrouped-all-knowing borgset */
    def borg(connectedGraph: ConnectedSubgraph): M[BorgResult] = {
      sys.error("todo")
    }

    def crossAll(borgResults: Set[BorgResult]): M[BorgResult] = {
      sys.error("todo")
    }

    // Create the omniverse
    def unionAll(borgResults: Set[BorgResult]): M[BorgResult] = {
      sys.error("todo")
    }

    /**
     * Merge controls the iteration over the table of group key values. 
     */
    def merge(grouping: GroupingSpec)(body: (Table, GroupId => M[Table]) => M[Table]): M[Table] = {
      // all of the universes will be unioned together.
      val universes = findBindingUniverses(grouping)
      val borgedUniverses: M[Stream[BorgResult]] = universes.toStream.map { universe =>
        val alignedSpanningGraphsM: M[Set[Map[GroupId, Set[NodeSubset]]]] = universe.spanningGraphs.map(alignOnEdges).sequence
        val minimizedSpanningGraphsM: M[Set[ConnectedSubgraph]] = for {
          spanningGraphs <- alignedSpanningGraphsM
          intersected    <- spanningGraphs.map(_.values.toStream.map(intersect).sequence).sequence
        } yield intersected.map(_.toSet)

        for {
          spanningGraphs <- minimizedSpanningGraphsM
          borgedGraphs <- spanningGraphs.map(borg).sequence
          crossed <- crossAll(borgedGraphs)
        } yield crossed
      }.sequence

      for {
        omniverse <- borgedUniverses.flatMap(s => unionAll(s.toSet))
        result <- omniverse.table.partitionMerge(omniverse.groupKeyTrans) { partition =>
          val groups: M[Map[GroupId, Table]] = 
            for {
              grouped <- omniverse.rowTrans.toStream.map{ 
                           case (groupId, rowTrans) => 
                             val recordTrans = ArrayConcat(WrapArray(omniverse.idTrans(groupId)), WrapArray(rowTrans))
                             val sortByTrans = TransSpec.deepMap(omniverse.idTrans(groupId)) { 
                                                 case Leaf(_) => TransSpec1.DerefArray1 
                                                }

                             partition.transform(recordTrans).sort(sortByTrans) map {
                               t => (groupId -> t.transform(DerefArrayStatic(TransSpec1.DerefArray1, JPathIndex(1))))
                             }
                         }.sequence
            } yield grouped.toMap

          body(
            partition.take(1).transform(omniverse.groupKeyTrans), 
            (groupId: GroupId) => groups.map(_(groupId))
          )
        }
      } yield result
    }
  }
}
// vim: set ts=4 sw=4 et:
