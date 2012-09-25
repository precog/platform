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
import com.precog.bytecode.{ JType, JObjectUnfixedT }
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

trait SliceTransforms[M[+_]] extends TableModule[M] with ColumnarTableTypes {
  import TableModule._
  import trans._
  import trans.constants._

  protected object SliceTransform {
    def identity[A](initial: A) = SliceTransform1[A](initial, (a: A, s: Slice) => (a, s))
    def left[A](initial: A)  = SliceTransform2[A](initial, (a: A, sl: Slice, sr: Slice) => (a, sl))
    def right[A](initial: A) = SliceTransform2[A](initial, (a: A, sl: Slice, sr: Slice) => (a, sr))

    def lift(f: Slice => Slice): SliceTransform1[Unit] = SliceTransform1[Unit]((), Function.untupled(f.second[Unit]))
 
    def composeSliceTransform(spec: TransSpec1): SliceTransform1[_] = {
      composeSliceTransform2(spec).parallel
    }

    // No transform defined herein may reduce the size of a slice. Be it known!
    def composeSliceTransform2(spec: TransSpec[SourceType]): SliceTransform2[_] = {
      val result = spec match {
        case Leaf(source) if source == Source || source == SourceLeft => SliceTransform.left(())
        case Leaf(source) if source == SourceRight => SliceTransform.right(())

        case Map1(source, f) => 
          composeSliceTransform2(source) map {
            _ mapRoot f 
          }

        case Map2(left, right, f) =>
          val l0 = composeSliceTransform2(left)
          val r0 = composeSliceTransform2(right)

          l0.zip(r0) { (sl, sr) =>
            new Slice {
              val size = sl.size
              val columns: Map[ColumnRef, Column] = {
                val resultColumns = for {
                  cl <- sl.columns collect { case (ref, col) if ref.selector == JPath.Identity => col }
                  cr <- sr.columns collect { case (ref, col) if ref.selector == JPath.Identity => col }
                  result <- f(cl, cr)
                } yield result
                  
                resultColumns.groupBy(_.tpe) map { 
                  case (tpe, cols) => (ColumnRef(JPath.Identity, tpe), cols.reduceLeft((c1, c2) => Column.unionRightSemigroup.append(c1, c2)))
                }
              }
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

              s mapColumns { cf.util.filter(0, s.size, definedAt) }
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
                                  ctype <- List(CLong, CDouble, CNum)
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
                            case (col: BoolColumn) => col.isDefinedAt(row) && col(row)
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
                  case (ColumnRef(selector, tpe), col) 
                    if tpe.isNumeric && sl.columns.keys.forall { case ColumnRef(`selector`, ctype) => !ctype.isNumeric ; case _ => true } => col

                  case (ref @ ColumnRef(_, tpe), col)
                    if !tpe.isNumeric && !sl.columns.contains(ref) => col
                })

                val resultCol = new MemoBoolColumn(
                  new BoolColumn {
                    def isDefinedAt(row: Int): Boolean = {
                      (sl.columns exists { case (_, c) => c.isDefinedAt(row) }) || (sr.columns exists { case (_, c) => c.isDefinedAt(row) }) 
                    }

                    def apply(row: Int): Boolean = {
                      // if any excluded column exists for the row, unequal
                      !excluded.exists(_.isDefinedAt(row)) && 
                       // if any paired column compares unequal, unequal
                      paired.exists { 
                        case (_, equal: BoolColumn) if equal.isDefinedAt(row) => equal(row)
                        case _ => false
                      }
                    }
                  })
                
                Map(ColumnRef(JPath.Identity, CBoolean) -> resultCol)
              }
            }
          }

        case EqualLiteral(source, value, invert) => {
          val id = System.currentTimeMillis
          import cf.std.Eq

          val sourceSlice = composeSliceTransform2(source)

          def complement(col: BoolColumn) = new BoolColumn {
            def isDefinedAt(row: Int) = col.isDefinedAt(row)
            def apply(row: Int) = !col(row)
          }
          
          sourceSlice map { ss =>
            new Slice {
              val size = ss.size
              val columns = {
                val (comparable0, other0) = ss.columns.toList.partition {
                  case (ref @ ColumnRef(JPath.Identity, tpe), col) if CType.canCompare(CType.of(value),tpe) => true
                  case _ => false
                }
                
                val comparable = comparable0.map(_._2).flatMap { col => Eq.partialRight(value)(col).map(_.asInstanceOf[BoolColumn]) }
                val other = other0.map(_._2).map { col => new Map1Column(col) with BoolColumn { def apply(row: Int) = false } }
                
                val columns = comparable ++ other
                val aggregate = new BoolColumn {
                  def isDefinedAt(row: Int) = columns.exists { _.isDefinedAt(row) }
                  def apply(row: Int)       = columns.exists { col => col.isDefinedAt(row) && col(row) }
                }
                
                Map(ColumnRef(JPath.Identity, CBoolean) -> (if(invert) complement(aggregate) else aggregate))
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

        case OuterObjectConcat(objects @ _*) =>
          if (objects.size == 1) {
            val typed = Typed(objects.head, JObjectUnfixedT)
            composeSliceTransform2(typed) 
          } else {
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

                    val finalCols = remapped ++ sr.columns

                    // We should never return a slice with zero columns in an outer object concat
                    if (finalCols.size == 0) {
                      Map(ColumnRef(JPath.Identity, CEmptyObject) -> new EmptyObjectColumn {
                        def isDefinedAt(row: Int) = sl.isDefinedAt(row) || sr.isDefinedAt(row)
                      })
                    } else {
                      finalCols
                    }
                  }
                }
              }
            }
          }

        case InnerObjectConcat(objects @ _*) =>
          if (objects.size == 1) {
            val typed = Typed(objects.head, JObjectUnfixedT)
            composeSliceTransform2(typed) 
          } else {
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
                      val emptyArrayCol = new IntersectColumn(accEmpty, s2Empty) with EmptyArrayColumn
                      (ColumnRef(JPath.Identity, CEmptyArray) -> emptyArrayCol)
                    } 

                    intersectedEmptyColumn.toMap
                  } else if ((accCols.isEmpty && !sliceAcc.columns.keys.exists(_.ctype == CEmptyArray)) || 
                             (s2cols.isEmpty && !s2.columns.keys.exists(_.ctype == CEmptyArray))) {
                    Map.empty[ColumnRef, Column]
                  } else {
                    val maxId = if (accCols.isEmpty) -1 else accCols.map(_._1).max
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
                val (newState, newCols) = scanner.scan(state, slice.columns, 0 until slice.size)
                val newSlice = new Slice {
                  val size = slice.size
                  val columns = newCols
                }

                (newState, newSlice)
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

        case FilterDefined(source, definedFor, definedness) =>
          val sourceTransform = composeSliceTransform2(source)
          val keyTransform = composeSliceTransform2(definedFor)

          sourceTransform.zip(keyTransform) { (s1, s2) => s1.filterDefined(s2, definedness) }
      }
      
      result.withSource(spec)
    }  
  }

  protected case class SliceTransform1[A](initial: A, f: (A, Slice) => (A, Slice)) {
    def apply(s: Slice) = f(initial, s)

    def advance(s: Slice): (SliceTransform1[A], Slice)  = {
      val (a0, s0) = f(initial, s)
      (this.copy(initial = a0), s0)
    }

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

}

// vim: set ts=4 sw=4 et:
