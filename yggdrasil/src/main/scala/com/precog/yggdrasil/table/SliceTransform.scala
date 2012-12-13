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

import com.precog.common.json._
import com.precog.common.{Path, VectorCase}
import com.precog.bytecode.{ JType, JBooleanT, JObjectUnfixedT, JArrayUnfixedT }
import com.precog.yggdrasil.jdbm3._
import com.precog.yggdrasil.util._
import com.precog.util.BitSet

import blueeyes.bkka.AkkaTypeClasses
import blueeyes.json._
import org.apache.commons.collections.primitives.ArrayIntList
import org.joda.time.DateTime
import com.google.common.io.Files
import com.weiglewilczek.slf4s.Logging

import org.apache.jdbm.DBMaker
import java.io.File
import java.util.SortedMap

import com.precog.util.{BitSet, BitSetUtil, Loop}
import com.precog.util.BitSetUtil.Implicits._

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

trait SliceTransforms[M[+_]] extends
    TableModule[M] with 
    ColumnarTableTypes with 
    ObjectConcatHelpers with 
    ArrayConcatHelpers {

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
        case Leaf(source) if source == Source || source == SourceLeft =>
          SliceTransform.left(())

        case Leaf(source) if source == SourceRight =>
          SliceTransform.right(())

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
                  cl <- sl.columns collect { case (ref, col) if ref.selector == CPath.Identity => col }
                  cr <- sr.columns collect { case (ref, col) if ref.selector == CPath.Identity => col }
                  result <- f(cl, cr)
                } yield result
                  
                resultColumns.groupBy(_.tpe) map { 
                  case (tpe, cols) => (ColumnRef(CPath.Identity, tpe), cols.reduceLeft((c1, c2) => Column.unionRightSemigroup.append(c1, c2)))
                }
              }
            }
          }

        case Filter(source, predicate) => 
          val typed = Typed(predicate, JBooleanT)
          composeSliceTransform2(source).zip(composeSliceTransform2(typed)) { (s: Slice, filter: Slice) =>
            assert(filter.size == s.size)
            
            if (s.columns.isEmpty) {
              s
            } else {

              val definedAt = new BitSet
              filter.columns.values.foreach {
                case col: BoolColumn => {
                  cf.util.isSatisfied(col).foreach {
                    c => definedAt.or(c.definedAt(0, s.size))
                  }
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
                val (paired, excludedLeft) = sl.columns.foldLeft((Map.empty[CPath, Column], Set.empty[Column])) {
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
                
                Map(ColumnRef(CPath.Identity, CBoolean) -> resultCol)
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
                  case (ref @ ColumnRef(CPath.Identity, tpe), col) if CType.canCompare(CType.of(value),tpe) => true
                  case _ => false
                }
                
                val comparable = comparable0.map(_._2).flatMap { col => Eq.partialRight(value)(col).map(_.asInstanceOf[BoolColumn]) }
                val other = other0.map(_._2).map { col => new Map1Column(col) with BoolColumn { def apply(row: Int) = false } }
                
                val columns = comparable ++ other
                val aggregate = new BoolColumn {
                  def isDefinedAt(row: Int) = columns.exists { _.isDefinedAt(row) }
                  def apply(row: Int)       = columns.exists { col => col.isDefinedAt(row) && col(row) }
                }
                
                Map(ColumnRef(CPath.Identity, CBoolean) -> (if(invert) complement(aggregate) else aggregate))
              }
            }
          }
        }

        case ConstLiteral(value, target) =>
          composeSliceTransform2(target) map { _.definedConst(value) }

        case WrapObject(source, field) =>
          composeSliceTransform2(source) map {
            _ wrap CPathField(field) 
          }

        case WrapArray(source) =>
          composeSliceTransform2(source) map {
            _ wrap CPathIndex(0) 
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

                  val columns: Map[ColumnRef, Column] = {
                    val leftObjects = filterObjects(sl.columns).values toList
                    val rightObjects = filterObjects(sr.columns).values toList

                    val leftEmpty = filterEmptyObjects(sl.columns).values toList
                    val rightEmpty = filterEmptyObjects(sr.columns).values toList
                    
                    val leftFields = filterFields(sl.columns)
                    val rightFields = filterFields(sr.columns)

                    val maskComplement = new BoolColumn {
                      def defined(row: Int) = check(leftObjects)(row) || check(rightObjects)(row)

                      def isDefinedAt(row: Int) = !defined(row)
                      def apply(row: Int) = true
                    }

                    val emptyComplement = new BoolColumn {
                      def defined(row: Int) =
                        (check(rightEmpty)(row) && check(leftEmpty)(row)) ||
                        (check(rightEmpty)(row) && !check(leftObjects)(row)) ||
                        (check(leftEmpty)(row) && !check(rightObjects)(row))

                      def isDefinedAt(row: Int) = !defined(row)
                      def apply(row: Int) = true
                    }

                    val result = emptyObjectCols(emptyComplement, size) ++
                      nonemptyObjectCols(leftFields, rightFields)
                    
                    result mapValues { col =>
                      cf.util.FilterComplement(maskComplement)(col).get
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
            objects map composeSliceTransform2 reduceLeft { (l0, r0) =>
              l0.zip(r0) { (sl, sr) =>
                new Slice {
                  val size = sl.size

                  val columns: Map[ColumnRef, Column] = {
                    if (sl.columns.isEmpty || sr.columns.isEmpty) {
                      Map.empty[ColumnRef, Column]
                    } else {
                      val leftObjects = filterObjects(sl.columns).values toList
                      val rightObjects = filterObjects(sr.columns).values toList

                      val leftEmpty = filterEmptyObjects(sl.columns).values toList
                      val rightEmpty = filterEmptyObjects(sr.columns).values toList

                      val leftFields = filterFields(sl.columns)
                      val rightFields = filterFields(sr.columns)
                      
                      val maskComplement = new BoolColumn {
                        def defined(row: Int) = check(leftObjects)(row) && check(rightObjects)(row)

                        def isDefinedAt(row: Int) = !defined(row)
                        def apply(row: Int) = true
                      }

                      val intersectComplement = new BoolColumn {
                        def defined(row: Int) = check(rightEmpty)(row) && check(leftEmpty)(row)

                        def isDefinedAt(row: Int) = !defined(row)
                        def apply(row: Int) = true
                      }

                      val result = emptyObjectCols(intersectComplement, size) ++
                        nonemptyObjectCols(leftFields, rightFields)
                      
                      result mapValues { col =>
                        cf.util.FilterComplement(maskComplement)(col).get   // assert
                      }
                    }
                  }
                }
              }
            }
          }

        case InnerArrayConcat(elements @ _*) =>
          if (elements.size == 1) {
            val typed = Typed(elements.head, JArrayUnfixedT)
            composeSliceTransform2(typed)
          } else {
            elements.map(composeSliceTransform2).reduceLeft { (l0, r0) =>
              l0.zip(r0) { (sl, sr) =>
                new Slice {
                  val size = sl.size
                  val columns: Map[ColumnRef, Column] = {
                    if (sl.columns.isEmpty || sr.columns.isEmpty) {
                      Map.empty[ColumnRef, Column]
                    } else {
                      val leftArrays = filterArrays(sl.columns).values toList
                      val rightArrays = filterArrays(sr.columns).values toList

                      val leftEmpty = filterEmptyArrays(sl.columns).values toList
                      val rightEmpty = filterEmptyArrays(sr.columns).values toList

                      val maskComplement = new BoolColumn {
                        def defined(row: Int) = check(leftArrays)(row) && check(rightArrays)(row)

                        def isDefinedAt(row: Int) = !defined(row)
                        def apply(row: Int) = true
                      }

                      val intersectComplement = new BoolColumn {
                        def defined(row: Int) = check(rightEmpty)(row) && check(leftEmpty)(row)

                        def isDefinedAt(row: Int) = !defined(row)
                        def apply(row: Int) = true
                      }

                      val result = emptyArrayCols(intersectComplement, size) ++
                        nonemptyArrayCols(sl.columns, sr.columns)

                      result mapValues { col =>
                        cf.util.FilterComplement(maskComplement)(col).get
                      }
                    }
                  }
                }
              }
            }
          }

        case OuterArrayConcat(elements @ _*) =>
          if (elements.size == 1) {
            val typed = Typed(elements.head, JArrayUnfixedT)
            composeSliceTransform2(typed)
          } else {
            elements.map(composeSliceTransform2).reduceLeft { (l0, r0) =>
              l0.zip(r0) { (sl, sr) =>
                new Slice {
                  val size = sl.size
                  val columns: Map[ColumnRef, Column] = {
                    val leftArrays = filterArrays(sl.columns).values toList
                    val rightArrays = filterArrays(sr.columns).values toList

                    val leftEmpty = filterEmptyArrays(sl.columns).values toList
                    val rightEmpty = filterEmptyArrays(sr.columns).values toList

                    val maskComplement = new BoolColumn {
                      def defined(row: Int) = check(leftArrays)(row) || check(rightArrays)(row)

                      def isDefinedAt(row: Int) = !defined(row)
                      def apply(row: Int) = true
                    }
          
                    val emptyComplement = new BoolColumn {
                      def defined(row: Int) =
                        (check(rightEmpty)(row) && check(leftEmpty)(row)) ||
                        (check(rightEmpty)(row) && !check(leftArrays)(row)) ||
                        (check(leftEmpty)(row) && !check(rightArrays)(row))

                      def isDefinedAt(row: Int) = !defined(row)
                      def apply(row: Int) = true
                    }

                    val result = emptyArrayCols(emptyComplement, size) ++
                      nonemptyArrayCols(sl.columns, sr.columns)

                    result mapValues { col =>
                      cf.util.FilterComplement(maskComplement)(col).get
                    }
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

        case DerefMetadataStatic(source, field) =>
          composeSliceTransform2(source) map {
            _ deref field
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
              case (ColumnRef(CPath.Identity, CString), c: StrColumn) => 
                new DerefSlice(slice, { case row: Int if c.isDefinedAt(row) => CPathField(c(row)) })
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
              case (ColumnRef(CPath.Identity, CLong), c: LongColumn) => 
                new DerefSlice(slice, { case row: Int if c.isDefinedAt(row) => CPathIndex(c(row).toInt) })

              case (ColumnRef(CPath.Identity, CDouble), c: DoubleColumn) => 
                new DerefSlice(slice, { case row: Int if c.isDefinedAt(row) => CPathIndex(c(row).toInt) })

              case (ColumnRef(CPath.Identity, CNum), c: NumColumn) => 
                new DerefSlice(slice, { case row: Int if c.isDefinedAt(row) => CPathIndex(c(row).toInt) })
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

trait ConcatHelpers {
  def check(columns: List[Column])(row: Int): Boolean = columns exists { _ isDefinedAt row }
}

trait ArrayConcatHelpers extends ConcatHelpers {
  def filterArrays(columns: Map[ColumnRef, Column]) = columns.filter {
    case (ColumnRef(CPath(CPathIndex(_), _ @ _*), _), _) => true
    case (ColumnRef(CPath.Identity, CEmptyArray), _) => true
    case _ => false
  }

  def filterEmptyArrays(columns: Map[ColumnRef, Column]) = columns.filter {
    case (ColumnRef(CPath.Identity, CEmptyArray), _) => true
    case _ => false
  }

  def collectIndices(columns: Map[ColumnRef, Column]) = columns.collect {
    case (ref @ ColumnRef(CPath(CPathIndex(i), xs @ _*), ctype), col) => (i, xs, ref, col)
  }

  def emptyArrayCols(column: BoolColumn, size: Int) = {
    val baseEmptyCol = EmptyArrayColumn(BitSetUtil.range(0,size))
    val emptyCol = cf.util.FilterComplement(column)(baseEmptyCol).get

    Map(ColumnRef(CPath.Identity, CEmptyArray) -> emptyCol)
  }

  def nonemptyArrayCols(left: Map[ColumnRef, Column], right: Map[ColumnRef, Column]) = {
    val leftIndices = collectIndices(left)
    val rightIndices = collectIndices(right)

    val maxId = if (leftIndices.isEmpty) -1 else leftIndices.map(_._1).max
    val newCols = (leftIndices map { case (_, _, ref, col) => ref -> col }) ++
                  (rightIndices map { case (i, xs, ref, col) => ColumnRef(CPath(CPathIndex(i + maxId + 1) :: xs.toList), ref.ctype) -> col })

    newCols.toMap
  }
}

trait ObjectConcatHelpers extends ConcatHelpers {
  def filterObjects(columns: Map[ColumnRef, Column]) = columns.filter {
    case (ColumnRef(CPath(CPathField(_), _ @ _*), _), _) => true
    case (ColumnRef(CPath.Identity, CEmptyObject), _) => true
    case _ => false
  }

  def filterEmptyObjects(columns: Map[ColumnRef, Column]) = columns.filter {
    case (ColumnRef(CPath.Identity, CEmptyObject), _) => true
    case _ => false
  }

  def filterFields(columns: Map[ColumnRef, Column]) = columns.filter {
    case (ColumnRef(CPath(CPathField(_), _ @ _*), _), _) => true
    case _ => false
  }

  def emptyObjectCols(column: BoolColumn, size: Int) = {
    val baseEmptyCol = EmptyObjectColumn(BitSetUtil.range(0,size))
    val emptyCol = cf.util.FilterComplement(column)(baseEmptyCol).get

    Map(ColumnRef(CPath.Identity, CEmptyObject) -> emptyCol)
  }

  def nonemptyObjectCols(leftFields: Map[ColumnRef, Column], rightFields: Map[ColumnRef, Column]) = {
    val (leftInner, leftOuter) = leftFields partition {
      case (ColumnRef(path, _), _) =>
        rightFields exists { case (ColumnRef(path2, _), _) => path == path2 }
    }
    
    val (rightInner, rightOuter) = rightFields partition {
      case (ColumnRef(path, _), _) =>
        leftFields exists { case (ColumnRef(path2, _), _) => path == path2 }
    }
    
    val innerPaths = Set(leftInner.keys map { _.selector } toSeq: _*)
    
    val mergedPairs: Set[(ColumnRef, Column)] = innerPaths flatMap { path =>
      val rightSelection = rightInner filter {
        case (ColumnRef(path2, _), _) => path == path2
      }
      
      val leftSelection = leftInner filter {
        case (ref @ ColumnRef(path2, _), _) =>
          path == path2 && !rightSelection.contains(ref)
      }
      
      val rightMerged = rightSelection map {
        case (ref, col) => {
          if (leftInner contains ref)
            ref -> cf.util.UnionRight(leftInner(ref), col).get
          else
            ref -> col
        }
      }
      
      rightMerged ++ leftSelection
    }

    leftOuter ++ rightOuter ++ mergedPairs
  }
}

