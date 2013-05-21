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

import com.precog.common._
import com.precog.common.Path
import com.precog.util.VectorCase
import com.precog.bytecode.{ JType, JBooleanT, JObjectUnfixedT, JArrayUnfixedT }
import com.precog.yggdrasil.jdbm3._
import com.precog.yggdrasil.util._
import com.precog.util._

import blueeyes.bkka._
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

trait SliceTransforms[M[+_]] extends TableModule[M]
    with ColumnarTableTypes 
    with ObjectConcatHelpers 
    with ArrayConcatHelpers
    with MapUtils {

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
      //todo missing case WrapObjectDynamic
      val result = spec match {
        case Leaf(source) if source == Source || source == SourceLeft =>
          SliceTransform.left(())

        case Leaf(source) if source == SourceRight =>
          SliceTransform.right(())

        case Map1(source, f) => 
          composeSliceTransform2(source) map {
            _ mapRoot f 
          }

        case DeepMap1(source, f) => 
          composeSliceTransform2(source) map {
            _ mapColumns f 
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
                  case (tpe, cols) =>
                    val col = cols reduceLeft { (c1, c2) =>
                      Column.unionRightSemigroup.append(c1, c2)
                    }
                    (ColumnRef(CPath.Identity, tpe), col)
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
          
          /*
           * 1. split each side into numeric and non-numeric columns
           * 2. cogroup non-numerics by ColumnRef
           * 3. for each pair of matched columns, compare by cf.std.Eq
           * 4. when matched column is undefined, return true iff other side is undefined
           * 5. reduce non-numeric matches by And
           * 6. analogously for numeric columns, save for the following
           * 7. for numeric columns with multiple possible types, consider all
           *    matches and reduce the results by And
           * 8. reduce numeric and non-numeric results by And
           * 9. mask definedness on output column by definedness on input columns
           *
           * Generally speaking, for each pair of columns cogrouped by path, we
           * compute a single equality column which is defined everywhere and
           * definedness behaves according to the following truth table:
           *
           * - v1 = v2 == <equality>
           * - v1 = undefined = false
           * - undefined = v2 = false
           * - undefined = undefined = true
           * 
           * The undefined comparison is required because we are comparing columns
           * that may be nested inside a larger object.  We don't have the information
           * at the pair level to determine whether the answer should be
           * undefined for the entire row, or if the current column pair simply
           * does not contribute to the final truth value.  Thus, we return a
           * "non-contributing" value (the zero for boolean And) and mask definedness
           * as a function of all columns.  A computed result will be overridden
           * by undefined (masked off) if *either* the LHS or the RHS is fully
           * undefined (for all columns) at a particular row.
           */

          l0.zip(r0) { (sl, sr) =>
            new Slice {
              val size = sl.size
              val columns: Map[ColumnRef, Column] = {
                val (leftNonNum, leftNum) = sl.columns partition {
                  case (ColumnRef(_, CLong | CDouble | CNum), _) => false
                  case _ => true
                }
                
                val (rightNonNum, rightNum) = sr.columns partition {
                  case (ColumnRef(_, CLong | CDouble | CNum), _) => false
                  case _ => true
                }
                
                val groupedNonNum = (leftNonNum mapValues { _ :: Nil }) cogroup (rightNonNum mapValues { _ :: Nil })
                
                val simplifiedGroupNonNum = groupedNonNum map {
                  case (_, Left3(column)) => Left(column)
                  case (_, Right3(column)) => Left(column)
                  case (_, Middle3((left :: Nil, right :: Nil))) => Right((left, right))
                }

                class FuzzyEqColumn(left: Column, right: Column) extends BoolColumn {
                  val equality = cf.std.Eq(left, right).get.asInstanceOf[BoolColumn]     // yay!
                  def isDefinedAt(row: Int) = (left isDefinedAt row) || (right isDefinedAt row)
                  def apply(row: Int) = equality.isDefinedAt(row) && equality(row)
                }
                
                val testedNonNum: Array[BoolColumn] = simplifiedGroupNonNum.map({
                  case Left(column) => new BoolColumn {
                    def isDefinedAt(row: Int) = column.isDefinedAt(row)
                    def apply(row: Int) = false
                  }
                  
                  case Right((left, right)) =>
                    new FuzzyEqColumn(left, right)

                })(collection.breakOut)
                
                // numeric stuff
                
                def stripTypes(cols: Map[ColumnRef, Column]) = {
                  cols.foldLeft(Map[CPath, Set[Column]]()) {
                    case (acc, (ColumnRef(path, _), column)) => {
                      val set = acc get path map { _ + column } getOrElse Set(column)
                      acc.updated(path, set)
                    }
                  }
                }
                
                val leftNumMulti = stripTypes(leftNum)
                val rightNumMulti = stripTypes(rightNum)
                
                val groupedNum = leftNumMulti cogroup rightNumMulti
                
                val simplifiedGroupedNum = groupedNum map {
                  case (_, Left3(column)) => Left(column): Either[Column, (Set[Column], Set[Column])]
                  case (_, Right3(column)) => Left(column): Either[Column, (Set[Column], Set[Column])]
                  
                  case (_, Middle3((left, right))) =>
                    Right((left, right)): Either[Column, (Set[Column], Set[Column])]
                }
                
                val testedNum: Array[BoolColumn] = simplifiedGroupedNum.map({
                  case Left(column) =>
                    new BoolColumn {
                      def isDefinedAt(row: Int) = column.isDefinedAt(row)
                      def apply(row: Int) = false
                    }
                  
                  case Right((left, right)) =>
                    val tests: Array[BoolColumn] = (for (l <- left; r <- right) yield {
                      new FuzzyEqColumn(l, r)
                    }).toArray
                    new OrLotsColumn(tests)
                })(collection.breakOut)

                val unifiedNonNum = new AndLotsColumn(testedNonNum)
                val unifiedNum = new AndLotsColumn(testedNum)
                val unified = new BoolColumn {
                  def isDefinedAt(row: Int): Boolean = unifiedNonNum.isDefinedAt(row) || unifiedNum.isDefinedAt(row)
                  def apply(row: Int): Boolean = {
                    val left = !unifiedNonNum.isDefinedAt(row) || unifiedNonNum(row)
                    val right = !unifiedNum.isDefinedAt(row) || unifiedNum(row)
                    left && right
                  }
                }

                val mask = sl.definedAt & sr.definedAt
                val column = new BoolColumn {
                  def isDefinedAt(row: Int) = mask(row) && unified.isDefinedAt(row)
                  def apply(row: Int) = unified(row)
                }
                
                Map(ColumnRef(CPath.Identity, CBoolean) -> column)
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
                    val (leftObjectBits, leftEmptyBits) = buildFilters(sl.columns, sl.size, filterObjects, filterEmptyObjects)
                    val (rightObjectBits, rightEmptyBits) = buildFilters(sr.columns, sr.size, filterObjects, filterEmptyObjects)
                    
                    val (leftFields, rightFields) = buildFields(sl.columns, sr.columns)

                    val emptyBits = buildOuterBits(leftEmptyBits, rightEmptyBits, leftObjectBits, rightObjectBits)

                    val emptyObjects = buildEmptyObjects(emptyBits)
                    val nonemptyObjects = buildNonemptyObjects(leftFields, rightFields)

                    emptyObjects ++ nonemptyObjects
                  }
                }
              }
            }
          }

        case InnerObjectConcat(objects @ _*) =>
          /**
           * This test is for special casing object concats when we know we
           * won't have any unions, or funky behaviour arising from empty
           * objects.
           */
          def isDisjoint(s1: Slice, s2: Slice): Boolean = {
            false // TODO: We really want to optimize the case where
                  // we are constructing a simple object from some
                  // other object where usually the definedness is equal
                  // on both sides, so we can just ++ the columns. But,
                  // we need to be a bit smarter about checking for equal
                  // definedness.

            // def containsEmptyObject(slice: Slice): Boolean =
            //   slice.columns.exists(_._1.ctype == CEmptyObject)

            // if (containsEmptyObject(s1) || containsEmptyObject(s2))
            //   return false

            // val keys = s1.columns.map(_._1.selector).toSet
            // !s2.columns.map(_._1.selector).exists(keys)
          }

          if (objects.size == 1) {
            val typed = Typed(objects.head, JObjectUnfixedT)
            composeSliceTransform2(typed) 
          } else {
            objects map composeSliceTransform2 reduceLeft { (l0, r0) =>
              l0.zip(r0) { (sl0, sr0) =>
                val sl = sl0.typed(JObjectUnfixedT) // Help out the special cases.
                val sr = sr0.typed(JObjectUnfixedT)

                new Slice {
                  val size = sl.size

                  val columns: Map[ColumnRef, Column] = {
                    if (sl.columns.isEmpty || sr.columns.isEmpty) {
                      Map.empty[ColumnRef, Column]
                    } else if (isDisjoint(sl, sr)) {
                      // If we know sl & sr are disjoint, which is often the
                      // case for queries where objects are constructed
                      // manually, then we can do a lot less work.
                      sl.columns ++ sr.columns
                    } else {
                      val (leftObjectBits, leftEmptyBits) = buildFilters(sl.columns, sl.size, filterObjects, filterEmptyObjects)
                      val (rightObjectBits, rightEmptyBits) = buildFilters(sr.columns, sr.size, filterObjects, filterEmptyObjects)
                    
                      val (leftFields, rightFields) = buildFields(sl.columns, sr.columns)

                      val (emptyBits, nonemptyBits) = buildInnerBits(leftEmptyBits, rightEmptyBits, leftObjectBits, rightObjectBits)

                      val emptyObjects = buildEmptyObjects(emptyBits)
                      val nonemptyObjects = buildNonemptyObjects(leftFields, rightFields)
                      
                      val result = emptyObjects ++ nonemptyObjects
                      
                      result lazyMapValues { col =>
                        cf.util.filter(0, sl.size max sr.size, nonemptyBits)(col).get
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
                    val (leftArrayBits, leftEmptyBits) = buildFilters(sl.columns, sl.size, filterArrays, filterEmptyArrays)
                    val (rightArrayBits, rightEmptyBits) = buildFilters(sr.columns, sr.size, filterArrays, filterEmptyArrays)
                    
                    val emptyBits = buildOuterBits(leftEmptyBits, rightEmptyBits, leftArrayBits, rightArrayBits)
                    
                    val emptyArrays = buildEmptyArrays(emptyBits)
                    val nonemptyArrays = buildNonemptyArrays(sl.columns, sr.columns)

                    emptyArrays ++ nonemptyArrays
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
                      val (leftArrayBits, leftEmptyBits) = buildFilters(sl.columns, sl.size, filterArrays, filterEmptyArrays)
                      val (rightArrayBits, rightEmptyBits) = buildFilters(sr.columns, sr.size, filterArrays, filterEmptyArrays)
                    
                      val (emptyBits, nonemptyBits) = buildInnerBits(leftEmptyBits, rightEmptyBits, leftArrayBits, rightArrayBits)

                      val emptyArrays = buildEmptyArrays(emptyBits)
                      val nonemptyArrays = buildNonemptyArrays(sl.columns, sr.columns)

                      val result = emptyArrays ++ nonemptyArrays

                      result lazyMapValues { col =>
                        cf.util.filter(0, sl.size max sr.size, nonemptyBits)(col).get
                      }
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

        case TypedSubsumes(source, tpe) =>
          composeSliceTransform2(source) map {
            _ typedSubsumes tpe
          }

        case IsType(source, tpe) =>
          composeSliceTransform2(source) map {
            _ isType tpe
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
          
        case Cond(pred, left, right) => {
          val predTransform = composeSliceTransform2(pred)
          val leftTransform = composeSliceTransform2(left)
          val rightTransform = composeSliceTransform2(right)
          
          predTransform.zip2(leftTransform, rightTransform) { (predS, leftS, rightS) =>
            new Slice {
              val size = predS.size
              
              val columns: Map[ColumnRef, Column] = {
                predS.columns get ColumnRef(CPath.Identity, CBoolean) map { predC =>
                  val leftMask = predC.asInstanceOf[BoolColumn].asBitSet(false, size)
                  
                  val rightMask = predC.asInstanceOf[BoolColumn].asBitSet(true, size)
                  rightMask.flip(0, size)
                  
                  val grouped = (leftS.columns mapValues { _ :: Nil }) cogroup (rightS.columns mapValues { _ :: Nil })
                  
                  val joined: Map[ColumnRef, Column] = grouped.map({
                    case (ref, Left3(col)) =>
                      ref -> cf.util.filter(0, size, leftMask)(col).get
                    
                    case (ref, Right3(col)) =>
                      ref -> cf.util.filter(0, size, rightMask)(col).get
                    
                    case (ref, Middle3((left :: Nil, right :: Nil))) => {
                      val left2 = cf.util.filter(0, size, leftMask)(left).get
                      val right2 = cf.util.filter(0, size, rightMask)(right).get
                      
                      ref -> cf.util.MaskedUnion(leftMask)(left2, right2).get    // safe because types are grouped
                    }
                  })(collection.breakOut)
                  
                  joined
                } getOrElse Map[ColumnRef, Column]()
              }
            }
          }
        }
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

    def zip2[B, C](t: SliceTransform1[B], t2: SliceTransform1[C])(combine: (Slice, Slice, Slice) => Slice): SliceTransform1[(A, B, C)] = {
      SliceTransform1(
        (initial, t.initial, t2.initial),
        { case ((a, b, c), s) =>
            val (a0, sa) = f(a, s)
            val (b0, sb) = t.f(b, s)
            val (c0, sc) = t2.f(c, s)
            assert(sa.size == sb.size)
            assert(sb.size == sc.size)
            ((a0, b0, c0), combine(sa, sb, sc))
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

    def advance(s1: Slice, s2: Slice): (SliceTransform2[A], Slice) = {
      val (a0, s0) = f(initial, s1, s2)
      (this.copy(initial = a0), s0)
    }

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

    def zip2[B, C](t: SliceTransform2[B], t2: SliceTransform2[C])(combine: (Slice, Slice, Slice) => Slice): SliceTransform2[(A, B, C)] = {
      SliceTransform2(
        (initial, t.initial, t2.initial),
        { case ((a, b, c), sl, sr) =>
            val (a0, sa) = f(a, sl, sr)
            val (b0, sb) = t.f(b, sl, sr)
            val (c0, sc) = t2.f(c, sl, sr)
            assert(sa.size == sb.size)
            assert(sb.size == sc.size)
            ((a0, b0, c0), combine(sa, sb, sc))
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
  def buildFilters(columns: Map[ColumnRef, Column], size: Int, filter: Map[ColumnRef, Column] => Map[ColumnRef, Column], filterEmpty: Map[ColumnRef, Column] => Map[ColumnRef, Column]) = {
    val definedBits = filter(columns).values.map(_.definedAt(0, size)).reduceOption(_ | _) getOrElse new BitSet
    val emptyBits = filterEmpty(columns).values.map(_.definedAt(0, size)).reduceOption(_ | _) getOrElse new BitSet
    (definedBits, emptyBits)
  }

  def buildOuterBits(leftEmptyBits: BitSet, rightEmptyBits: BitSet, leftDefinedBits: BitSet, rightDefinedBits: BitSet): BitSet = {
    (rightEmptyBits & leftEmptyBits) |
    (rightEmptyBits &~ leftDefinedBits) |
    (leftEmptyBits &~ rightDefinedBits)
  }

  def buildInnerBits(leftEmptyBits: BitSet, rightEmptyBits: BitSet, leftDefinedBits: BitSet, rightDefinedBits: BitSet) = {
    val emptyBits = rightEmptyBits & leftEmptyBits
    val nonemptyBits = leftDefinedBits & rightDefinedBits
    (emptyBits, nonemptyBits)
  }
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

  def buildEmptyArrays(emptyBits: BitSet) = Map(ColumnRef(CPath.Identity, CEmptyArray) -> EmptyArrayColumn(emptyBits))

  def buildNonemptyArrays(left: Map[ColumnRef, Column], right: Map[ColumnRef, Column]) = {
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

  def buildFields(leftColumns: Map[ColumnRef, Column], rightColumns: Map[ColumnRef, Column]) =
    (filterFields(leftColumns), filterFields(rightColumns))

  def buildEmptyObjects(emptyBits: BitSet) = {
    if (emptyBits.isEmpty) Map.empty[ColumnRef, Column]
    else Map(ColumnRef(CPath.Identity, CEmptyObject) -> EmptyObjectColumn(emptyBits))
  }

  def buildNonemptyObjects(leftFields: Map[ColumnRef, Column], rightFields: Map[ColumnRef, Column]) = {
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

