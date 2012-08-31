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

import util.CPathUtils

import com.precog.common.VectorCase
import com.precog.bytecode.{ JType, JUnionT, JPrimitiveType }
import com.precog.bytecode.{ JNumberT, JTextT, JBooleanT }
import com.precog.bytecode.{ JArrayHomogeneousT, JArrayFixedT, JArrayUnfixedT }
import com.precog.bytecode.{ JObjectFixedT, JObjectUnfixedT }

import com.precog.common.json._

import blueeyes.json._
import blueeyes.json.JsonAST._
import org.apache.commons.collections.primitives.ArrayIntList

import scala.annotation.tailrec
import scala.collection.breakOut
import scalaz._
import scalaz.Ordering._
import scalaz.Validation._
import scalaz.syntax.foldable._
import scalaz.syntax.semigroup._
import scalaz.std.iterable._

trait RowComparator {
  def compare(i1: Int, i2: Int): Ordering
}

trait Slice { source =>
  import Slice._

  def size: Int
  def isEmpty: Boolean = size == 0

  def columns: Map[ColumnRef, Column]

  def logicalColumns: JType => Set[Column] = { jtpe =>
    // TODO Use a flatMap and:
    // If ColumnRef(_, CArrayType(_)) and jType has a JArrayFixedT of this type,
    //   then we need to map these to multiple columns.
    // Else if Schema.includes(...), then return List(col).
    // Otherwise return Nil.
    columns collect {
      case (ColumnRef(jpath, ctype), col) if Schema.includes(jtpe, jpath, ctype) => col
    } toSet
  }

  lazy val valueColumns: Set[Column] = columns collect { case (ColumnRef(CPath.Identity, _), col) => col } toSet
  
  def isDefinedAt(row: Int) = columns.values.exists(_.isDefinedAt(row))

  def mapColumns(f: CF1): Slice = new Slice {
    val size = source.size
    val columns = source.columns flatMap {
      case (ref, col) =>
        if (ref.selector == CPath.Identity) f(col) map { (ref, _ ) }  
        else None
    }
  }

  def filterColumns(f: CF1): Slice = new Slice {
    val size = source.size
    val columns = source.columns flatMap {
      case (ref, col) => f(col) map { (ref, _ ) }  
    }
  }
  
  def definedConst(value: CValue): Slice = new Slice {
    val size = source.size
    val columns = {
      Map(
        value match {
          case CString(s) => (ColumnRef(CPath.Identity, CString), new StrColumn {
            def isDefinedAt(row: Int) = source.isDefinedAt(row)
            def apply(row: Int) = s
          })
          case CBoolean(b) => (ColumnRef(CPath.Identity, CBoolean), new BoolColumn {
            def isDefinedAt(row: Int) = source.isDefinedAt(row)
            def apply(row: Int) = b
          })
          case CLong(l) => (ColumnRef(CPath.Identity, CLong), new LongColumn {
            def isDefinedAt(row: Int) = source.isDefinedAt(row)
            def apply(row: Int) = l
          })
          case CDouble(d) => (ColumnRef(CPath.Identity, CDouble), new DoubleColumn {
            def isDefinedAt(row: Int) = source.isDefinedAt(row)
            def apply(row: Int) = d
          })
          case CNum(n) => (ColumnRef(CPath.Identity, CNum), new NumColumn {
            def isDefinedAt(row: Int) = source.isDefinedAt(row)
            def apply(row: Int) = n
          })
          case CDate(d) => (ColumnRef(CPath.Identity, CDate), new DateColumn {
            def isDefinedAt(row: Int) = source.isDefinedAt(row)
            def apply(row: Int) = d
          })
          case CNull => (ColumnRef(CPath.Identity, CNull), new NullColumn {
            def isDefinedAt(row: Int) = source.isDefinedAt(row)
          })
          case CEmptyObject => (ColumnRef(CPath.Identity, CEmptyObject), new EmptyObjectColumn {
            def isDefinedAt(row: Int) = source.isDefinedAt(row)
          })
          case CEmptyArray => (ColumnRef(CPath.Identity, CEmptyArray), new EmptyArrayColumn {
            def isDefinedAt(row: Int) = source.isDefinedAt(row)
          })
        }
      )
    }
  }

  def deref(node: CPathNode): Slice = new Slice {
    val size = source.size
    val columns = node match {
      case CPathIndex(i) => source.columns collect {
        case (ColumnRef(CPath(CPathArray, xs @ _*), CArrayType(elemType)), col: HomogeneousArrayColumn[_]) =>
          (ColumnRef(CPath(xs: _*), elemType), col.select(i))

        case (ColumnRef(CPath(CPathIndex(`i`), xs @ _*), ctype), col) =>
          (ColumnRef(CPath(xs: _*), ctype), col)
      }

      case _ => source.columns collect {
        case (ColumnRef(CPath(`node`, xs @ _*), ctype), col) =>
          (ColumnRef(CPath(xs: _*), ctype), col)
      }
    }
  }

  def wrap(wrapper: CPathNode): Slice = new Slice {
    val size = source.size

    // TODO This is a little weird; CPathArray actually wraps in CPathIndex(0).
    // Unfortunately, CArrayType(_) cannot wrap CNullTypes, so we can't just
    // arbitrarily wrap everything in a CPathArray.

    val columns = wrapper match {
      case CPathArray => source.columns map {
        case (ColumnRef(CPath(nodes @ _*), ctype), col) =>
          (ColumnRef(CPath(CPathIndex(0) +: nodes : _*), ctype), col)
      }
      case _ => source.columns map {
        case (ColumnRef(CPath(nodes @ _*), ctype), col) =>
          (ColumnRef(CPath(wrapper +: nodes : _*), ctype), col)
      }
    }
  }

  // ARRAYS:
  // TODO Here, if we delete a JPathIndex/JArrayFixedT, then we need to
  // construct a new Homo*ArrayColumn that has some indices missing.
  //
  // -- I've added a col.without(indicies) method to H*ArrayColumn to support
  // this operation.
  //
  def delete(jtype: JType): Slice = new Slice {
    def fixArrays(columns: Map[ColumnRef, Column]): Map[ColumnRef, Column] = {
      columns.toSeq.sortBy(_._1).foldLeft((Map.empty[Vector[CPathNode], Int], Map.empty[ColumnRef, Column])) {
        case ((arrayPaths, acc), (ColumnRef(jpath, ctype), col)) => 
          val (arrayPaths0, nodes) = jpath.nodes.foldLeft((arrayPaths, Vector.empty[CPathNode])) {
            case ((ap, nodes), CPathIndex(_)) => 
              val idx = ap.getOrElse(nodes, -1) + 1
              (ap + (nodes -> idx), nodes :+ CPathIndex(idx))

            case ((ap, nodes), fieldNode) => (ap, nodes :+ fieldNode)
          }

          (arrayPaths0, acc + (ColumnRef(CPath(nodes: _*), ctype) -> col))
      }._2
    }

    // Used for homogeneous arrays. Constructs a function, suitable for use in a
    // flatMap, that will modify the homogeneous array according to `jType`.
    //
    def flattenDeleteTree[A](jType: JType, cType: CValueType[A], cPath: CPath): A => Option[A] = {
      val delete: A => Option[A] = _ => None
      val retain: A => Option[A] = Some(_)

      (jType, cType, cPath) match {
        case (JUnionT(aJType, bJType), _, _) =>
          flattenDeleteTree(aJType, cType, cPath) andThen (_ flatMap flattenDeleteTree(bJType, cType, cPath))
        case (JTextT, CString, CPath.Identity) =>
          delete
        case (JBooleanT, CBoolean, CPath.Identity) =>
          delete
        case (JNumberT, CLong | CDouble | CNum, CPath.Identity) =>
          delete
        case (JObjectUnfixedT, _, CPath(CPathField(_), _*)) =>
          delete
        case (JObjectFixedT(fields), _, CPath(CPathField(name), cPath @ _*)) =>
          fields get name map (flattenDeleteTree(_, cType, CPath(cPath: _*))) getOrElse(retain)
        case (JArrayUnfixedT, _, CPath(CPathArray | CPathIndex(_), _*)) =>
          delete
        case (JArrayFixedT(elems), cType, CPath(CPathIndex(i), cPath @ _*)) =>
          elems get i map (flattenDeleteTree(_, cType, CPath(cPath: _*))) getOrElse (retain)
        case (JArrayFixedT(elems), CArrayType(cElemType), CPath(CPathArray, cPath @ _*)) =>
          val mappers = elems mapValues (flattenDeleteTree(_, cElemType, CPath(cPath: _*)))
          xs => Some(xs.zipWithIndex map { case (x, j) =>
            mappers get j match {
              case Some(f) => f(x)
              case None => x
            }
          })
        case (JArrayHomogeneousT(jType), CArrayType(cType), CPath(CPathArray, _*)) if Schema.ctypes(jType)(cType) =>
          delete
        case _ =>
          retain
      }
    }

    val size = source.size
    val columns = fixArrays(source.columns flatMap {
      case (ColumnRef(cpath, ctype), _) if Schema.includes(jtype, cpath, ctype) =>
        None

      case (ref @ ColumnRef(cpath, ctype: CArrayType[a]), col: HomogeneousArrayColumn[_]) if ctype == col.tpe =>
        val trans = flattenDeleteTree(jtype, ctype, cpath)
        Some((ref, new HomogeneousArrayColumn[a] {
          val tpe = ctype
          def isDefinedAt(row: Int) = col.isDefinedAt(row)
          def apply(row: Int): IndexedSeq[a] = trans(col(row).asInstanceOf[IndexedSeq[a]]) getOrElse sys.error("Oh dear, this cannot be happening to me.")
        }))

      case (ref, col) =>
        Some((ref, col))
    })
  }

  def deleteFields(prefixes: scala.collection.Set[CPathField]) = {
    new Slice {
      val size = source.size
      val columns = source.columns filterNot {
        case (ColumnRef(CPath(head @ CPathField(_), _ @ _*), _), _) => prefixes contains head
        case _ => false
      }
    }
  }

  def typed(jtpe : JType) : Slice = new Slice {
    val size = source.size
    val columns = {
      if(size == 0 || Schema.subsumes(source.columns.map { case (ColumnRef(path, ctpe), _) => (path, ctpe) }(breakOut), jtpe))
        source.columns filter {
          case (ColumnRef(path, ctpe), _) => Schema.requiredBy(jtpe, path, ctpe)
        }
      else
        Map.empty[ColumnRef, Column]
    }
  }

  def nest(selectorPrefix: CPath) = new Slice {
    val arraylessPrefix = CPath(selectorPrefix.nodes map {
      case CPathArray => CPathIndex(0)
      case n => n
    }: _*)

    val size = source.size
    val columns = source.columns map { case (ColumnRef(selector, ctype), v) => ColumnRef(arraylessPrefix \ selector, ctype) -> v }
  }

  def arraySwap(index: Int) = new Slice {
    val size = source.size
    val columns = source.columns.collect {
      case (ColumnRef(cPath @ CPath(CPathArray, _*), cType), col: HomogeneousArrayColumn[a]) =>
        (ColumnRef(cPath, cType), new HomogeneousArrayColumn[a] {
           val tpe = col.tpe
           def isDefinedAt(row: Int) = col.isDefinedAt(row)
           def apply(row: Int) = {
             val xs = col(row)
             if (xs.size == 0 || index >= xs.size) xs else {
               xs.updated(0, xs(index)).updated(index, xs(0))
             }
           }
        })
      case (ColumnRef(CPath(CPathIndex(0), xs @ _*), ctype), col) => 
        (ColumnRef(CPath(CPathIndex(index) +: xs : _*), ctype), col)

      case (ColumnRef(CPath(CPathIndex(`index`), xs @ _*), ctype), col) => 
        (ColumnRef(CPath(CPathIndex(0) +: xs : _*), ctype), col)

      case unchanged => unchanged
    }
  }

  // Takes an array where the indices correspond to indices in this slice,
  // and the values give the indices in the sparsened slice.
  def sparsen(index: Array[Int], toSize: Int): Slice = new Slice {
    val size = toSize
    val columns = source.columns mapValues { col => 
      cf.util.Sparsen(index, toSize)(col).get //sparsen is total
    }
  }

  def remap(indices: ArrayIntList) = new Slice {
    val size = indices.size
    val columns: Map[ColumnRef, Column] = source.columns mapValues { col => 
      cf.util.Remap.forIndices(indices).apply(col).get //Remap is total
    }
  }

  def map(from: CPath, to: CPath)(f: CF1): Slice = new Slice {
    val size = source.size
    val columns = source.columns flatMap {
                    case (ref, col) if ref.selector.hasPrefix(from) => f(col) map {v => (ref, v)}
                    case unchanged => Some(unchanged)
                  }
  }

  def map2(froml: CPath, fromr: CPath, to: CPath)(f: CF2): Slice = new Slice {
    val size = source.size

    val columns: Map[ColumnRef, Column] = {
      val resultColumns = for {
        left   <- source.columns collect { case (ref, col) if ref.selector.hasPrefix(froml) => col }
        right  <- source.columns collect { case (ref, col) if ref.selector.hasPrefix(fromr) => col }
        result <- f(left, right)
      } yield result

      resultColumns.groupBy(_.tpe) map { case (tpe, cols) => (ColumnRef(to, tpe), cols.reduceLeft((c1, c2) => Column.unionRightSemigroup.append(c1, c2))) }
    }
  }

  def compact(filter: Slice): Slice = {
    new Slice {
      lazy val retained =
        (0 until filter.size).foldLeft(new ArrayIntList) {
          case (acc, i) => if(filter.columns.values.exists(_.isDefinedAt(i))) acc.add(i) ; acc
        }

      lazy val size = retained.size
      lazy val columns: Map[ColumnRef, Column] = source.columns mapValues { col => (col |> cf.util.Remap.forIndices(retained)).get }
    }
  }

  def retain(refs: Set[ColumnRef]) = {
    new Slice {
      val size = source.size
      val columns: Map[ColumnRef, Column] = source.columns.filterKeys(refs)
    }
  }
  
  /**
   * Assumes that this and the previous slice (if any) are sorted.
   */
  def distinct(prevFilter: Option[Slice], filter: Slice): Slice = {
    new Slice {
      lazy val retained : ArrayIntList = {
        val acc = new ArrayIntList
        
        def findSelfDistinct(prevRow: Int, curRow: Int) = {
          val selfComparator = rowComparatorFor(filter, filter)(_.columns.keys.toList.sorted)
        
          @tailrec
          def findSelfDistinct0(prevRow: Int, curRow: Int) : ArrayIntList = {
            if(curRow >= filter.size) acc
            else {
              val retain = selfComparator.compare(prevRow, curRow) != EQ
              if(retain) acc.add(curRow)
              findSelfDistinct0(if(retain) curRow else prevRow, curRow+1)
            }
          }
          
          findSelfDistinct0(prevRow, curRow)
        }

        def findStraddlingDistinct(prev: Slice, prevRow: Int, curRow: Int) = {
          val straddleComparator = rowComparatorFor(prev, filter)(_.columns.keys.toList.sorted) 

          @tailrec
          def findStraddlingDistinct0(prevRow: Int, curRow: Int): ArrayIntList = {
            if(curRow >= filter.size) acc
            else {
              val retain = straddleComparator.compare(prevRow, curRow) != EQ
              if(retain) acc.add(curRow)
              if(retain)
                findSelfDistinct(curRow, curRow+1)
              else
                findStraddlingDistinct0(prevRow, curRow+1)
            }
          }

          findStraddlingDistinct0(prevRow, curRow)
        }
        
        val lastDefined = prevFilter.flatMap { slice =>
          (slice.size-1 to 0 by -1).find(row => slice.columns.values.exists(_.isDefinedAt(row))) }.map {
            (prevFilter.get, _)
          }
        
        val firstDefined = (0 until filter.size).find(i => filter.columns.values.exists(_.isDefinedAt(i)))

        (lastDefined, firstDefined) match {
          case (Some((prev, i)), Some(j)) => findStraddlingDistinct(prev, i, j)
          case (_,               Some(j)) => acc.add(j) ; findSelfDistinct(j, j+1)
          case _                          => acc
        }
      }

      lazy val size = retained.size
      lazy val columns: Map[ColumnRef, Column] = source.columns mapValues { col => (col |> cf.util.Remap.forIndices(retained)).get }
    }
  }

  def sortBy(refs: VectorCase[CPath]): Slice = {
    val sortedIndices: Array[Int] = {
      import java.util.Arrays
      val arr = Array.range(0, source.size)

      val comparator = new IntOrder {
        def order(i1: Int, i2: Int) = {
          var i = 0
          var result: Ordering = EQ
          //while (i < accessors.length && (result eq EQ)) {
            sys.error("todo")
          //}
          result
        }
      }

      Slice.qsort(arr, comparator)
      arr
    }

    source mapColumns cf.util.Remap(sortedIndices)
  }

  def split(idx: Int): (Slice, Slice) = (
    new Slice {
      val size = idx
      val columns = source.columns mapValues { col => (col |> cf.util.Remap({case i if i < idx => i})).get }
    },
    new Slice {
      val size = source.size - idx
      val columns = source.columns mapValues { col => (col |> cf.util.Remap({case i if i < size => i + idx})).get }
    }
  )

  def takeRange(startIndex: Int, numberToTake: Int): Slice = {
    new Slice {
      val size = numberToTake
      val columns = source.columns mapValues { 
        col => (col |> cf.util.Remap( { case i if i < numberToTake => i + startIndex} )).get 
      }
    }
  }

  def append(other: Slice): Slice = {
    new Slice {
      val size = source.size + other.size
      val columns = other.columns.foldLeft(source.columns) {
        case (acc, (ref, col)) => 
          val appendedCol = acc.get(ref) flatMap { sc => 
            cf.util.Concat(source.size)(sc, col)
          } getOrElse {
            (col |> cf.util.Shift(source.size)).get
          }

          acc + (ref -> appendedCol)
      }
    }
  }

  def zip(other: Slice): Slice = {
    new Slice {
      val size = source.size max other.size
      val columns: Map[ColumnRef, Column] = other.columns.foldLeft(source.columns) {
        case (acc, (ref, col)) => acc + (ref -> (acc get ref flatMap { c => cf.util.UnionRight(c, col) } getOrElse col))
      }
    }
  }

  def toJson(row: Int): Option[JValue] = {
    columns.foldLeft[JValue](JNothing) {
      case (jv, (ref @ ColumnRef(selector, _), col)) if col.isDefinedAt(row) => {
        CPathUtils.cPathToJPaths(selector, col.cValue(row)).foldLeft(jv) {
          case (jv, (path, value)) => jv.unsafeInsert(path, value.toJValue)
        }
      }

      case (jv, _) => jv
    } match {
      case JNothing => None
      case jv       => Some(jv)
    }
  }

  def toString(row: Int): Option[String] = {
    (columns collect { case (ref, col) if col.isDefinedAt(row) => ref.toString + ": " + col.strValue(row) }) match {
      case Nil => None
      case l   => Some(l.mkString("[", ", ", "]")) 
    }
  }

  override def toString = (0 until size).map(toString).mkString("\n")
}

object Slice {
  def apply(columns0: Map[ColumnRef, Column], dataSize: Int) = {
    new Slice {
      val size = dataSize
      val columns = columns0
    }
  }

  // scalaz order isn't @specialized
  trait IntOrder {
    def order(i1: Int, i2: Int): Ordering
  }

  private val MIN_QSORT_SIZE = 7; 

  def qsort(x: Array[Int], ord: IntOrder): Unit = {
    val random = new java.util.Random();
    qsortPartial(x, 0, x.length-1, ord, random);
    isort(x, ord);
  }

  private def isort(x: Array[Int], ord: IntOrder): Unit = {
    @tailrec def sort(i: Int): Unit = if (i < x.length) {
      val t = x(i);
      var j = i;
      while(j > 0 && (ord.order(t, x(j-1)) eq LT)) { x(j) = x(j-1); j -= 1 } 
      x(j) = t;
      sort(i + 1)
    }

    sort(0)
  }

  private def qsortPartial(x: Array[Int], lower: Int, upper: Int, ord: IntOrder, random: java.util.Random): Unit = {
    if (upper - lower >= MIN_QSORT_SIZE) {
      swap(x, lower, lower + random.nextInt(upper-lower+1));
      val t = x(lower);
      var i = lower;
      var j = upper + 1;
      var cont = true
      while (cont) {
        do { i += 1 } while (i <= upper && (ord.order(x(i), t) eq LT))
        do { j -= 1 } while (ord.order(t, x(j)) eq LT)
        if (i > j) cont = false
        swap(x, i, j)
      }
    }
  }

  @inline 
  private def swap(xs: Array[Int], i: Int, j: Int) {
    val temp = xs(i);
    xs(i) = xs(j);
    xs(j) = temp;
  }

  def rowComparatorFor(s1: Slice, s2: Slice)(keyf: Slice => List[ColumnRef]): RowComparator = {
    def compare0(cols: (Column, Column)): RowComparator = {
      (cols: @unchecked) match {
        case (c1: BoolColumn, c2: BoolColumn) => new RowComparator {
          def compare(thisRow: Int, thatRow: Int) = {
            val thisVal = c1(thisRow) 
            if (thisVal == c2(thatRow)) EQ else if (thisVal) GT else LT
          }
        }

        case (c1: LongColumn, c2: LongColumn) => new RowComparator {
          def compare(thisRow: Int, thatRow: Int) = {
            val thisVal = c1(thisRow)
            val thatVal = c2(thatRow)
            if (thisVal > thatVal) GT else if (thisVal == thatVal) EQ else LT
          }
        }

        case (c1: LongColumn, c2: DoubleColumn) => new RowComparator {
          def compare(thisRow: Int, thatRow: Int) = {
            val thisVal = c1(thisRow)
            val thatVal = c2(thatRow)
            if (thisVal > thatVal) GT else if (thisVal == thatVal) EQ else LT
          }
        }

        case (c1: LongColumn, c2: NumColumn) => new RowComparator {
          def compare(thisRow: Int, thatRow: Int) = {
            val thisVal = c1(thisRow)
            val thatVal = c2(thatRow)
            if (thisVal > thatVal) GT else if (thisVal == thatVal) EQ else LT
          }
        }

        case (c1: DoubleColumn, c2: LongColumn) => new RowComparator {
          def compare(thisRow: Int, thatRow: Int) = {
            val thisVal = c1(thisRow)
            val thatVal = c2(thatRow)
            if (thisVal > thatVal) GT else if (thisVal == thatVal) EQ else LT
          }
        }

        case (c1: DoubleColumn, c2: DoubleColumn) => new RowComparator {
          def compare(thisRow: Int, thatRow: Int) = {
            val thisVal = c1(thisRow)
            val thatVal = c2(thatRow)
            if (thisVal > thatVal) GT else if (thisVal == thatVal) EQ else LT
          }
        }

        case (c1: DoubleColumn, c2: NumColumn) => new RowComparator {
          def compare(thisRow: Int, thatRow: Int) = {
            val thisVal = BigDecimal(c1(thisRow))
            val thatVal = c2(thatRow)
            if (thisVal > thatVal) GT else if (thisVal == thatVal) EQ else LT
          }
        }

        case (c1: NumColumn, c2: LongColumn) => new RowComparator {
          def compare(thisRow: Int, thatRow: Int) = {
            val thisVal = c1(thisRow)
            val thatVal = BigDecimal(c2(thatRow))
            if (thisVal > thatVal) GT else if (thisVal == thatVal) EQ else LT
          }
        }

        case (c1: NumColumn, c2: DoubleColumn) => new RowComparator {
          def compare(thisRow: Int, thatRow: Int) = {
            val thisVal = c1(thisRow)
            val thatVal = BigDecimal(c2(thatRow))
            if (thisVal > thatVal) GT else if (thisVal == thatVal) EQ else LT
          }
        }

        case (c1: NumColumn, c2: NumColumn) => new RowComparator {
          val ord = Order[BigDecimal]
          def compare(thisRow: Int, thatRow: Int) = {
            ord.order(c1(thisRow), c2(thatRow))
          }
        }

        // TODO This should be more efficient... Also, should check if c1.tpe =~ c2.tpe (modulo num types).
        case (c1: HomogeneousArrayColumn[_], c2: HomogeneousArrayColumn[_]) => new RowComparator {
          val cmps = Stream.from(0) map { i => compare0(c1.select(i), c2.select(i)) }

          def compare(thisRow: Int, thatRow: Int) = {
            val c1size = c1(thisRow).size
            val c2size = c2(thisRow).size
            
            cmps take (c1size min c2size) map (_.compare(thisRow, thatRow)) find (_ != EQ) getOrElse {
              if (c1size < c2size) LT
              else if (c1size > c2size) GT
              else EQ
            }
          }
        }

        case (c1: StrColumn, c2: StrColumn) => new RowComparator {
          val ord = Order[String]
          def compare(thisRow: Int, thatRow: Int) = {
            ord.order(c1(thisRow), c2(thatRow))
          }
        }

        case (c1: DateColumn, c2: DateColumn) => new RowComparator {
          def compare(thisRow: Int, thatRow: Int) = {
            val thisVal = c1(thisRow)
            val thatVal = c2(thatRow)
            if (thisVal isAfter thatVal) GT else if (thisVal == thatVal) EQ else LT
          }
        }

        case (c1: EmptyObjectColumn, c2: EmptyObjectColumn) => new RowComparator {
          def compare(thisRow: Int, thatRow: Int) = EQ
        }

        case (c1: EmptyArrayColumn, c2: EmptyArrayColumn) => new RowComparator {
          def compare(thisRow: Int, thatRow: Int) = EQ
        }

        case (c1: NullColumn, c2: NullColumn) => new RowComparator {
          def compare(thisRow: Int, thatRow: Int) = EQ
        }
      }
    }

    val refs1 = keyf(s1)
    val refs2 = keyf(s2)

    // Return the first column in the array defined at the row, or -1 if none are defined for that row
    @inline def firstDefinedIndexFor(columns: Array[Column], row: Int): Int = {
      var i = 0
      while (i < columns.length && ! columns(i).isDefinedAt(row)) { i += 1 }
      if (i == columns.length) -1 else i
    }

    // Returns 2 columns for the intersection between ref1 and ref2.
    def intersection(ref1: ColumnRef, ref2: ColumnRef): (Column, Column) = {
      val col1 = s1.columns(ref1)
      val col2 = s2.columns(ref2)

      if (ref1.selector == ref2.selector) (col1, col2) else {
        @tailrec
        def rec(ps1: List[CPathNode], ps2: List[CPathNode], col1: Column, col2: Column): (Column, Column) =
          (ps1, ps2, col1, col2) match {
            case (Nil, Nil, _, _) =>
              (col1, col2)
            case (CPathArray :: ps1, CPathIndex(i) :: ps2, col1: HomogeneousArrayColumn[_], _) =>
              rec(ps1, ps2, col1.select(i), col2)
            case (CPathIndex(i) :: ns1, CPathArray :: ns2, _, col2: HomogeneousArrayColumn[_]) =>
              rec(ps1, ps2, col1, col2.select(i))
            case (p1 :: ps1, p2 :: ps2, _, _) =>
              rec(ps1, ps2, col1, col2)
          }

        rec(ref1.selector.nodes, ref2.selector.nodes, col1, col2)
      }
    }

    @inline def genComparatorFor(l1: List[ColumnRef], l2: List[ColumnRef]): RowComparator = {
      val array1: Array[Column] = l1.map(s1.columns)(collection.breakOut)
      val array2: Array[Column] = l2.map(s2.columns)(collection.breakOut)


      // Build an array of pairwise comparator functions for later use
      val comparators: Array[RowComparator] = (for {
        i1 <- 0 until array1.length
        i2 <- 0 until array2.length
      } yield compare0(array1(i1), array2(i2)))(collection.breakOut)

      new RowComparator {
        def compare(i: Int, j: Int) = {
          val first1 = firstDefinedIndexFor(array1, i)
          val first2 = firstDefinedIndexFor(array2, j)

          // In the following, undefined always sorts LT defined values
          if (first1 == -1 && first2 == -1) {
            EQ
          } else if (first1 == -1) {
            LT
          } else if (first2 == -1) {
            GT
          } else {
            // We have the indices, so use it to look up the comparator for the rows
            comparators(first1 * array2.length + first2).compare(i, j)
          }
        }
      }
    }


    @inline @tailrec
    def pairColumns(l1: List[ColumnRef], l2: List[ColumnRef], comparators: List[RowComparator]): List[RowComparator] = (l1, l2) match {
      case (h1 :: t1, h2 :: t2) if h1.selector == h2.selector => {
        val (l1Equal, l1Rest) = l1.partition(_.selector == h1.selector)
        val (l2Equal, l2Rest) = l2.partition(_.selector == h2.selector)

        pairColumns(l1Rest, l2Rest, genComparatorFor(l1Equal, l2Equal) :: comparators)
      }

      case (h1 :: t1, h2 :: t2) if CPathUtils.intersect(h1.selector, h2.selector).isDefined => {

        // The union of h1 & h2 form a k-dimensional sub-space of array indices (N^k).
        // For a fixed set of `ColumnRef`s that are either axis-aligned
        // subspaces or single points, we construct an oracle that, given a point in
        // N^k, returns all the `ColumnRef`s that intersect that point.

        // Constructing the oracle is simple; for each dim, we project all `ColumnRef`s
        // onto it. Each projection will either be a single point or a it'll span
        // the entire dimension. We create a Map[Int, Set[ColumnRef]] for the single
        // points of intersection and then create a Set[ColumnRef] for all the
        // columns that span the entire dimension. We can then answer a query in this
        // projection by returning the union of the `ColumnRef`s that span the dim and
        // the `ColumnRef`s at the point projected on the dimension itself.
        // To find the set of `ColumnRef`s that intersect a single point, we just
        // intersect all the answers for each dimension individually together.

        val Some(path) = CPathUtils.union(h1.selector, h2.selector)

        val (l1Equal, l1Rest) = l1 partition { ref =>
          CPathUtils.intersect(path, path).isDefined
        }
        val (l2Equal, l2Rest) = l2 partition { ref =>
          CPathUtils.intersect(path, path).isDefined
        }

        sealed trait Step[+A]
        case object Inc extends Step[Nothing]
        case object Shift extends Step[Nothing]
        case class Done[A](a: A) extends Step[A]

        def walkArraySpace[A](dimension: Int)(f: List[Int] => Step[A]): Option[A] = {
          def rec(left: List[Int], lvl: Int): Step[A] = if (lvl > 0) {
            @inline @tailrec def loop(x: Int): Step[A] = rec(x :: left, lvl - 1) match {
              case Inc => loop(x + 1)
              case Shift if x > 0 => Inc
              case step => step
            }

            loop(0)
          } else {
            f(left)
          }

          rec(Nil, dimension) match {
            case Done(a) => Some(a)
            case _ => None
          }
        }

        // Is this enough? Do we need to recurse? If not, we can blank out all CPathIndex's.
        val space = (l1Equal ++ l2Equal).map(_.selector).foldLeft(path)(CPathUtils.union(_, _).get)
        val dimension = space.nodes.foldLeft(0) {
          case (acc, CPathArray) => acc + 1
          case (acc, _) => acc
        }

        // Projections are either a single point (Some) or span the entire dimension (None).
        def projections(cPath: CPath): List[Option[Int]] =
          (cPath.nodes zip space.nodes).foldLeft(Nil: List[Option[Int]]) {
            case (acc, (CPathIndex(x), CPathArray)) => Some(x) :: acc
            case (acc, (CPathArray, CPathArray)) => None :: acc
            case (acc, _) => acc
          }.reverse

        def oracle(cols: List[ColumnRef]): List[Int] => List[ColumnRef] = {
          type Col = (ColumnRef, Int)
          val all = cols.zipWithIndex.toSet

          val colProjections = all.foldLeft(List.fill(dimension)((Map.empty[Int, Set[Col]], Set.empty[Col]))) {
            case (acc, col @ (ColumnRef(cPath, _), _)) =>
              (acc zip projections(cPath)) map {
                case ((points, ranges), Some(x)) =>
                  (points + (x -> (points.getOrElse(x, Set.empty[Col]) + col)), ranges)
                case ((points, ranges), None) =>
                  (points, ranges + col)
              }
          }

          // Note: Anytime ranges == all, we can drop that dimension.

          p => (colProjections zip p).foldLeft(all) { case (acc, ((points, ranges), x)) =>
            acc intersect (points(x) union ranges)
          }.toList.sortBy(_._2).map(_._1)
        }

        val l1Oracle = oracle(l1Equal)
        val l2Oracle = oracle(l2Equal)

        @tailrec
        def narrow(ps: List[CPathNode], is: List[Int], col: Column): Option[Column] = {
          (ps, is, col) match {
            case (CPathIndex(i) :: ps, j :: is, col: Column) if i == j =>
              narrow(ps, is, col)
            case (CPathArray :: ps, i :: is, col: HomogeneousArrayColumn[_]) =>
              narrow(ps, is, col.select(i))
            case (CPathIndex(_) :: _, _, _) =>
              None
            case (p :: ps, _, _) =>
              narrow(ps, is, col)
            case (Nil, Nil, _) =>
              Some(col)
            case (Nil, _, _) =>
              None
            case (_, Nil, _) =>
              None
          }
        }

        val columns1 = s1.columns
        val columns2 = s2.columns

        val cmp = new RowComparator {
          def compare(row1: Int, row2: Int) = walkArraySpace(dimension) { p =>
            val refs1 = l1Oracle(p.reverse)
            val refs2 = l2Oracle(p.reverse)

            val col1 = refs1 map { ref => ref -> columns1(ref) } flatMap {
              case (ColumnRef(path, tpe), col) => narrow(path.nodes, p, col)
            } find (_ isDefinedAt row1)

            val col2 = refs2 map { ref => ref -> columns2(ref) } flatMap {
              case (ColumnRef(path, tpe), col) => narrow(path.nodes, p, col)
            } find (_ isDefinedAt row2)

            
            (col1, col2) match {
              case (Some(col1), Some(col2)) =>
                val cmp = compare0(col1, col2).compare(row1, row2)
                if (cmp == EQ) Inc else Done(cmp)
              case (Some(_), None) => Done(GT)
              case (None, Some(_)) => Done(LT)
              case (None, None) => Shift
                // TODO ^^ this is not enough for this case. We need to see if any single
                // point projections exist beyond this and, if so, we need to Inc instead.
            }
          } getOrElse EQ
        }

        pairColumns(l1Rest, l2Rest, cmp :: comparators)
      }

      case (h1 :: t1, h2 :: t2) if h1.selector < h2.selector => {
        val (l1Equal, l1Rest) = l1.partition(_.selector == h1.selector)

        pairColumns(l1Rest, l2, genComparatorFor(l1Equal, Nil) :: comparators)
      }

      case (h1 :: t1, h2 :: t2) if h1.selector > h2.selector => {
        val (l2Equal, l2Rest) = l2.partition(_.selector == h2.selector)

        pairColumns(l1, l2Rest, genComparatorFor(Nil, l2Equal) :: comparators)
      }

      case (h1 :: t1, Nil) => {
        val (l1Equal, l1Rest) = l1.partition(_.selector == h1.selector)

        pairColumns(l1Rest, Nil, genComparatorFor(l1Equal, Nil) :: comparators)
      }

      case (Nil, h2 :: t2) => {
        val (l2Equal, l2Rest) = l2.partition(_.selector == h2.selector)

        pairColumns(Nil, l2Rest, genComparatorFor(Nil, l2Equal) :: comparators)
      }

      case (Nil, Nil) => comparators.reverse

      case (h1 :: t1, h2 :: t2) => sys.error("selector guard failure in pairColumns")
    }

    val comparators: Array[RowComparator] = pairColumns(refs1, refs2, Nil).toArray

    new RowComparator {
      def compare(i1: Int, i2: Int) = {
        var i = 0
        var result: Ordering = EQ

        while (i < comparators.length && result == EQ) {
          result = comparators(i).compare(i1, i2)
          i += 1
        }
        
        result
      }
    }
  }
}
