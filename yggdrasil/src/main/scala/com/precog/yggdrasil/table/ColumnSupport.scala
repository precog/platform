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

import org.joda.time.DateTime

import com.precog.util.{BitSet, BitSetUtil, Loop}
import com.precog.util.BitSetUtil.Implicits._

import scala.annotation.tailrec
import org.apache.commons.collections.primitives.ArrayIntList

class BitsetColumn(definedAt: BitSet) { this: Column =>
  def isDefinedAt(row: Int): Boolean = definedAt(row)

  override def toString = {
    val limit = definedAt.max
    val repr = (row: Int) => if (definedAt(row)) 'x' else '_'
    getClass.getName + "(" + (0 until limit).map(repr).mkString("[", ",", "]") + ", " + limit + ")"
  }
}

object BitsetColumn {
  def bitset(definedAt: Seq[Boolean]) = {
    val bs = new BitSet
    var i = 0
    definedAt.foreach { v =>
      if (v) bs.set(i)
      i += 1
    }
    bs
  }
}

class Map1Column(c: Column) { this: Column =>
  def isDefinedAt(row: Int) = c.isDefinedAt(row)
}

class Map2Column(c1: Column, c2: Column) { this: Column =>
  def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row)
}

class UnionColumn[T <: Column](c1: T, c2: T) { this: T =>
  def isDefinedAt(row: Int) = c1.isDefinedAt(row) || c2.isDefinedAt(row)
}

class IntersectColumn[T <: Column](c1: T, c2: T) { this: T =>
  def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row)
}

class IntersectLotsColumn[T <: Column](cols: Set[Column]) { this: T =>
  def isDefinedAt(row: Int) = cols.toList(0).isDefinedAt(row) && cols.toList(1).isDefinedAt(row)
}

class ConcatColumn[T <: Column](at: Int, c1: T, c2: T) { this: T =>
  def isDefinedAt(row: Int) = row >= 0 && ((row < at && c1.isDefinedAt(row)) || (row >= at && c2.isDefinedAt(row - at)))
}

class NConcatColumn[T <: Column](offsets: Array[Int], columns: Array[T]) { this: T =>

  // Is this worth it? For some operations, but not sorting... all the more
  // reason to add materialise I suppose.

  @volatile private var lastIndex = 0

  @inline private final def inBound(row: Int, idx: Int): Boolean = {
    val lb = if (idx < 0) 0 else offsets(idx)
    val ub = if ((idx + 1) < offsets.length) offsets(idx + 1) else (row + 1)
    row >= lb && row < ub
  }

  /** Returns the index info `offsets` and `columns` for row. */
  protected def indexOf(row: Int): Int = {
    val lastIdx = lastIndex
    if (inBound(row, lastIdx)) {
      lastIdx
    } else {
      var idx = java.util.Arrays.binarySearch(offsets, row)
      idx = if (idx < 0) -idx - 2 else idx
      lastIndex = idx
      idx
    }
  }

  def isDefinedAt(row: Int) = {
    val idx = indexOf(row)
    if (idx < 0) false else {
      val column = columns(idx)
      val offset = offsets(idx)
      column.isDefinedAt(row - offset)
    }
  }
}

class ShiftColumn[T <: Column](by: Int, c1: T) { this: T =>
  def isDefinedAt(row: Int) = c1.isDefinedAt(row - by)
}

class RemapColumn[T <: Column](delegate: T, f: Int => Int) {
  this: T =>
  def isDefinedAt(row: Int) = delegate.isDefinedAt(f(row))
}

class RemapFilterColumn[T <: Column](delegate: T, filter: Int => Boolean, offset: Int) {
  this: T =>
  def isDefinedAt(row: Int) = row >= 0 && filter(row) && delegate.isDefinedAt(row + offset)
}

class RemapIndicesColumn[T <: Column](delegate: T, indices: ArrayIntList) { this: T =>
  private val _size = indices.size
  def isDefinedAt(row: Int) = row >= 0 && row < _size && delegate.isDefinedAt(indices.get(row))
}

class SparsenColumn[T <: Column](delegate: T, idx: Array[Int], toSize: Int) { this: T =>
  @inline @tailrec private def fill(a: Array[Int], i: Int): Array[Int] = {
    if (i < toSize && i < idx.length) {
      if (a(idx(i)) == -1) {
        // We can only update indices that aren't already mapped
        a(idx(i)) = i
      }
      fill(a, i+1)
    } else a
  }

  val remap: Array[Int] = fill(Array.fill[Int](toSize)(-1), 0)

  def isDefinedAt(row: Int) = row >= 0 && row < toSize && remap(row) != -1 && delegate.isDefinedAt(remap(row))
}

class InfiniteColumn { this: Column =>
  def isDefinedAt(row: Int) = true
}

class RangeColumn(range: Range) { this: Column =>
  def isDefinedAt(row: Int) = range.contains(row)
}

class EmptyColumn[T <: Column] { this: T =>
  def isDefinedAt(row: Int) = false
  def apply(row: Int): Nothing = sys.error("Undefined.")
}

abstract class ArraySetColumn[T <: Column](val tpe: CType, protected val backing: Array[T]) { this: T =>
  protected def firstDefinedIndexAt(row: Int): Int = {
    var i = 0
    while (i < backing.length && ! backing(i).isDefinedAt(row)) { i += 1 }
    if (i != backing.length) i else -1
  }
  def isDefinedAt(row: Int) = firstDefinedIndexAt(row) != -1
  def jValue(row: Int) = backing(firstDefinedIndexAt(row)).jValue(row)
  def cValue(row: Int) = backing(firstDefinedIndexAt(row)).cValue(row)
  def strValue(row: Int) = backing(firstDefinedIndexAt(row)).strValue(row)
}

object ArraySetColumn {
  def apply[T <: Column](ctype: CType, columnSet: Array[T]): Column = {
    assert(columnSet.length != 0)
    ctype match {
      case CString      => new ArraySetColumn[StrColumn](ctype, columnSet.map(_.asInstanceOf[StrColumn])) with StrColumn { 
        def apply(row: Int): String = backing(firstDefinedIndexAt(row)).asInstanceOf[StrColumn].apply(row)
      }

      case CBoolean     => new ArraySetColumn[BoolColumn](ctype, columnSet.map(_.asInstanceOf[BoolColumn])) with BoolColumn { 
        def apply(row: Int): Boolean = backing(firstDefinedIndexAt(row)).asInstanceOf[BoolColumn].apply(row)
      }

      case CLong        => new ArraySetColumn[LongColumn](ctype, columnSet.map(_.asInstanceOf[LongColumn])) with LongColumn { 
        def apply(row: Int): Long = backing(firstDefinedIndexAt(row)).asInstanceOf[LongColumn].apply(row)
      }

      case CDouble      => new ArraySetColumn[DoubleColumn](ctype, columnSet.map(_.asInstanceOf[DoubleColumn])) with DoubleColumn { 
        def apply(row: Int): Double = backing(firstDefinedIndexAt(row)).asInstanceOf[DoubleColumn].apply(row)
      }

      case CNum         => new ArraySetColumn[NumColumn](ctype, columnSet.map(_.asInstanceOf[NumColumn])) with NumColumn {
        def apply(row: Int): BigDecimal = backing(firstDefinedIndexAt(row)).asInstanceOf[NumColumn].apply(row)
      }

      case CDate        => new ArraySetColumn[DateColumn](ctype, columnSet.map(_.asInstanceOf[DateColumn])) with DateColumn {
        def apply(row: Int): DateTime = backing(firstDefinedIndexAt(row)).asInstanceOf[DateColumn].apply(row)
      }

      case ctype: CArrayType[a] => new ArraySetColumn[HomogeneousArrayColumn[a]](ctype, columnSet.map(_.asInstanceOf[HomogeneousArrayColumn[a]])) with HomogeneousArrayColumn[a] {
        override val tpe = ctype
        def apply(row: Int): IndexedSeq[a] = backing(firstDefinedIndexAt(row)).asInstanceOf[HomogeneousArrayColumn[a]].apply(row)
      }

      case CNull        => new ArraySetColumn[NullColumn](ctype, columnSet.map(_.asInstanceOf[NullColumn])) with NullColumn {}

      case CEmptyObject => new ArraySetColumn[EmptyObjectColumn](ctype, columnSet.map(_.asInstanceOf[EmptyObjectColumn])) with EmptyObjectColumn {}

      case CEmptyArray  => new ArraySetColumn[EmptyArrayColumn](ctype, columnSet.map(_.asInstanceOf[EmptyArrayColumn])) with EmptyArrayColumn {}

      case CUndefined   => UndefinedColumn(columnSet(0))
    }
  }
}

/* help for ctags
type ColumnSupport */
