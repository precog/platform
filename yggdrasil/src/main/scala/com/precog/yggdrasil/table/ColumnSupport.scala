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
import scala.collection.BitSet
import scala.annotation.tailrec

class BitsetColumn(definedAt: BitSet) { this: Column =>
  def isDefinedAt(row: Int): Boolean = definedAt(row)

  override def toString = {
    val limit = definedAt.reduce(_ max _)
    val repr = (row: Int) => if (definedAt(row)) 'x' else '_'
    getClass.getName + "(" + (0 until limit).map(repr).mkString("[", ",", "]") + ", " + limit + ")"
  }
}

object BitsetColumn {
  def bitset(definedAt: Seq[Boolean]) = {
    BitSet(definedAt.zipWithIndex collect { case (v, i) if v => i }: _*)
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

class ConcatColumn[T <: Column](at: Int, c1: T, c2: T) { this: T =>
  def isDefinedAt(row: Int) = row >= 0 && ((row < at && c1.isDefinedAt(row)) || (row >= at && c2.isDefinedAt(row - at)))
}

class ShiftColumn[T <: Column](by: Int, c1: T) { this: T =>
  def isDefinedAt(row: Int) = c1.isDefinedAt(row + by)
}

class RemapColumn[T <: Column](delegate: T, f: PartialFunction[Int, Int]) { this: T =>
  def isDefinedAt(row: Int) = f.isDefinedAt(row) && delegate.isDefinedAt(f(row))
}

class SparsenColumn[T <: Column](delegate: T, idx: Array[Int], toSize: Int) { this: T =>
  //println("Sparsen to size " + toSize)
  @inline @tailrec private def fill(a: Array[Int], i: Int): Array[Int] = {
    if (i < toSize && i < idx.length) {
      //println("Assign %d to a(%d)".format(i, idx(i)))
      if (a(idx(i)) == -1) {
        // We can only update indices that aren't already mapped
        a(idx(i)) = i
      }
      fill(a, i+1)
    } else a
  }

  val remap: Array[Int] = fill(Array.fill[Int](toSize)(-1), 0)

  //println("Remapped indices %s to %s".format(idx.mkString("[",", ","]"), remap.mkString("[",", ","]")))

  def isDefinedAt(row: Int) = row < toSize && remap(row) != -1 && delegate.isDefinedAt(remap(row))
}

class InfiniteColumn { this: Column =>
  def isDefinedAt(row: Int) = true
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

      case CNull        => new ArraySetColumn[NullColumn](ctype, columnSet.map(_.asInstanceOf[NullColumn])) with NullColumn {}

      case CEmptyObject => new ArraySetColumn[EmptyObjectColumn](ctype, columnSet.map(_.asInstanceOf[EmptyObjectColumn])) with EmptyObjectColumn {}

      case CEmptyArray  => new ArraySetColumn[EmptyArrayColumn](ctype, columnSet.map(_.asInstanceOf[EmptyArrayColumn])) with EmptyArrayColumn {}

      case CUndefined   => UndefinedColumn(columnSet(0))
    }
  }
}

/* help for ctags
type ColumnSupport */
