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
package com.precog.yggdrasil.table

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
  @inline @tailrec private def fill(a: Array[Int], i: Int): Array[Int] = {
    if (i < idx.length) {
      a(idx(i)) = i
      fill(a, i+1)
    } else a
  }

  val remap: Array[Int] = fill(new Array[Int](toSize), 0)

  def isDefinedAt(row: Int) = row < toSize && delegate.isDefinedAt(remap(row))
}

class InfiniteColumn { this: Column =>
  def isDefinedAt(row: Int) = true
}

class EmptyColumn[T <: Column] { this: T =>
  def isDefinedAt(row: Int) = false
  def apply(row: Int): Nothing = sys.error("Undefined.")
}


/* help for ctags
type ColumnSupport */
