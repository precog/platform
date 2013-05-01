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

import com.precog.util._
import com.precog.common._

import scala.collection.mutable
import scala.annotation.tailrec

/**
 * Creates an efficient hash for a slice. From this, when given another slice, we can
 * map rows from that slice to rows in the hashed slice.
 */
final class HashedSlice private (slice0: Slice, rowMap: scala.collection.Map[Int, IntList]) {
  def mapRowsFrom(slice1: Slice): Int => (Int => Unit) => Unit = {
    val hasher = new SliceHasher(slice1)
    val rowComparator: RowComparator = Slice.rowComparatorFor(slice1, slice0) {
      _.columns.keys map (_.selector)
    }

    { (lrow: Int) =>
      { (f: Int => Unit) =>
        val matches = rowMap get hasher.hash(lrow) getOrElse IntNil
        matches foreach { rrow =>
          if (rowComparator.compare(lrow, rrow) == scalaz.Ordering.EQ) f(rrow)
        }
      }
    }
  }
}

object HashedSlice {
  def apply(slice: Slice): HashedSlice = {
    val hasher = new SliceHasher(slice)

    val rowMap: mutable.Map[Int, IntList] = mutable.Map.empty
    Loop.range(0, slice.size) { row =>
      val hash = hasher.hash(row)
      val rows = rowMap.getOrElse(hash, IntNil)
      rowMap.put(hash, row :: rows)
    }

    new HashedSlice(slice, rowMap)
  }
}

/**
 * Wraps a slice and provides a way to hash its rows efficiently. Given 2
 * equivalent rows in 2 different slices, this should hash both rows to the
 * same value, regardless of whether the slices look similar otherwise.
 */
private final class SliceHasher(slice: Slice) {
  private val hashers: Array[ColumnHasher] = slice.columns.toArray map { case (ref, col) =>
    ColumnHasher(ref, col)
  }

  @tailrec private final def hashOf(row: Int, i: Int = 0, hc: Int = 0): Int = {
    if (i >= hashers.length) hc else {
      hashOf(row, i + 1, hc ^ hashers(i).hash(row))
    }
  }

  def hash(row: Int): Int = hashOf(row)
}

/**
 * A simple way to hash rows in a column. This should guarantee that equivalent
 * columns have the same hash. For instance, equivalent Double and Longs should
 * hash to the same number.
 */
private sealed trait ColumnHasher {
  def columnRef: ColumnRef
  def column: Column

  final def hash(row: Int): Int = if (column isDefinedAt row) hashImpl(row) else 0

  protected def hashImpl(row: Int): Int
}

private final case class StrColumnHasher(columnRef: ColumnRef, column: StrColumn)
    extends ColumnHasher {
  private val pathHash = columnRef.selector.hashCode
  protected final def hashImpl(row: Int): Int = 3 * pathHash + 23 * column(row).hashCode
}

private final case class BoolColumnHasher(columnRef: ColumnRef, column: BoolColumn)
    extends ColumnHasher {
  private val pathHash = columnRef.selector.hashCode
  protected final def hashImpl(row: Int): Int = 5 * pathHash + 457 * (if (column(row)) 42 else 21)
}

private final case class DateColumnHasher(columnRef: ColumnRef, column: DateColumn)
    extends ColumnHasher {
  private val pathHash = columnRef.selector.hashCode
  protected final def hashImpl(row: Int): Int = 7 * pathHash + 17 * column(row).toString().hashCode
}

private final case class PeriodColumnHasher(columnRef: ColumnRef, column: PeriodColumn)
    extends ColumnHasher {
  private val pathHash = columnRef.selector.hashCode
  protected final def hashImpl(row: Int): Int = 11 * pathHash + 503 * column(row).hashCode
}

private object NumericHash {
  def apply(n: Long): Int = n.toInt ^ (n >>> 32).toInt

  def apply(n: Double): Int = {
    val rounded = math.round(n)
    if (rounded == n) {
      apply(rounded)
    } else {
      val bits = java.lang.Double.doubleToLongBits(n)
      17 * bits.toInt + 23 * (bits >>> 32).toInt
    }
  }

  def apply(n: BigDecimal): Int = {
    val approx = n.toDouble
    if (n == approx) {
      apply(approx)
    } else {
      n.bigDecimal.hashCode
    }
  }
}

private final case class LongColumnHasher(columnRef: ColumnRef, column: LongColumn)
    extends ColumnHasher {
  private val pathHash = columnRef.selector.hashCode
  protected final def hashImpl(row: Int): Int = 13 * pathHash + 23 * NumericHash(column(row))
}

private final case class DoubleColumnHasher(columnRef: ColumnRef, column: DoubleColumn)
    extends ColumnHasher {
  private val pathHash = columnRef.selector.hashCode
  protected final def hashImpl(row: Int): Int = 13 * pathHash + 23 * NumericHash(column(row))
}

private final case class NumColumnHasher(columnRef: ColumnRef, column: NumColumn)
    extends ColumnHasher {
  private val pathHash = columnRef.selector.hashCode
  protected final def hashImpl(row: Int): Int = 13 * pathHash + 23 * NumericHash(column(row))
}

private final case class CValueColumnHasher(columnRef: ColumnRef, column: Column)
    extends ColumnHasher {
  private val pathHash = columnRef.selector.hashCode
  protected final def hashImpl(row: Int): Int = 17 * pathHash + 23 * column.cValue(row).hashCode
}

private object ColumnHasher {
  def apply(ref: ColumnRef, col0: Column): ColumnHasher = col0 match {
    case (col: StrColumn) => new StrColumnHasher(ref, col)
    case (col: BoolColumn) => new BoolColumnHasher(ref, col)
    case (col: LongColumn) => new LongColumnHasher(ref, col)
    case (col: DoubleColumn) => new DoubleColumnHasher(ref, col)
    case (col: NumColumn) => new NumColumnHasher(ref, col)
    case (col: DateColumn) => new DateColumnHasher(ref, col)
    case (col: PeriodColumn) => new PeriodColumnHasher(ref, col)
    case _ => new CValueColumnHasher(ref, col0)
  }
}
