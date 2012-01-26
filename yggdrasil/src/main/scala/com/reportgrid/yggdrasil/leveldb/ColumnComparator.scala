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
package com.reportgrid.yggdrasil
package leveldb

import com.reportgrid.common._ 
import com.reportgrid.util._
import Bijection._

import org.iq80.leveldb._
import scala.math.BigDecimal
import java.util.Comparator

trait ColumnComparator extends Comparator[Array[Byte]] {
  def width: Array[Byte] => Int
}

object ColumnComparator {
  def apply(t: ColumnType): ColumnComparator = t match {
    case SLong    => new LongComparator
    case SDouble  => new DoubleComparator
    case SBoolean => new BooleanComparator
    case SNull    => AlwaysEqualComparator

    case SStringFixed(width)  => new StringComparator(_ => width)
    case SStringArbitrary     => new StringComparator(_.take(4).as[Int])

    case SDecimalArbitrary => new BigDecimalComparator(_.take(4).as[Int])
  }
}

object AlwaysEqualComparator extends ColumnComparator {
  def width: Array[Byte] => Int = a => 0
  def compare(a : Array[Byte], b : Array[Byte]) = 0
}

trait ProjectionComparator extends Comparator[Array[Byte]] with DBComparator { 
  /** ID-based comparison is common to all other comparators, and must have lowest
   *  priority */
  abstract override def compare(a : Array[Byte], b : Array[Byte]) = {
    val valCompare = super.compare(a, b)
    if (valCompare != 0) valCompare
    else a.drop(a.length - 8).as[Long].compareTo(b.drop(b.length - 8).as[Long])
  }

  // Don't override unless you really know what you're doing
  def findShortestSeparator(start : Array[Byte], limit : Array[Byte]) = start
  def findShortSuccessor(key : Array[Byte]) = key
}

class LongComparator extends ColumnComparator {
  val width = (_: Array[Byte]) => 8
  def compare(a : Array[Byte], b : Array[Byte]) = {
    a.take(8).as[Long].compareTo(b.take(8).as[Long])
  }
}

class LongProjectionComparator extends LongComparator with ProjectionComparator {
  val name = "LongComparatorV1"
}

class DoubleComparator extends ColumnComparator {
  val width = (_: Array[Byte]) => 8
  def compare(a : Array[Byte], b : Array[Byte]) = {
    a.take(8).as[Double].compareTo(b.take(8).as[Double])
  }
}

class DoubleProjectionComparator extends DoubleComparator with ProjectionComparator {
  val name = "DoubleComparatorV1"
}

class BigDecimalComparator(val width: Array[Byte] => Int) extends ColumnComparator {
  def compare(a : Array[Byte], b : Array[Byte]) = {
    a.take(width(a)).as[BigDecimal].bigDecimal.compareTo(b.take(width(b)).as[BigDecimal].bigDecimal)
  }
}

class BigDecimalProjectionComparator
extends BigDecimalComparator(a => a.length - 8) with ProjectionComparator {
  val name = "BigDecimalComparatorV1"
}

class BooleanComparator extends ColumnComparator {
  val width = (_: Array[Byte]) => 1
  def compare(a: Array[Byte], b: Array[Byte]) = {
    a(0).compareTo(b(0))
  }
}

class BooleanProjectionComparator
extends BooleanComparator with ProjectionComparator {
  val name = "BooleanComparatorV1"
}

class StringComparator(val width: Array[Byte] => Int) extends ColumnComparator {
  def compare(a : Array[Byte], b : Array[Byte]) = {
    a.take(width(a)).as[String].compareTo(b.take(width(b)).as[String])
  }
}

class StringProjectionComparator
extends StringComparator(a => a.length - 8) with ProjectionComparator {
  val name = "StringComparatorV1"
}

protected class BaseMultiColumnComparator(columnComparators: ColumnComparator*) extends Comparator[Array[Byte]] {
  //TODO: Optimize.
  def compare(a : Array[Byte], b : Array[Byte]) = {
    val (result, _, _) = columnComparators.foldLeft((0, a, b)) { 
      case (acc @ (result, a, b), comparator) => 
        if (result == 0) (comparator.compare(a, b), a.drop(comparator.width(a)), b.drop(comparator.width(b)))
        else acc
    }

    result
  }
}

class MultiColumnComparator(columnComparators: ColumnComparator*) extends BaseMultiColumnComparator(columnComparators: _*) with ProjectionComparator {
  val name = "MultiColumnComparatorV1"
}

object ProjectionComparator {
  val Long = new LongProjectionComparator
  val Double = new DoubleProjectionComparator
  val BigDecimal = new BigDecimalProjectionComparator
  val Boolean = new BooleanProjectionComparator
  val String = new StringProjectionComparator

  def forProjection(p: ProjectionDescriptor): DBComparator = p.columns.map(_.valueType).toList match {
    case SLong :: Nil           => ProjectionComparator.Long
    case SDouble :: Nil         => ProjectionComparator.Double
    case SBoolean :: Nil        => ProjectionComparator.Boolean
    case SDecimalArbitrary :: Nil  => ProjectionComparator.BigDecimal
    case SStringArbitrary :: Nil      => ProjectionComparator.String
    case types => new MultiColumnComparator(types.map(ColumnComparator(_)): _*)
  }
}

// vim: set ts=4 sw=4 et:
