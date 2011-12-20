package com.reportgrid.storage
package leveldb

import org.iq80.leveldb._
import java.math.BigDecimal
import java.util.Comparator
import Bijection._

trait ColumnComparator extends Comparator[Array[Byte]] {
  def width: Array[Byte] => Int
}

object ColumnComparator {
  def apply(t: ColumnType): ColumnComparator = t match {
    case ColumnType.Long    => new LongComparator
    case ColumnType.Double  => new DoubleComparator
    case ColumnType.Boolean => new BooleanComparator
    case ColumnType.Null    => AlwaysEqualComparator
    case ColumnType.Nothing => AlwaysEqualComparator

    case ColumnType.String(width) => 
      width.map(w => new StringComparator(_ => w))
      .getOrElse(new StringComparator(_.take(4).as[Int]))

    case ColumnType.BigDecimal(width) => 
      width.map(w => sys.error("Fixed-length BigDecimal format not yet supported."))
      .getOrElse(new BigDecimalComparator(_.take(4).as[Int]))
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
    a.take(width(a)).as[BigDecimal].compareTo(b.take(width(b)).as[BigDecimal])
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

  def forProjection(p: ProjectionDescriptor): DBComparator = p.columns.map(_.columnType) match {
    case ColumnType.Long :: Nil           => ProjectionComparator.Long
    case ColumnType.Double :: Nil         => ProjectionComparator.Double
    case ColumnType.Boolean :: Nil        => ProjectionComparator.Boolean
    case ColumnType.BigDecimal(_) :: Nil  => ProjectionComparator.BigDecimal
    case ColumnType.String(_) :: Nil      => ProjectionComparator.String
    case types => new MultiColumnComparator(types.map(ColumnComparator(_)): _*)
  }
}


// vim: set ts=4 sw=4 et:
