package com.precog.yggdrasil
package table

import com.precog.util.bitset.makeMutable
import org.joda.time.DateTime

import scala.collection._

trait DefinedAtIndex {
  val defined: BitSet
  def isDefinedAt(row: Int) = defined.contains(row)
}

trait ArrayColumn[@specialized(Boolean, Long, Double) A] extends DefinedAtIndex { 
  def update(row: Int, value: A): Unit
}

class ArrayBoolColumn(val defined: mutable.BitSet, values: mutable.BitSet) extends ArrayColumn[Boolean] with BoolColumn {
  def apply(row: Int) = values.contains(row)

  def update(row: Int, value: Boolean) = {
    defined += row
    if (value) values += row else values -= row
  }
}

object ArrayBoolColumn {
  def apply(defined: BitSet, values: BitSet) = new ArrayBoolColumn(makeMutable(defined), makeMutable(values))
  def apply(defined: BitSet, values: Array[Boolean]) = new ArrayBoolColumn(makeMutable(defined), mutable.BitSet((0 until values.length).filter(values): _*))
  def empty(): ArrayBoolColumn = new ArrayBoolColumn(mutable.BitSet.empty, mutable.BitSet.empty)
}


class ArrayLongColumn(val defined: mutable.BitSet, values: Array[Long]) extends ArrayColumn[Long] with LongColumn {
  def apply(row: Int) = values(row)

  def update(row: Int, value: Long) = {
    defined += row
    values(row) = value
  }
}

object ArrayLongColumn {
  def apply(defined: BitSet, values: Array[Long]) = new ArrayLongColumn(makeMutable(defined), values)
  def empty(size: Int): ArrayLongColumn = new ArrayLongColumn(mutable.BitSet.empty, new Array[Long](size))
}


class ArrayDoubleColumn(val defined: mutable.BitSet, values: Array[Double]) extends ArrayColumn[Double] with DoubleColumn {
  def apply(row: Int) = values(row)

  def update(row: Int, value: Double) = {
    defined += row
    values(row) = value
  }
}

object ArrayDoubleColumn {
  def apply(defined: BitSet, values: Array[Double]) = new ArrayDoubleColumn(makeMutable(defined), values)
  def empty(size: Int): ArrayDoubleColumn = new ArrayDoubleColumn(mutable.BitSet.empty, new Array[Double](size))
}


class ArrayNumColumn(val defined: mutable.BitSet, values: Array[BigDecimal]) extends ArrayColumn[BigDecimal] with NumColumn {
  def apply(row: Int) = values(row)

  def update(row: Int, value: BigDecimal) = {
    defined += row
    values(row) = value
  }
}

object ArrayNumColumn {
  def apply(defined: BitSet, values: Array[BigDecimal]) = new ArrayNumColumn(makeMutable(defined), values)
  def empty(size: Int): ArrayNumColumn = new ArrayNumColumn(mutable.BitSet.empty, new Array[BigDecimal](size))
}


class ArrayStrColumn(val defined: mutable.BitSet, values: Array[String]) extends ArrayColumn[String] with StrColumn {
  def apply(row: Int) = values(row)

  def update(row: Int, value: String) = {
    defined += row
    values(row) = value
  }
}

object ArrayStrColumn {
  def apply(defined: BitSet, values: Array[String]) = new ArrayStrColumn(makeMutable(defined), values)
  def empty(size: Int): ArrayStrColumn = new ArrayStrColumn(mutable.BitSet.empty, new Array[String](size))
}

class ArrayDateColumn(val defined: mutable.BitSet, values: Array[DateTime]) extends ArrayColumn[DateTime] with DateColumn {
  def apply(row: Int) = values(row)

  def update(row: Int, value: DateTime) = {
    defined += row
    values(row) = value
  }
}

object ArrayDateColumn {
  def apply(defined: BitSet, values: Array[DateTime]) = new ArrayDateColumn(makeMutable(defined), values)
  def empty(size: Int): ArrayDateColumn = new ArrayDateColumn(mutable.BitSet.empty, new Array[DateTime](size))
}

class MutableEmptyArrayColumn(val defined: mutable.BitSet) extends ArrayColumn[Boolean] with EmptyArrayColumn {
  def update(row: Int, value: Boolean) = {
    if (value) defined += row else defined -= row
  }
}

object MutableEmptyArrayColumn {
  def empty(): MutableEmptyArrayColumn = new MutableEmptyArrayColumn(mutable.BitSet.empty)
}

class MutableEmptyObjectColumn(val defined: mutable.BitSet) extends ArrayColumn[Boolean] with EmptyObjectColumn {
  def update(row: Int, value: Boolean) = {
    if (value) defined += row else defined -= row
  }
}

object MutableEmptyObjectColumn {
  def empty(): MutableEmptyObjectColumn = new MutableEmptyObjectColumn(mutable.BitSet.empty)
}

class MutableNullColumn(val defined: mutable.BitSet) extends ArrayColumn[Boolean] with NullColumn {
  def update(row: Int, value: Boolean) = {
    if (value) defined += row else defined -= row
  }
}

object MutableNullColumn {
  def empty(): MutableNullColumn = new MutableNullColumn(mutable.BitSet.empty)
}

/* help for ctags
type ArrayColumn */
