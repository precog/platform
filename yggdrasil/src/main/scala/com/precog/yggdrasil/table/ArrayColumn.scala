package com.precog.yggdrasil
package table

import org.joda.time.DateTime

import scala.collection.BitSet

trait DefinedAtIndex {
  val defined: BitSet
  def isDefinedAt(row: Int) = defined.contains(row)
}


case class ArrayBoolColumn(defined: BitSet, values: Array[Boolean]) extends BoolColumn with DefinedAtIndex {
  def apply(row: Int) = values(row)
}

case class ArrayLongColumn(defined: BitSet, values: Array[Long]) extends LongColumn with DefinedAtIndex {
  def apply(row: Int) = values(row)
}

case class ArrayDoubleColumn(defined: BitSet, values: Array[Double]) extends DoubleColumn with DefinedAtIndex {
  def apply(row: Int) = values(row)
}

case class ArrayNumColumn(defined: BitSet, values: Array[BigDecimal]) extends NumColumn with DefinedAtIndex {
  def apply(row: Int) = values(row)
}

case class ArrayStrColumn(defined: BitSet, values: Array[String]) extends StrColumn with DefinedAtIndex {
  def apply(row: Int) = values(row)
}

case class ArrayDateColumn(defined: BitSet, values: Array[DateTime]) extends DateColumn with DefinedAtIndex {
  def apply(row: Int) = values(row)
}


/* help for ctags
type ArrayColumn */
