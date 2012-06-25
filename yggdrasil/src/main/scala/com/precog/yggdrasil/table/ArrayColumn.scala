package com.precog.yggdrasil
package table

import org.joda.time.DateTime

case class ArrayBoolColumn(values: Array[Boolean]) extends BoolColumn {
  def isDefinedAt(row: Int) = row >= 0 && row < values.length
  def apply(row: Int) = values(row)
}

case class ArrayLongColumn(values: Array[Long]) extends LongColumn {
  def isDefinedAt(row: Int) = row >= 0 && row < values.length
  def apply(row: Int) = values(row)
}

case class ArrayDoubleColumn(values: Array[Double]) extends DoubleColumn {
  def isDefinedAt(row: Int) = row >= 0 && row < values.length
  def apply(row: Int) = values(row)
}

case class ArrayNumColumn(values: Array[BigDecimal]) extends NumColumn {
  def isDefinedAt(row: Int) = row >= 0 && row < values.length
  def apply(row: Int) = values(row)
}

case class ArrayStrColumn(values: Array[String]) extends StrColumn {
  def isDefinedAt(row: Int) = row >= 0 && row < values.length
  def apply(row: Int) = values(row)
}

case class ArrayDateColumn(values: Array[DateTime]) extends DateColumn {
  def isDefinedAt(row: Int) = row >= 0 && row < values.length
  def apply(row: Int) = values(row)
}


/* help for ctags
type ArrayColumn */
