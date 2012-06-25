package com.precog.yggdrasil
package table

import org.joda.time.DateTime

class MemoBoolColumn(c: BoolColumn) extends BoolColumn {
  private var row0 = -1
  private var memo: Boolean = _
  def isDefinedAt(row: Int) = c.isDefinedAt(row)
  def apply(row: Int) = {
    if (row != row0) { row0 = row; memo = c(row) }
    memo
  }
}

class MemoLongColumn(c: LongColumn) extends LongColumn {
  private var row0 = -1 
  private var memo: Long = _
  def isDefinedAt(row: Int) = c.isDefinedAt(row)
  def apply(row: Int) = {
    if (row != row0) { row0 = row; memo = c(row) }
    memo
  }
}

class MemoDoubleColumn(c: DoubleColumn) extends DoubleColumn {
  private var row0 = -1 
  private var memo: Double = _
  def isDefinedAt(row: Int) = c.isDefinedAt(row)
  def apply(row: Int) = {
    if (row != row0) { row0 = row; memo = c(row) }
    memo
  }
}

class MemoNumColumn(c: NumColumn) extends NumColumn {
  private var row0 = -1 
  private var memo: BigDecimal = _
  def isDefinedAt(row: Int) = c.isDefinedAt(row)
  def apply(row: Int) = {
    if (row != row0) { row0 = row; memo = c(row) }
    memo
  }
}

class MemoStrColumn(c: StrColumn) extends StrColumn {
  private var row0 = -1 
  private var memo: String = _
  def isDefinedAt(row: Int) = c.isDefinedAt(row)
  def apply(row: Int) = {
    if (row != row0) { row0 = row; memo = c(row) }
    memo
  }
}

class MemoDateColumn(c: DateColumn) extends DateColumn {
  private var row0 = -1 
  private var memo: DateTime = _
  def isDefinedAt(row: Int) = c.isDefinedAt(row)
  def apply(row: Int) = {
    if (row != row0) { row0 = row; memo = c(row) }
    memo
  }
}

/* help for ctags
type MemoColumn 
*/

