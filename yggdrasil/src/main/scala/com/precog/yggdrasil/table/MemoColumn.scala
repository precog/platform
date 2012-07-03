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

class MemoBoolColumn(c: BoolColumn) extends BoolColumn {
  private[this] var row0 = -1
  private[this] var memo: Boolean = _
  def isDefinedAt(row: Int) = c.isDefinedAt(row)
  def apply(row: Int) = {
    if (row != row0) { row0 = row; memo = c(row) }
    memo
  }
}

class MemoLongColumn(c: LongColumn) extends LongColumn {
  private[this] var row0 = -1
  private[this] var memo: Long = _
  def isDefinedAt(row: Int) = c.isDefinedAt(row)
  def apply(row: Int) = {
    if (row != row0) { row0 = row; memo = c(row) }
    memo
  }
}

class MemoDoubleColumn(c: DoubleColumn) extends DoubleColumn {
  private[this] var row0 = -1
  private[this] var memo: Double = _
  def isDefinedAt(row: Int) = c.isDefinedAt(row)
  def apply(row: Int) = {
    if (row != row0) { row0 = row; memo = c(row) }
    memo
  }
}

class MemoNumColumn(c: NumColumn) extends NumColumn {
  private[this] var row0 = -1
  private[this] var memo: BigDecimal = _
  def isDefinedAt(row: Int) = c.isDefinedAt(row)
  def apply(row: Int) = {
    if (row != row0) { row0 = row; memo = c(row) }
    memo
  }
}

class MemoStrColumn(c: StrColumn) extends StrColumn {
  private[this] var row0 = -1
  private[this] var memo: String = _
  def isDefinedAt(row: Int) = c.isDefinedAt(row)
  def apply(row: Int) = {
    if (row != row0) { row0 = row; memo = c(row) }
    memo
  }
}

class MemoDateColumn(c: DateColumn) extends DateColumn {
  private[this] var row0 = -1
  private[this] var memo: DateTime = _
  def isDefinedAt(row: Int) = c.isDefinedAt(row)
  def apply(row: Int) = {
    if (row != row0) { row0 = row; memo = c(row) }
    memo
  }
}
