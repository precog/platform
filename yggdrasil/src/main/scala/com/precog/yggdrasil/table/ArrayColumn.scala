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
