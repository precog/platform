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

import com.precog.util.bitset.makeMutable
import org.joda.time.DateTime

import scala.collection._

trait DefinedAtIndex {
  val defined: BitSet
  def isDefinedAt(row: Int) = defined.contains(row)
}

trait ArrayColumn[@specialized(Boolean, Long, Double) A] extends DefinedAtIndex with ExtensibleColumn { 
  def update(row: Int, value: A): Unit
}

class ArrayHomogeneousArrayColumn[A](val defined: mutable.BitSet, values: Array[IndexedSeq[A]])(val tpe: CArrayType[A]) extends HomogeneousArrayColumn[A] with ArrayColumn[IndexedSeq[A]] {
  def apply(row: Int) = values(row)

  def update(row: Int, value: IndexedSeq[A]) {
    defined += row
    values(row) = value
  }
}

object ArrayHomogeneousArrayColumn {
  def apply[A: CValueType](values: Array[IndexedSeq[A]]) =
    new ArrayHomogeneousArrayColumn(mutable.BitSet(0 until values.length: _*), values)(CArrayType(CValueType[A]))
  def apply[A: CValueType](defined: BitSet, values: Array[IndexedSeq[A]]) =
    new ArrayHomogeneousArrayColumn(makeMutable(defined), values)(CArrayType(CValueType[A]))
  def empty[A: CValueType](size: Int): ArrayHomogeneousArrayColumn[A] =
    new ArrayHomogeneousArrayColumn(mutable.BitSet.empty, new Array[IndexedSeq[A]](size))(CArrayType(CValueType[A]))
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
  def apply(values: Array[Boolean]) = {
    val definedAt = mutable.BitSet(0 until values.length: _*)
    new ArrayBoolColumn(definedAt, definedAt.filter(values))
  }

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
  def apply(values: Array[Long]) = new ArrayLongColumn(mutable.BitSet(0 until values.length: _*), values)
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
  def apply(values: Array[Double]) = new ArrayDoubleColumn(mutable.BitSet(0 until values.length: _*), values)
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
  def apply(values: Array[BigDecimal]) = new ArrayNumColumn(mutable.BitSet(0 until values.length: _*), values)
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
  def apply(values: Array[String]) = new ArrayStrColumn(mutable.BitSet(0 until values.length: _*), values)
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
  def apply(values: Array[DateTime]) = new ArrayDateColumn(mutable.BitSet(0 until values.length: _*), values)
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
