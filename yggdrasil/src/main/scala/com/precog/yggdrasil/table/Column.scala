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

trait Column[@specialized(Boolean, Long, Double) A] extends FN[A] with (Int => A) { outer =>
  def isDefinedAt(row: Int): Boolean
  def apply(row: Int): A

  def remap(f: F1P[Int, Int]): Column[A] = new RemapColumn(f) with MemoizingColumn[A]

  def map[@specialized(Boolean, Long, Double) B](f: F1[A, B]): Column[B] = new MapColumn(f) with MemoizingColumn[B]

  private class RemapColumn(f: F1P[Int, Int]) extends Column[A] {
    val returns = outer.returns
    def isDefinedAt(row: Int): Boolean = f.isDefinedAt(row) && outer.isDefinedAt(f(row))
    def apply(row: Int) = outer(f(row))
  }

  private class MapColumn[B](f: F1[A, B]) extends Column[B] {
    val returns = f.returns
    def isDefinedAt(row: Int) = f(outer).isDefinedAt(row)
    def apply(row: Int): B = f(outer)(row)
  }
}

trait MemoizingColumn[@specialized(Boolean, Long, Double) A] extends Column[A] {
  private var _row = -1
  private var _value: A = _
  abstract override def apply(row: Int) = {
    if (_row != row) {
      _row = row
      _value = super.apply(row)
    }

    _value
  }
}

case class ArrayColumn[@specialized(Boolean, Long, Double) A](ctype: CType { type CA = A }, data: Array[A], limit: Int) extends Column[A] {
  val returns = ctype.asInstanceOf[CType { type CA = A }]
  @inline final def isDefinedAt(row: Int) = row >= 0 && row < limit && row < data.length
  @inline final def apply(row: Int) = data(row)
  @inline final def update(row: Int, a: A) = data(row) = a
  @inline final def resize(limit: Int): ArrayColumn[A] = {
    assert(limit <= this.limit)
    ArrayColumn(ctype, data, limit)
  }
}

object ArrayColumn {
  def apply[@specialized(Boolean, Long, Double) A: ClassManifest](ctype: CType { type CA = A }, size: Int): ArrayColumn[A] = ArrayColumn(ctype, new Array[A](size), size)
}

object Column {
  @inline def forArray[@specialized(Boolean, Long, Double) A](ctype: CType { type CA = A }, a: Array[A]): Column[A] = ArrayColumn[A](ctype, a, a.length)

  @inline def forArray[@specialized(Boolean, Long, Double) A](ctype: CType { type CA = A }, a: Array[A], limit: Int): Column[A] = ArrayColumn[A](ctype, a, limit)

  @inline def const[@specialized(Boolean, Long, Double) A](ctype: CType { type CA = A }, a: A): Column[A] = new Column[A] {
    val returns = ctype
    def isDefinedAt(row: Int) = true
    def apply(row: Int) = a
  }
}

// vim: set ts=4 sw=4 et:
