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

trait Column[@specialized(Boolean, Int, Long, Float, Double) A] extends Returning[A] with (Int => A) { outer =>
  def isDefinedAt(row: Int): Boolean
  def apply(row: Int): A

  def remap(f: PartialFunction[Int, Int]): Column[A] = new Remap(f) with MemoizingColumn[A]

  def |> [@specialized(Boolean, Int, Long, Float, Double) B](f: F1[A, B]): Column[B] = new Thrush(f) with MemoizingColumn[B]

  private class Remap(f: PartialFunction[Int, Int]) extends Column[A] {
    val returns = outer.returns
    def isDefinedAt(row: Int): Boolean = f.isDefinedAt(row) && outer.isDefinedAt(f(row))
    def apply(row: Int) = outer(f(row))
  }

  private class Thrush[B](f: F1[A, B]) extends Column[B] {
    val returns = f.returns
    def isDefinedAt(row: Int) = f(outer).isDefinedAt(row)
    def apply(row: Int): B = f(outer)(row)
  }
}

trait MemoizingColumn[@specialized(Boolean, Int, Long, Float, Double) A] extends Column[A] {
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

object Column {
  def forArray[@specialized(Boolean, Int, Long, Float, Double) A](ctype: CType, a: Array[A]): Column[A] = new Column[A] {
    val returns = ctype.asInstanceOf[CType { type CA = A }]
    def isDefinedAt(row: Int) = row >= 0 && row < a.length
    def apply(row: Int) = a(row)
  }

  def const[@specialized(Boolean, Int, Long, Float, Double) A](ctype: CType { type CA = A }, a: A): Column[A] = new Column[A] {
    val returns = ctype
    def isDefinedAt(row: Int) = true
    def apply(row: Int) = a
  }
}

// vim: set ts=4 sw=4 et:
