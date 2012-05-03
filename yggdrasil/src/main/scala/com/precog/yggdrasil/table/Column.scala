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

import scala.collection.mutable.BitSet

trait Column[@specialized(Boolean, Long, Double) A] extends FN[A] { outer =>
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

class ArrayColumn[@specialized(Boolean, Long, Double) A](ctype: CType { type CA = A }, data: Array[A], limit: Int, definedAt: BitSet) extends Column[A] {
  val returns = ctype//.asInstanceOf[CType { type CA = A }]
  @inline final def isDefinedAt(row: Int) = row >= 0 && row < limit && row < data.length && definedAt(row)
  @inline final def apply(row: Int) = data(row)

  @inline final def update(row: Int, a: A) = {
    data(row) = a
    definedAt(row) = true
  }

  @inline final def prefix(limit: Int): ArrayColumn[A] = {
    assert(limit <= this.limit && limit >= 0)
    new ArrayColumn(ctype, data, limit, definedAt)
  }

  override def toString = {
    val repr = (row: Int) => if (isDefinedAt(row)) apply(row).toString else '_'
    "ArrayColumn(" + ctype + ", " + (0 until limit).map(repr).mkString("[", ",", "]") + ", " + limit + ")"
  }
}

object ArrayColumn {
  def apply[@specialized(Boolean, Long, Double) A: ClassManifest](ctype: CType { type CA = A }, size: Int): ArrayColumn[A] = {
    val definedAt = new BitSet(size)
    for (i <- 0 until size) definedAt(i) = false
    new ArrayColumn(ctype, new Array[A](size), size, definedAt)
  }
}

sealed abstract class NullColumn(definedAt: BitSet) extends Column[Null] {
  @inline final def isDefinedAt(row: Int) = definedAt(row)
  @inline final def apply(row: Int) = null

  object defined {
    def update(row: Int, value: Boolean) = definedAt(row) = value
  }

  override def toString = {
    val limit = definedAt.reduce(_ max _)
    val repr = (row: Int) => if (definedAt(row)) 'x' else '_'
    "NullColumn(" + returns + ", " + (0 until limit).map(repr).mkString("[", ",", "]") + ", " + limit + ")"
  }
}

class CNullColumn(size: Int) extends NullColumn(new BitSet(size)) {
  val returns = CNull
}

class CEmptyObjectColumn(size: Int) extends NullColumn(new BitSet(size)) {
  val returns = CEmptyObject
}

class CEmptyArrayColumn(size: Int) extends NullColumn(new BitSet(size)) {
  val returns = CEmptyArray
}

object Column {
  @inline def forArray[@specialized(Boolean, Long, Double) A](ctype: CType { type CA = A }, a: Array[A]): Column[A] = forArray(ctype, a, a.length)

  @inline def forArray[@specialized(Boolean, Long, Double) A](ctype: CType { type CA = A }, a: Array[A], limit: Int): Column[A] = {
    val definedAt = new BitSet(limit)
    for (i <- 0 until limit) definedAt(i) = true
    new ArrayColumn[A](ctype, a, limit, definedAt)
  }

  @inline def const[@specialized(Boolean, Long, Double) A](ctype: CType { type CA = A }, a: A): Column[A] = new Column[A] {
    val returns = ctype
    def isDefinedAt(row: Int) = true
    def apply(row: Int) = a
  }
}

// vim: set ts=4 sw=4 et:
