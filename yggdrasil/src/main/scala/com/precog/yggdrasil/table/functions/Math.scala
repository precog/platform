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
package functions

object FAddInt extends F2[Int, Int, Int] {
  val accepts = (CInt, CInt)
  val returns = CInt
  def apply(a: Column[Int], b: Column[Int]): Column[Int] = new Column[Int] {
    val returns = CInt
    def isDefinedAt(row: Int) = a.isDefinedAt(row) || b.isDefinedAt(row)
    def apply(row: Int) = if (a.isDefinedAt(row)) {
      if (b.isDefinedAt(row)) {
        a(row) + b(row)
      } else {
        a(row)
      }
    } else {
      if (b.isDefinedAt(row)) {
        b(row)
      } else {
        throw new IllegalStateException("Attempt to retrieve undefined value for row: " + row)
      }
    }
  }
}

object FAddLong extends F2[Long, Long, Long] {
  val accepts = (CLong, CLong)
  val returns = CLong
  def apply(a: Column[Long], b: Column[Long]): Column[Long] = new Column[Long] {
    val returns = CLong
    def isDefinedAt(row: Int) = a.isDefinedAt(row) || b.isDefinedAt(row)
    def apply(row: Int) = if (a.isDefinedAt(row)) {
      if (b.isDefinedAt(row)) {
        a(row) + b(row)
      } else {
        a(row)
      }
    } else {
      if (b.isDefinedAt(row)) {
        b(row)
      } else {
        throw new IllegalStateException("Attempt to retrieve undefined value for row: " + row)
      }
    }
  }
}

object FAddFloat extends F2[Float, Float, Float] {
  val accepts = (CFloat, CFloat)
  val returns = CFloat
  def apply(a: Column[Float], b: Column[Float]): Column[Float] = new Column[Float] {
    val returns = CFloat
    def isDefinedAt(row: Int) = a.isDefinedAt(row) || b.isDefinedAt(row)
    def apply(row: Int) = if (a.isDefinedAt(row)) {
      if (b.isDefinedAt(row)) {
        a(row) + b(row)
      } else {
        a(row)
      }
    } else {
      if (b.isDefinedAt(row)) {
        b(row)
      } else {
        throw new IllegalStateException("Attempt to retrieve undefined value for row: " + row)
      }
    }
  }
}

object FAddDouble extends F2[Double, Double, Double] {
  val accepts = (CDouble, CDouble)
  val returns = CDouble
  def apply(a: Column[Double], b: Column[Double]): Column[Double] = new Column[Double] {
    val returns = CDouble
    def isDefinedAt(row: Int) = a.isDefinedAt(row) || b.isDefinedAt(row)
    def apply(row: Int) = if (a.isDefinedAt(row)) {
      if (b.isDefinedAt(row)) {
        a(row) + b(row)
      } else {
        a(row)
      }
    } else {
      if (b.isDefinedAt(row)) {
        b(row)
      } else {
        throw new IllegalStateException("Attempt to retrieve undefined value for row: " + row)
      }
    }
  }
}

object FAddDecimal extends F2[BigDecimal, BigDecimal, BigDecimal] {
  val accepts = (CDecimalArbitrary, CDecimalArbitrary)
  val returns = CDecimalArbitrary
  def apply(a: Column[BigDecimal], b: Column[BigDecimal]): Column[BigDecimal] = new Column[BigDecimal] {
    val returns = CDecimalArbitrary
    def isDefinedAt(row: Int) = a.isDefinedAt(row) || b.isDefinedAt(row)
    def apply(row: Int) = if (a.isDefinedAt(row)) {
      if (b.isDefinedAt(row)) {
        a(row) + b(row)
      } else {
        a(row)
      }
    } else {
      if (b.isDefinedAt(row)) {
        b(row)
      } else {
        throw new IllegalStateException("Attempt to retrieve undefined value for row: " + row)
      }
    }
  }
}
// vim: set ts=4 sw=4 et:
