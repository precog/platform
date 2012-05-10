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

object CoerceIntLong extends TotalF1P[Int, Long] {
  val accepts = CInt
  val returns = CLong
  def apply(i: Int): Long = i
}

object CoerceIntFloat extends TotalF1P[Int, Float] {
  val accepts = CInt
  val returns = CFloat
  def apply(i: Int): Float = i
}

object CoerceIntDouble extends TotalF1P[Int, Double] {
  val accepts = CInt
  val returns = CDouble
  def apply(i: Int): Double = i
}

object CoerceIntDecimal extends TotalF1P[Int, BigDecimal] {
  val accepts = CInt
  val returns = CDecimalArbitrary
  def apply(i: Int) = BigDecimal(i)
}

object CoerceLongFloat extends TotalF1P[Long, Float] {
  val accepts = CLong
  val returns = CFloat
  def apply(i: Long): Float = i
}

object CoerceLongDouble extends TotalF1P[Long, Double] {
  val accepts = CLong
  val returns = CDouble
  def apply(i: Long): Double = i
}

object CoerceLongDecimal extends TotalF1P[Long, BigDecimal] {
  val accepts = CLong
  val returns = CDecimalArbitrary
  def apply(i: Long) = BigDecimal(i)
}

object CoerceFloatDouble extends TotalF1P[Float, Double] {
  val accepts = CFloat
  val returns = CDouble
  def apply(i: Float): Double = i
}

object CoerceFloatDecimal extends TotalF1P[Float, BigDecimal] {
  val accepts = CFloat
  val returns = CDecimalArbitrary
  def apply(i: Float) = BigDecimal(i)
}

object CoerceDoubleDecimal extends TotalF1P[Double, BigDecimal] {
  val accepts = CDouble
  val returns = CDecimalArbitrary
  def apply(i: Double) = BigDecimal(i)
}
// vim: set ts=4 sw=4 et:
