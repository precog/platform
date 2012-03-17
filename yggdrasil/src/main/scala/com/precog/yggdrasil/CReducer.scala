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

sealed trait CReducer[@specialized(Int, Boolean, Long, Float, Double) A]
trait StringCR[A] extends CReducer[A] {
  def applyString(a: A, s: String): A
}
trait BoolCR[A] extends CReducer[A] {
  def applyBool(a: A, b: Boolean): A
}
trait IntCR[A] extends CReducer[A] {
  def applyInt(a: A, i: Int): A
}
trait LongCR[A] extends CReducer[A] {
  def applyLong(a: A, l: Long): A
}
trait FloatCR[A] extends CReducer[A] {
  def applyFloat(a: A, f: Float): A
}
trait DoubleCR[A] extends CReducer[A] {
  def applyDouble(a: A, d: Double): A
}
trait NumCR[A] extends CReducer[A] {
  def applyNum(a: A, v: BigDecimal): A
}


// vim: set ts=4 sw=4 et:
