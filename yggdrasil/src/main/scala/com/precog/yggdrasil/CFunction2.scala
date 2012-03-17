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

sealed trait CFunction2 

trait StringCF2 extends CFunction2 {
  def applyString(s1: String, s2: String): String
}
trait BoolCF2 extends CFunction2 {
  def applyBool(b1: Boolean, b2: Boolean): Boolean
}
trait IntCF2 extends CFunction2 {
  def applyInt(i1: Int, i2: Int): Int
}
trait LongCF2 extends CFunction2 {
  def applyLong(l1: Long, l2: Long): Long
}
trait FloatCF2 extends CFunction2 {
  def applyFloat(f1: Float, f2: Float): Float
}
trait DoubleCF2 extends CFunction2 {
  def applyDouble(d1: Double, d2: Double): Double
}
trait NumCF2 extends CFunction2 {
  def applyNum(v1: BigDecimal, v2: BigDecimal): BigDecimal
}

//non-endomorphic instances

trait StringPCF2 extends CFunction2 {
  def apply(cv1: CValue, cv2: CValue): String 
}
trait BoolPCF2 extends CFunction2 {
  def apply(cv1: CValue, cv2: CValue): Boolean 
}
trait IntPCF2 extends CFunction2 {
  def apply(cv1: CValue, cv2: CValue): Int 
}
trait LongPCF2 extends CFunction2 {
  def apply(cv1: CValue, cv2: CValue): Long 
}
trait FloatPCF2 extends CFunction2 {
  def apply(cv1: CValue, cv2: CValue): Float 
}
trait DoublePCF2 extends CFunction2 {
  def apply(cv1: CValue, cv2: CValue): Double 
}
trait NumPCF2 extends CFunction2 {
  def apply(cv1: CValue, cv2: CValue): BigDecimal 
}

// vim: set ts=4 sw=4 et:
