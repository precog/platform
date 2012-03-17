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

/**
 * A built-in function can implement any number of type-specialized interfaces.
 * In the case that a type conversion must be made by the function,
 * it is necessary to implement one of the PCF1 interfaces; in this case,
 * the fact that the 'apply' method is overloaded will allow the compiler
 * to enforce the fact that only a single target type can be chosen.
 */
sealed trait CFunction1 
trait StringCF1 extends CFunction1 {
  def applyString(s: String): String
}
trait BoolCF1 extends CFunction1 {
  def applyBool(b: Boolean): Boolean
}
trait IntCF1 extends CFunction1 {
  def applyInt(i: Int): Int
}
trait LongCF1 extends CFunction1 {
  def applyLong(l: Long): Long
}
trait FloatCF1 extends CFunction1 {
  def applyFloat(f: Float): Float
}
trait DoubleCF1 extends CFunction1 {
  def applyDouble(d: Double): Double
}
trait NumCF1 extends CFunction1 {
  def applyNum(v: BigDecimal): BigDecimal
}

// non-endomorphic instances 

trait StringPCF1 extends CFunction1 {
  def apply(cv: CValue): String
}
trait BoolPCF1 extends CFunction1 {
  def apply(cv: CValue): Boolean
}
trait IntPCF1 extends CFunction1 {
  def apply(cv: CValue): Int
}
trait LongPCF1 extends CFunction1 {
  def apply(cv: CValue): Long
}
trait FloatPCF1 extends CFunction1 {
  def apply(cv: CValue): Float
}
trait DoublePCF1 extends CFunction1 {
  def apply(cv: CValue): Double
}
trait NumPCF1 extends CFunction1 {
  def apply(cv: CValue): BigDecimal
}


// vim: set ts=4 sw=4 et:
