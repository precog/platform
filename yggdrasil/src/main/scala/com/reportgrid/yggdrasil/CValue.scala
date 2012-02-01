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
package com.reportgrid.yggdrasil

import scalaz.Order
import scalaz.std.AllInstances._

trait CValue {
  def typeIndex: Int
  def fold[A](
    str:    String => A,
    bool:   Boolean => A,
    int:    Int => A,
    long:   Long => A,
    float:  Float => A,
    double: Double => A,
    num:    BigDecimal => A,
    emptyObj: => A,
    emptyArr: => A,
    nul:      => A
  ): A

  def toSValue: SValue = fold[SValue](
    str  = SString(_),
    bool = SBoolean(_), 
    int  = SInt(_),
    long = SLong(_),
    float  = SFloat(_),
    double = SDouble(_),
    num    = SDecimal(_),
    emptyObj = SObject(Map()),
    emptyArr = SArray(Vector.empty[SValue]),
    nul      = SNull
  )
}

object CValue {
  implicit object order extends Order[CValue] {
    def order(v1: CValue, v2: CValue) = (v1, v2) match {
      case (CString(a), CString(b)) => Order[String].order(a, b)
      case (CBoolean(a), CBoolean(b)) => Order[Boolean].order(a, b)
      case (CInt(a), CInt(b)) => Order[Int].order(a, b)
      case (CLong(a), CLong(b)) => Order[Long].order(a, b)
      case (CFloat(a), CFloat(b)) => Order[Float].order(a, b)
      case (CDouble(a), CDouble(b)) => Order[Double].order(a, b)
      case (CNum(a), CNum(b)) => Order[BigDecimal].order(a, b)
      case (vx, vy) => Order[Int].order(vx.typeIndex, vy.typeIndex)
    }

  }
}

case class CString(value: String) extends CValue {
  val typeIndex = 0
  def fold[A](
    str:    String => A,
    bool:   Boolean => A,
    int:    Int => A,
    long:   Long => A,
    float:  Float => A,
    double: Double => A,
    num:    BigDecimal => A,
    emptyObj: => A,
    emptyArr: => A,
    nul:      => A
  ): A = str(value)
}

case class CBoolean(value: Boolean) extends CValue {
  val typeIndex = 1
  def fold[A](
    str:    String => A,
    bool:   Boolean => A,
    int:    Int => A,
    long:   Long => A,
    float:  Float => A,
    double: Double => A,
    num:    BigDecimal => A,
    emptyObj: => A,
    emptyArr: => A,
    nul:      => A
  ): A = bool(value)
}

case class CInt(value: Int) extends CValue {
  val typeIndex = 2
  def fold[A](
    str:    String => A,
    bool:   Boolean => A,
    int:    Int => A,
    long:   Long => A,
    float:  Float => A,
    double: Double => A,
    num:    BigDecimal => A,
    emptyObj: => A,
    emptyArr: => A,
    nul:      => A
  ): A = int(value)
}

case class CLong(value: Long) extends CValue {
  val typeIndex = 3
  def fold[A](
    str:    String => A,
    bool:   Boolean => A,
    int:    Int => A,
    long:   Long => A,
    float:  Float => A,
    double: Double => A,
    num:    BigDecimal => A,
    emptyObj: => A,
    emptyArr: => A,
    nul:      => A
  ): A = long(value)
}

case class CFloat(value: Float) extends CValue {
  val typeIndex = 4
  def fold[A](
    str:    String => A,
    bool:   Boolean => A,
    int:    Int => A,
    long:   Long => A,
    float:  Float => A,
    double: Double => A,
    num:    BigDecimal => A,
    emptyObj: => A,
    emptyArr: => A,
    nul:      => A
  ): A = float(value)
}

case class CDouble(value: Double) extends CValue {
  val typeIndex = 5
  def fold[A](
    str:    String => A,
    bool:   Boolean => A,
    int:    Int => A,
    long:   Long => A,
    float:  Float => A,
    double: Double => A,
    num:    BigDecimal => A,
    emptyObj: => A,
    emptyArr: => A,
    nul:      => A
  ): A = double(value)
}

case class CNum(value: BigDecimal) extends CValue {
  val typeIndex = 6
  def fold[A](
    str:    String => A,
    bool:   Boolean => A,
    int:    Int => A,
    long:   Long => A,
    float:  Float => A,
    double: Double => A,
    num:    BigDecimal => A,
    emptyObj: => A,
    emptyArr: => A,
    nul:      => A
  ): A = num(value)
}


case object CEmptyObject extends CValue {
  val typeIndex = 7
  def fold[A](
    str:    String => A,
    bool:   Boolean => A,
    int:    Int => A,
    long:   Long => A,
    float:  Float => A,
    double: Double => A,
    num:    BigDecimal => A,
    emptyObj: => A,
    emptyArr: => A,
    nul:      => A
  ): A = emptyObj
}

case object CEmptyArray extends CValue {
  val typeIndex = 8
  def fold[A](
    str:    String => A,
    bool:   Boolean => A,
    int:    Int => A,
    long:   Long => A,
    float:  Float => A,
    double: Double => A,
    num:    BigDecimal => A,
    emptyObj: => A,
    emptyArr: => A,
    nul:      => A
  ): A = emptyArr
}

case object CNull extends CValue {
  val typeIndex = 9
  def fold[A](
    str:    String => A,
    bool:   Boolean => A,
    int:    Int => A,
    long:   Long => A,
    float:  Float => A,
    double: Double => A,
    num:    BigDecimal => A,
    emptyObj: => A,
    emptyArr: => A,
    nul:      => A
  ): A = nul
}
// vim: set ts=4 sw=4 et:
