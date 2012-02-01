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
