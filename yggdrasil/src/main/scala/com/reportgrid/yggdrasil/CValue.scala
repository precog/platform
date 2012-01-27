package com.reportgrid.yggdrasil

trait CValue {
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
}

case class CInt(value: Int) extends CValue {
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


case class CString(value: String) extends CValue {
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


case class CBool(value: Boolean) extends CValue {
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


case class CFloat(value: Float) extends CValue {
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
