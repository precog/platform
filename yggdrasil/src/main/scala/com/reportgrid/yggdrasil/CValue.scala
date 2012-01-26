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



// vim: set ts=4 sw=4 et:
