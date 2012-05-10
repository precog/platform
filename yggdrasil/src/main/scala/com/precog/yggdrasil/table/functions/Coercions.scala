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
