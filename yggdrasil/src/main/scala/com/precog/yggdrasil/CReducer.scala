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
