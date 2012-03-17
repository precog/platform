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
