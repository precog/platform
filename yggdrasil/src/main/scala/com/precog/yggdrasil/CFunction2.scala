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
