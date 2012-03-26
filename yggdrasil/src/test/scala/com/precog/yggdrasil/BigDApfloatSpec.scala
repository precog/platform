package com.precog.yggdrasil

import org.apfloat.Apfloat
import org.apfloat.Apfloat._
import org.apfloat.ApfloatMath._

import org.specs2.mutable.Specification

object Timer {
  var initial:Long = 0L
  var ending:Long = 0L
  
  def start = {
   initial = System.currentTimeMillis
  }
  
  def stop = {
    ending = System.currentTimeMillis
    println((ending - initial) + " ms")
  }
}

class BigDApfloatSpec extends Specification{
  import Timer._

  val d1: BigDecimal = -12343234234232.234234
  val d2: BigDecimal = 234324995.229098090

  val a1 = new Apfloat(-12343234234232.234234, 128) 
  val a2 = new Apfloat(234324995.229098090, 128)

  print("BigDecimal abs: ")
  start
  d1.abs
  stop

  print("Apfloat abs: ")
  start
  abs(a1)
  stop

  print("BigDecimal multiply: ")
  start
  d1 * d2
  stop

  print("Apfloat multiply: ")
  start
  a1.multiply(a2)
  stop
  
  

}

// vim: set ts=4 sw=4 et:
