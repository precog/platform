package com.precog.yggdrasil

import org.apfloat.Apfloat
import org.apfloat.Apfloat._
import org.apfloat.ApfloatMath._

import org.specs2.mutable.Specification

import org.scalacheck._
import org.scalacheck.Prop._
import org.scalacheck.Arbitrary._

object Timer {
  var initial: Long = 0L
  var ending: Long = 0L
  
  def start = {
   initial = System.currentTimeMillis
  }
  
  def stop = {
    ending = System.currentTimeMillis
    println((ending - initial) + " ms")
  }
}

class BigDApfloatSpec extends Specification {
  import Timer._

  implicit def intTimes(n: Int) = new {
    def times(f: => Unit) = 1 to n foreach { _ => f }
  }

  val d20 = BigDecimal(9374.8726354987652380)
  val d40 = BigDecimal(9374.872635498765238093748726354987652380)
  val d60 = BigDecimal(9374.87263549876523809374872635498765238093748726354987652380)
  val d80 = BigDecimal(9374.8726354987652380937487263549876523809374872635498765238093748726354987652380)

  val a20 = new Apfloat(9374.8726354987652380)
  val a40 = new Apfloat(9374.872635498765238093748726354987652380)
  val a60 = new Apfloat(9374.87263549876523809374872635498765238093748726354987652380)
  val a80 = new Apfloat(9374.8726354987652380937487263549876523809374872635498765238093748726354987652380)



  println("BigDecimal multiply p=20: ")

  3 times {
    start
    1000000 times {
      d20 * d20
    }
    stop
  }

  println("Apfloat multiply p=20: ")

  3 times {
    start
    1000000 times {
      a20.multiply(a20)
    }
    stop

  }  
  
  println("BigDecimal multiply p=40: ")

  3 times {
    start
    1000000 times {
      d40 * d40
    }
    stop
  }

  println("Apfloat multiply p=40: ")

  3 times {
    start
    1000000 times {
      a40.multiply(a40)
    }
    stop

  }  
  
  println("BigDecimal multiply p=60: ")

  3 times {
    start
    1000000 times {
      d60 * d60
    }
    stop
  }

  println("Apfloat multiply p=60: ")

  3 times {
    start
    1000000 times {
      a60.multiply(a60)
    }
    stop
  }    
  println("BigDecimal multiply p=80: ")

  3 times {
    start
    1000000 times {
      d80 * d80
    }
    stop
  }

  println("Apfloat multiply p=80: ")

  3 times {
    start
    1000000 times {
      a80.multiply(a80)
    }
    stop
  }  
}

// vim: set ts=4 sw=4 et:
