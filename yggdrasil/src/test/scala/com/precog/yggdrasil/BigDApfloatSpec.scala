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
  
  //println("BigDecimal add large: ")

  //3 times {
  //  start
  //  1000000 times {
  //    d3 + d4
  //  }
  //  stop
  //}

  //println("Apfloat add large: ")

  //3 times {
  //  start
  //  1000000 times {
  //    a3.add(a4)
  //  }
  //  stop
  //}  
  //
  //println("BigDecimal add small: ")

  //3 times {
  //  start
  //  1000000 times {
  //    d1 + d2
  //  }
  //  stop
  //}

  //println("Apfloat add small: ")

  //3 times {
  //  start
  //  1000000 times {
  //    a1.add(a2)
  //  }
  //  stop

  //}
}

// vim: set ts=4 sw=4 et:
