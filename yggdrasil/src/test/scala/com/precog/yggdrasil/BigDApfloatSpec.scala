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
