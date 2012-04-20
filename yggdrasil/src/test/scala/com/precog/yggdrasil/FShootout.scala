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

import org.specs2.mutable._

class FShootout extends Specification {
  "function implementations" should {
    "draw!" in {
      val testNum = Array.iterate[Long](1000000L, 10000)(_ + 1)

      var vv = 0
      val testDenom = Array.iterate[Long](0, 10000) { _ =>
        vv += 1
        if (vv % 3 == 0) 0 else vv
      }
      
      val pdf = DivZeroLongP.toF2
      val paf = AddOneLongP.toF1
      val pcomposed = pdf andThen paf andThen paf andThen paf andThen paf andThen paf
      val pnum = Column.forArray(CLong, testNum)
      val pden = Column.forArray(CLong, testDenom)

      val edf = DivZeroLongE.toFE2
      val eaf = AddOneLongE.toFE1
      val ecomposed = edf andThen eaf andThen eaf andThen eaf andThen eaf andThen eaf
      val enum = FE0.forArray(CLong, testNum)
      val eden = FE0.forArray(CLong, testDenom)

      //val pt = new Thread {
      ////  override def run = {
        {
          var i = 0
          var sum: Long = 0
          var startTime: Long = 0
          val f = pcomposed(pnum, pden)
          while (i < 10000) {
            if (i == 1000) startTime = System.currentTimeMillis
            var j = 0
            while (j < 10000) {
              if (f.isDefinedAt(j)) sum += f(j)
              j += 1
            }

            i += 1
          }

          val endTime = System.currentTimeMillis

          println("Partial: " + ((endTime - startTime) / 1000.0) + ": " + sum)
        }
      //  }
      //}

      //val et = new Thread {
      //  override def run = {
        {
          var i = 0
          var sum: Long = 0
          var startTime: Long = 0
          val f = pcomposed(pnum, pden)
          while (i < 10000) {
            if (i == 1000) startTime = System.currentTimeMillis
            var j = 0
            while (j < 10000) {
              try {
               sum += f(j)
              } catch {
                case ex => ()
              }
              j += 1
            }

            i += 1
          }

          val endTime = System.currentTimeMillis

          println("Error: " + ((endTime - startTime) / 1000.0) + ": " + sum)
        }
      //  }
      //}

      //pt.start
      //et.start
      ok
    }
  }
}


object AddOneLongP extends F1P[Long, Long] {
  val accepts = CLong
  val returns = CLong
  def isDefinedAt(a: Long) = a % 5 != 0
  def apply(a: Long) = a + 1
}

object DivZeroLongP extends F2P[Long, Long, Long] {
  val accepts = (CLong, CLong)
  val returns = CLong
  def isDefinedAt(a: Long, b: Long) = b != 0
  def apply(a: Long, b: Long) = a / b
}

object AddOneLongE extends FE1P[Long, Long] {
  val addOneException = new Exception("a mod five")

  val accepts = CLong
  val returns = CLong
  def apply(a: Long) = {
    if (a % 5 == 0) throw addOneException
    else a + 1
  }
}

object DivZeroLongE extends FE2P[Long, Long, Long] {
  val divZeroException = new Exception("divide by zero")

  val accepts = (CLong, CLong)
  val returns = CLong
  def apply(a: Long, b: Long) = {
    if (b == 0) throw divZeroException else a / b
  }
}



// vim: set ts=4 sw=4 et:
