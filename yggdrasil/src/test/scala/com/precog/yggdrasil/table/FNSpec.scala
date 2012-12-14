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
package table

import org.specs2.mutable._

class FNSpec extends Specification {
  "function implementations" should {
    "partials must work correctly" in {
      val col5 = Column.const(5L)
      val col4 = Column.const(4L)
      val col2 = Column.const(2L)
      val col1 = Column.const(1L)
      val col0 = Column.const(0L)
      val f2 = DivZeroLongP

      f2(col4, col2).forall(_.isDefinedAt(0)) must beTrue
      f2(col4, col0).forall(_.isDefinedAt(0)) must beFalse
      f2(col4, col2) must beLike {
        case Some(c: LongColumn) => c(0) must_== 2L
      }

/*
      val f1 = AddOneLongP.toF1
      f1(col5).forall(_.isDefinedAt(0)) must beFalse
      f1(col4).forall(_.isDefinedAt(0)) must beTrue
      f1(col4).map(_(0)) must beSome(5)

      (f1 andThen f1)(col2).forall(_.isDefinedAt(0)) must beTrue
      (f1 andThen f1)(col2).map(_(0)) must beSome(4)

      (f1 andThen f1)(col4).isDefinedAt(0) must beFalse
      (f1 andThen f1 andThen f1 andThen f1)(col2).isDefinedAt(0) must beFalse

      (f2(col4, col2) |> f1).isDefinedAt(0) must beTrue
      (f2(col4, col2) |> f1)(0) must_== 3

      (f2(col4, col0) |> f1).isDefinedAt(0) must beFalse
      (f2(col5, col1) |> f1).isDefinedAt(0) must beFalse
      (f2(col4, col1) |> f1 |> f1).isDefinedAt(0) must beFalse
      */
    }

    /* performance test
    "draw!" in {
      val testNum = Array.iterate[Long](1000000L, 10000)(_ + 1)

      var vv = 0
      val testDenom = Array.iterate[Long](0, 10000) { _ =>
        vv += 1
        if (vv % 3 == 0) 0 else vv
      }
      
      val pdf = DivZeroLongP
      val paf = AddOneLongP
      val pcomposed = pdf andThen paf andThen paf andThen paf andThen paf 
      val pnum = Column.forArray(CLong, testNum)
      val pden = Column.forArray(CLong, testDenom)

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

      //pt.start
      ok
    }
    */
  }


  val AddOneLongP = CF1P ("testing::ct::addOneLong") {
    case (c: LongColumn) => new LongColumn {
      def isDefinedAt(row: Int) = c.isDefinedAt(row)
      def apply(row: Int) = c(row) + 1
    }
  }

  val DivZeroLongP = CF2P("testing::ct::divzerolong") {
    case (c1: LongColumn, c2: LongColumn) => new LongColumn {
      def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row) && c2(row) != 0
      def apply(row: Int) = c1(row) / c2(row)
    }
  }
}

// vim: set ts=4 sw=4 et:
