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

import blueeyes.json._

import scalaz.StreamT
import scalaz.syntax.copointed._

import org.specs2.ScalaCheck
import org.specs2.mutable._
import org.scalacheck.Gen

trait SampleSpec[M[+_]] extends ColumnarTableModuleTestSupport[M] with Specification with ScalaCheck {
  import SampleData._
  import trans._

  val simpleData: Stream[JValue] = Stream.tabulate(100) { i =>
    JObject(JField("id", if (i % 2 == 0) JString(i.toString) else JNum(i)) :: Nil)
  }

  def testSample = {
    val data = SampleData(simpleData)
    val table = fromSample(data)
    table.sample(15, 2).copoint.toList must beLike {
      case s1 :: s2 :: Nil =>
        val result1 = toJson(s1).copoint
        val result2 = toJson(s2).copoint
        result1 must have size(15)
        result2 must have size(15)
        simpleData must containAllOf(result1)
        simpleData must containAllOf(result2)
    }
  }

  def testLargeSampleSize = {
    val data = SampleData(simpleData)
    fromSample(data).sample(1000, 1).copoint.toList must beLike {
      case s :: Nil =>
        val result = toJson(s).copoint
        result must have size(100)
    }
  }

  def test0SampleSize = {
    val data = SampleData(simpleData)
    fromSample(data).sample(0, 1).copoint.toList must beLike {
      case s :: Nil =>
        val result = toJson(s).copoint
        result must have size(0)
    }
  }
}

