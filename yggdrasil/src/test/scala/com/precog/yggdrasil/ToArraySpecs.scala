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

import scalaz.syntax.comonad._

import org.specs2.mutable._

trait ToArraySpec[M[+_]] extends ColumnarTableModuleTestSupport[M] with Specification {
  def testToArrayHomogeneous = {
    val data: Stream[JValue] = 
      Stream(
        JObject(JField("value", JNum(23.4)) :: JField("key", JArray(JNum(1) :: Nil)) :: Nil),
        JObject(JField("value", JNum(12.4)) :: JField("key", JArray(JNum(2) :: Nil)) :: Nil),
        JObject(JField("value", JNum(-12.4)) :: JField("key", JArray(JNum(3) :: Nil)) :: Nil))

    val sample = SampleData(data)
    val table = fromSample(sample)

    val results = toJson(table.toArray[Double])

    val expected = Stream(
      JArray(JNum(23.4) :: Nil),
      JArray(JNum(12.4) :: Nil),
      JArray(JNum(-12.4) :: Nil))

    results.copoint must_== expected
  }

  def testToArrayHeterogeneous = {
    val data: Stream[JValue] = 
      Stream(
        JObject(JField("value", JObject(JField("foo", JNum(23.4)) :: JField("bar", JString("a")) :: Nil)) :: JField("key", JArray(JNum(2) :: Nil)) :: Nil),
        JObject(JField("value", JObject(JField("foo", JNum(23.4)) :: JField("bar", JNum(18.8)) :: Nil)) :: JField("key", JArray(JNum(1) :: Nil)) :: Nil),
        JObject(JField("value", JObject(JField("bar", JNum(44.4)) :: Nil)) :: JField("key", JArray(JNum(3) :: Nil)) :: Nil))

    val sample = SampleData(data)
    val table = fromSample(sample)

    val results = toJson(table.toArray[Double])

    val expected = Stream(JArray(JNum(18.8) :: JNum(23.4) :: Nil))

    results.copoint must_== expected
  }
}
      
        
