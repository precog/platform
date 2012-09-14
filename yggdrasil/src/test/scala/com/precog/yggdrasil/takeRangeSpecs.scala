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

import scala.collection.immutable.BitSet
import scala.util.Random

import blueeyes.json._
import blueeyes.json.JsonAST._

import scalaz.StreamT
import scalaz.syntax.copointed._

import org.specs2.ScalaCheck
import org.specs2.mutable._

trait TakeRangeSpec[M[+_]] extends ColumnarTableModuleTestSupport[M] with Specification with ScalaCheck {
  import SampleData._
  import trans._

  def testTakeRange = {
    val data: Stream[JValue] = 
      Stream(
        JObject(JField("value", JString("foo")) :: JField("key", JArray(JNum(1) :: Nil)) :: Nil),
        JObject(JField("value", JNum(12)) :: JField("key", JArray(JNum(2) :: Nil)) :: Nil),
        JObject(JField("value", JObject(JField("baz", JBool(true)) :: Nil)) :: JField("key", JArray(JNum(3) :: Nil)) :: Nil),
        JObject(JField("value", JString("ack")) :: JField("key", JArray(JNum(4) :: Nil)) :: Nil))

    val sample = SampleData(data)
    val table = fromSample(sample)

    val results = toJson(table.takeRange(1, 2))

    val expected = Stream(
      JObject(JField("value", JNum(12)) :: JField("key", JArray(JNum(2) :: Nil)) :: Nil),
      JObject(JField("value", JObject(JField("baz", JBool(true)) :: Nil)) :: JField("key", JArray(JNum(3) :: Nil)) :: Nil))

    results.copoint must_== expected
  }

  def testTakeRangeLarger = {
    val data: Stream[JValue] = 
      Stream(
        JObject(JField("value", JString("foo")) :: JField("key", JArray(JNum(1) :: Nil)) :: Nil),
        JObject(JField("value", JNum(12)) :: JField("key", JArray(JNum(2) :: Nil)) :: Nil),
        JObject(JField("value", JObject(JField("baz", JBool(true)) :: Nil)) :: JField("key", JArray(JNum(3) :: Nil)) :: Nil),
        JObject(JField("value", JString("ack")) :: JField("key", JArray(JNum(4) :: Nil)) :: Nil))

    val sample = SampleData(data)
    val table = fromSample(sample)

    val results = toJson(table.takeRange(2, 17))

    val expected = Stream(
      JObject(JField("value", JObject(JField("baz", JBool(true)) :: Nil)) :: JField("key", JArray(JNum(3) :: Nil)) :: Nil),
      JObject(JField("value", JString("ack")) :: JField("key", JArray(JNum(4) :: Nil)) :: Nil))

    results.copoint must_== expected
  }

  def testTakeRangeEmpty = {
    val data: Stream[JValue] = 
      Stream(
        JObject(JField("value", JString("foo")) :: JField("key", JArray(JNum(1) :: Nil)) :: Nil),
        JObject(JField("value", JNum(12)) :: JField("key", JArray(JNum(2) :: Nil)) :: Nil),
        JObject(JField("value", JObject(JField("baz", JBool(true)) :: Nil)) :: JField("key", JArray(JNum(3) :: Nil)) :: Nil),
        JObject(JField("value", JString("ack")) :: JField("key", JArray(JNum(4) :: Nil)) :: Nil))

    val sample = SampleData(data)
    val table = fromSample(sample)

    val results = toJson(table.takeRange(6, 17))

    val expected = Stream()

    results.copoint must_== expected
  }

  def testTakeRangeAcrossSlices = {
    val data: Stream[JValue] = 
      Stream(
        JObject(JField("value", JString("foo")) :: JField("key", JArray(JNum(1) :: Nil)) :: Nil),
        JObject(JField("value", JNum(12)) :: JField("key", JArray(JNum(2) :: Nil)) :: Nil),
        JObject(JField("value", JObject(JField("baz", JBool(true)) :: Nil)) :: JField("key", JArray(JNum(3) :: Nil)) :: Nil),
        JObject(JField("value", JString("ack1")) :: JField("key", JArray(JNum(4) :: Nil)) :: Nil),
        JObject(JField("value", JString("ack2")) :: JField("key", JArray(JNum(5) :: Nil)) :: Nil),
        JObject(JField("value", JString("ack3")) :: JField("key", JArray(JNum(6) :: Nil)) :: Nil),
        JObject(JField("value", JString("ack4")) :: JField("key", JArray(JNum(7) :: Nil)) :: Nil),
        JObject(JField("value", JString("ack5")) :: JField("key", JArray(JNum(8) :: Nil)) :: Nil))

    val sample = SampleData(data)
    val table = fromSample(sample, Some(5))

    val results = toJson(table.takeRange(1, 6))

    val expected = Stream(
      JObject(JField("value", JNum(12)) :: JField("key", JArray(JNum(2) :: Nil)) :: Nil),
      JObject(JField("value", JObject(JField("baz", JBool(true)) :: Nil)) :: JField("key", JArray(JNum(3) :: Nil)) :: Nil),
      JObject(JField("value", JString("ack1")) :: JField("key", JArray(JNum(4) :: Nil)) :: Nil),
      JObject(JField("value", JString("ack2")) :: JField("key", JArray(JNum(5) :: Nil)) :: Nil),
      JObject(JField("value", JString("ack3")) :: JField("key", JArray(JNum(6) :: Nil)) :: Nil),
      JObject(JField("value", JString("ack4")) :: JField("key", JArray(JNum(7) :: Nil)) :: Nil))

    results.copoint must_== expected
  }

  def testTakeRangeSecondSlice = {
    val data: Stream[JValue] = 
      Stream(
        JObject(JField("value", JString("foo")) :: JField("key", JArray(JNum(1) :: Nil)) :: Nil),
        JObject(JField("value", JNum(12)) :: JField("key", JArray(JNum(2) :: Nil)) :: Nil),
        JObject(JField("value", JObject(JField("baz", JBool(true)) :: Nil)) :: JField("key", JArray(JNum(3) :: Nil)) :: Nil),
        JObject(JField("value", JString("ack1")) :: JField("key", JArray(JNum(4) :: Nil)) :: Nil),
        JObject(JField("value", JString("ack2")) :: JField("key", JArray(JNum(5) :: Nil)) :: Nil),
        JObject(JField("value", JString("ack3")) :: JField("key", JArray(JNum(6) :: Nil)) :: Nil),
        JObject(JField("value", JString("ack4")) :: JField("key", JArray(JNum(7) :: Nil)) :: Nil),
        JObject(JField("value", JString("ack5")) :: JField("key", JArray(JNum(8) :: Nil)) :: Nil))

    val sample = SampleData(data)
    val table = fromSample(sample, Some(5))

    val results = toJson(table.takeRange(5, 2))

    val expected = Stream(
      JObject(JField("value", JString("ack3")) :: JField("key", JArray(JNum(6) :: Nil)) :: Nil),
      JObject(JField("value", JString("ack4")) :: JField("key", JArray(JNum(7) :: Nil)) :: Nil))

    results.copoint must_== expected
  }  
  
  def testTakeRangeFirstSliceOnly = {
    val data: Stream[JValue] = 
      Stream(
        JObject(JField("value", JString("foo")) :: JField("key", JArray(JNum(1) :: Nil)) :: Nil),
        JObject(JField("value", JNum(12)) :: JField("key", JArray(JNum(2) :: Nil)) :: Nil),
        JObject(JField("value", JObject(JField("baz", JBool(true)) :: Nil)) :: JField("key", JArray(JNum(3) :: Nil)) :: Nil),
        JObject(JField("value", JString("ack1")) :: JField("key", JArray(JNum(4) :: Nil)) :: Nil),
        JObject(JField("value", JString("ack2")) :: JField("key", JArray(JNum(5) :: Nil)) :: Nil),
        JObject(JField("value", JString("ack3")) :: JField("key", JArray(JNum(6) :: Nil)) :: Nil),
        JObject(JField("value", JString("ack4")) :: JField("key", JArray(JNum(7) :: Nil)) :: Nil),
        JObject(JField("value", JString("ack5")) :: JField("key", JArray(JNum(8) :: Nil)) :: Nil))

    val sample = SampleData(data)
    val table = fromSample(sample, Some(5))

    val results = toJson(table.takeRange(0, 5))

    val expected = Stream(
      JObject(JField("value", JString("foo")) :: JField("key", JArray(JNum(1) :: Nil)) :: Nil),
      JObject(JField("value", JNum(12)) :: JField("key", JArray(JNum(2) :: Nil)) :: Nil),
      JObject(JField("value", JObject(JField("baz", JBool(true)) :: Nil)) :: JField("key", JArray(JNum(3) :: Nil)) :: Nil),
      JObject(JField("value", JString("ack1")) :: JField("key", JArray(JNum(4) :: Nil)) :: Nil),
      JObject(JField("value", JString("ack2")) :: JField("key", JArray(JNum(5) :: Nil)) :: Nil))

    results.copoint must_== expected
  }
}
