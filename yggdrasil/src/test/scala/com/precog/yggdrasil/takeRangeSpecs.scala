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

  def testTakeRangeNegStart = {
    val data: Stream[JValue] = 
      Stream(
        JObject(JField("value", JString("foo")) :: JField("key", JArray(JNum(1) :: Nil)) :: Nil),
        JObject(JField("value", JNum(12)) :: JField("key", JArray(JNum(2) :: Nil)) :: Nil),
        JObject(JField("value", JObject(JField("baz", JBool(true)) :: Nil)) :: JField("key", JArray(JNum(3) :: Nil)) :: Nil),
        JObject(JField("value", JString("ack")) :: JField("key", JArray(JNum(4) :: Nil)) :: Nil))

    val sample = SampleData(data)
    val table = fromSample(sample)

    val results = toJson(table.takeRange(-1, 5))

    results.copoint must_== Stream()
  }

  def testTakeRangeNegNumber = {
    val data: Stream[JValue] = 
      Stream(
        JObject(JField("value", JString("foo")) :: JField("key", JArray(JNum(1) :: Nil)) :: Nil),
        JObject(JField("value", JNum(12)) :: JField("key", JArray(JNum(2) :: Nil)) :: Nil),
        JObject(JField("value", JObject(JField("baz", JBool(true)) :: Nil)) :: JField("key", JArray(JNum(3) :: Nil)) :: Nil),
        JObject(JField("value", JString("ack")) :: JField("key", JArray(JNum(4) :: Nil)) :: Nil))

    val sample = SampleData(data)
    val table = fromSample(sample)

    val results = toJson(table.takeRange(2, -3))

    results.copoint must_== Stream()
  }

  def testTakeRangeNeg = {
    val data: Stream[JValue] = 
      Stream(
        JObject(JField("value", JString("foo")) :: JField("key", JArray(JNum(1) :: Nil)) :: Nil),
        JObject(JField("value", JNum(12)) :: JField("key", JArray(JNum(2) :: Nil)) :: Nil),
        JObject(JField("value", JObject(JField("baz", JBool(true)) :: Nil)) :: JField("key", JArray(JNum(3) :: Nil)) :: Nil),
        JObject(JField("value", JString("ack")) :: JField("key", JArray(JNum(4) :: Nil)) :: Nil))

    val sample = SampleData(data)
    val table = fromSample(sample)

    val results = toJson(table.takeRange(-1, 5))

    results.copoint must_== Stream()
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
