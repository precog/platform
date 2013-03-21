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
      
        
