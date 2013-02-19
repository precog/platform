package com.precog.yggdrasil
package table

import blueeyes.json._

import com.precog.common._

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

  val simpleData2: Stream[JValue] = Stream.tabulate(100) { i =>
    JObject(
      JField("id", if (i % 2 == 0) JString(i.toString) else JNum(i)) ::
      JField("value", if (i % 2 == 0) JBool(true) else JNum(i)) :: 
      Nil)
  }

  def testSample = {
    val data = SampleData(simpleData)
    val table = fromSample(data)
    table.sample(15, Seq(TransSpec1.Id, TransSpec1.Id)).copoint.toList must beLike {
      case s1 :: s2 :: Nil =>
        val result1 = toJson(s1).copoint
        val result2 = toJson(s2).copoint
        result1 must have size(15)
        result2 must have size(15)
        simpleData must containAllOf(result1)
        simpleData must containAllOf(result2)
    }
  }

  def testSampleEmpty = {
    val data = SampleData(simpleData)
    val table = fromSample(data)
    table.sample(15, Seq()).copoint.toList mustEqual Nil
  }

  def testSampleTransSpecs = {
    val data = SampleData(simpleData2)
    val table = fromSample(data)
    val specs = Seq(trans.DerefObjectStatic(TransSpec1.Id, CPathField("id")), trans.DerefObjectStatic(TransSpec1.Id, CPathField("value")))

    table.sample(15, specs).copoint.toList must beLike {
      case s1 :: s2 :: Nil =>
        val result1 = toJson(s1).copoint
        val result2 = toJson(s2).copoint
        result1 must have size(15)
        result2 must have size(15)

        val expected1 = toJson(table.transform(trans.DerefObjectStatic(TransSpec1.Id, CPathField("id")))).copoint
        val expected2 = toJson(table.transform(trans.DerefObjectStatic(TransSpec1.Id, CPathField("value")))).copoint
        expected1 must containAllOf(result1)
        expected2 must containAllOf(result2)
    }
  }

  def testLargeSampleSize = {
    val data = SampleData(simpleData)
    fromSample(data).sample(1000, Seq(TransSpec1.Id)).copoint.toList must beLike {
      case s :: Nil =>
        val result = toJson(s).copoint
        result must have size(100)
    }
  }

  def test0SampleSize = {
    val data = SampleData(simpleData)
    fromSample(data).sample(0, Seq(TransSpec1.Id)).copoint.toList must beLike {
      case s :: Nil =>
        val result = toJson(s).copoint
        result must have size(0)
    }
  }
}

