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

