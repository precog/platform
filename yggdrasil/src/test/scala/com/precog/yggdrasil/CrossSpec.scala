package com.precog.yggdrasil

import blueeyes.json.JsonAST._
import blueeyes.json.JsonParser.parse
import scalaz.syntax.copointed._

import org.specs2.ScalaCheck
import org.specs2.mutable._


trait CrossSpec[M[+_]] extends TableModuleTestSupport[M] with Specification with ScalaCheck {
  import SampleData._
  import trans._
  import trans.constants._

  def testCross(l: SampleData, r: SampleData) {
    val ltable = fromSample(l)
    val rtable = fromSample(r)

    def removeUndefined(jv: JValue): JValue = jv match {
      case JObject(jfields) => JObject(jfields collect { case JField(s, v) if v != JNothing => JField(s, removeUndefined(v)) })
      case JArray(jvs) => JArray(jvs map { jv => removeUndefined(jv) })
      case v => v
    }

    val expected: Stream[JValue] = for {
      lv <- l.data
      rv <- r.data
    } yield {
      JObject(JField("left", removeUndefined(lv)) :: JField("right", removeUndefined(rv)) :: Nil)
    }

    val result = ltable.cross(rtable)(
      InnerObjectConcat(WrapObject(Leaf(SourceLeft), "left"), WrapObject(Leaf(SourceRight), "right"))
    )

    val jsonResult: M[Stream[JValue]] = toJson(result)
    jsonResult.copoint must_== expected
  }

  def testSimpleCross = {
    val s1 = SampleData(Stream(toRecord(Array(1), parse("""{"a":[]}""")), toRecord(Array(2), parse("""{"a":[]}"""))))
    val s2 = SampleData(Stream(toRecord(Array(1), parse("""{"b":0}""")), toRecord(Array(2), parse("""{"b":1}"""))))

    testCross(s1, s2)
  }

  def testCrossSingles = {
    val s1 = SampleData(Stream(
      toRecord(Array(1), parse("""{ "a": 1 }""")),
      toRecord(Array(2), parse("""{ "a": 2 }""")),
      toRecord(Array(3), parse("""{ "a": 3 }""")),
      toRecord(Array(4), parse("""{ "a": 4 }""")),
      toRecord(Array(5), parse("""{ "a": 5 }""")),
      toRecord(Array(6), parse("""{ "a": 6 }""")),
      toRecord(Array(7), parse("""{ "a": 7 }""")),
      toRecord(Array(8), parse("""{ "a": 8 }""")),
      toRecord(Array(9), parse("""{ "a": 9 }""")),
      toRecord(Array(10), parse("""{ "a": 10 }""")),
      toRecord(Array(11), parse("""{ "a": 11 }"""))
    ))

    val s2 = SampleData(Stream(
      toRecord(Array(1), parse("""{"b":1}""")), 
      toRecord(Array(2), parse("""{"b":2}"""))))

    testCross(s1, s2)
    testCross(s2, s1)
  }
}

// vim: set ts=4 sw=4 et:
