package com.precog.yggdrasil

import com.precog.common.VectorCase
import blueeyes.json.JsonAST._
import blueeyes.json.JsonParser.parse
import blueeyes.json.JPathField

import scalaz.syntax.bind._
import scalaz.syntax.copointed._

trait IntersectSpec[M[+_]] extends TableModuleSpec[M] {
  import SampleData._
  import trans._
  import trans.constants._

  def testIntersect(l: SampleData, r: SampleData) {
    val ltable = fromSample(l)
    val rtable = fromSample(r)

    val expected: Stream[JValue] = for {
      lv <- l.data
      rv <- r.data
      if (lv \ "key") == (rv \ "key")
    } yield lv

    val result = ops.intersect(DerefObjectStatic(Leaf(Source), JPathField("key")), ltable, rtable)

    val jsonResult: M[Stream[JValue]] = result.flatMap { table => toJson(table) }

    jsonResult.copoint must_== expected
  }

  def testSimpleIntersect = {
    val s1 = SampleData(Stream(toRecord(VectorCase(1), parse("""{"a":[]}""")), toRecord(VectorCase(2), parse("""{"b":[]}"""))))
    val s2 = SampleData(Stream(toRecord(VectorCase(2), parse("""{"b":[]}"""))))

    testIntersect(s1, s2)
  }
}
    
