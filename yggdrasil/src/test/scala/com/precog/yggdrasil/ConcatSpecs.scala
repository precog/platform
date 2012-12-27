package com.precog.yggdrasil
package table

import org.specs2.mutable._

import scalaz.syntax.copointed._

import blueeyes.json._

trait ConcatSpec[M[+_]] extends ColumnarTableModuleTestSupport[M] with Specification {
  def testConcat = {
    val json1 = """{ "a": 1, "b": "x", "c": null }"""
    val json2 = """[4, "foo", null, true]"""

    val data1: Stream[JValue] = Stream.fill(25)(JParser.parse(json1))
    val data2: Stream[JValue] = Stream.fill(35)(JParser.parse(json2))
    
    val table1 = fromSample(SampleData(data1), Some(10))
    val table2 = fromSample(SampleData(data2), Some(10))

    val results = toJson(table1.concat(table2))
    val expected = data1 ++ data2

    results.copoint must_== expected
  }
}
