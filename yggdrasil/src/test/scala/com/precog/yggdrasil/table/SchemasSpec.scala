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

import com.precog.bytecode._

import scala.util.Random

import blueeyes.json._

import scalaz.StreamT
import scalaz.syntax.copointed._

import org.specs2.ScalaCheck
import org.specs2.mutable._

trait SchemasSpec[M[+_]] extends ColumnarTableModuleTestSupport[M] with Specification with ScalaCheck {
  import SampleData._
  import trans._

  def testSingleSchema = {
    val expected = Set(JObjectFixedT(Map("a" -> JNumberT, "b" -> JTextT, "c" -> JNullT)))
    val trivialData = Stream.fill(100)(JParser.parse("""{ "a": 1, "b": "x", "c": null }"""))
    val sample = SampleData(trivialData)
    val table = fromSample(sample, Some(10))
    table.schemas.copoint must_== expected
  }

  def testCrossSliceSchema = {
    val expected = Set(
      JObjectFixedT(Map("a" -> JNumberT, "b" -> JTextT)),
      JObjectFixedT(Map("a" -> JTextT, "b" -> JNumberT))
    )
    val data = Stream.fill(10)(JParser.parse("""{ "a": 1, "b": "2" }""")) ++
      Stream.fill(10)(JParser.parse("""{ "a": "x", "b": 2 }"""))
    val table = fromSample(SampleData(data), Some(10))
    table.schemas.copoint must_== expected
  }

  def testIntervleavedSchema = {
    val expected = Set(
      JObjectFixedT(Map("a" -> JArrayFixedT(Map.empty), "b" -> JTextT)),
      JObjectFixedT(Map("a" -> JNullT, "b" -> JTextT)),
      JObjectFixedT(Map("a" -> JArrayFixedT(Map(0 -> JNumberT, 1 -> JNumberT)), "b" -> JArrayFixedT(Map(0 -> JTextT, 1 -> JObjectFixedT(Map.empty)))))
    )
    val data = Stream.tabulate(30) {
      case i if i % 3 == 0 => JParser.parse("""{ "a": [], "b": "2" }""")
      case i if i % 3 == 1 => JParser.parse("""{ "a": null, "b": "2" }""")
      case _ => JParser.parse("""{ "a": [ 1, 2 ], "b": [ "2", {} ] }""")
    }
    val table = fromSample(SampleData(data), Some(10))
    table.schemas.copoint must_== expected
  }

  def testUndefinedsInSchema = {
    val expected = Set(
      JObjectFixedT(Map("a" -> JNumberT, "b" -> JNumberT)),
      JObjectFixedT(Map("a" -> JNumberT)),
      JObjectFixedT(Map("b" -> JNumberT)),
      JObjectFixedT(Map.empty)
    )
    val data = Stream.tabulate(100) {
      case i if i % 4 == 0 => JObject(List(JField("a", JNum(1)), JField("b", JNum(i))))
      case i if i % 4 == 1 => JObject(List(JField("a", JNum(1)), JField("b", JUndefined)))
      case i if i % 4 == 2 => JObject(List(JField("a", JUndefined), JField("b", JNum(i))))
      case _ => JObject(List(JField("a", JUndefined), JField("b", JUndefined)))
    }

    val table = fromSample(SampleData(data), Some(10))
    table.schemas.copoint must_== expected
  }

  def testAllTypesInSchema = {
    val expected = Set(
      JNumberT,
      JTextT,
      JBooleanT,
      JNullT,
      JArrayFixedT(Map(0 -> JNumberT, 1 -> JNumberT)),
      JObjectFixedT(Map("a" -> JNumberT)),
      JObjectFixedT(Map("a" -> JBooleanT)),
      JObjectFixedT(Map("a" -> JTextT)),
      JObjectFixedT(Map("a" -> JNullT)),
      JObjectFixedT(Map("a" -> JArrayFixedT(Map.empty))),
      JObjectFixedT(Map("a" -> JObjectFixedT(Map.empty))),
      JObjectFixedT(Map("a" -> JArrayFixedT(Map(0 -> JNumberT, 1 -> JTextT, 2 -> JBooleanT)))),
      JObjectFixedT(Map("a" -> JObjectFixedT(Map("b" -> JObjectFixedT(Map("c" -> JNumberT))))))
    )
    val data = Stream(
      "1", "true", "null",
      """ "abc" """,
      """[ 1, 2 ]""",
      """{ "a": 1 }""",
      """{ "a": true }""",
      """{ "a": null }""",
      """{ "a": "a" }""",
      """{ "a": 1.2 }""",
      """{ "a": 112311912931223e-1000 }""",
      """{ "a": [] }""",
      """{ "a": {} }""",
      """{ "a": [ 1, "a", true ] }""",
      """{ "a": { "b": { "c": 3 } } }"""
    ) map (JParser.parse(_))

    val table = fromSample(SampleData(data), Some(10))
    table.schemas.copoint must_== expected
  }
}
