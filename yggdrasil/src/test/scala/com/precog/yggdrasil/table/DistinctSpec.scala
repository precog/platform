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

import scala.util.Random

import blueeyes.json._

import scalaz.StreamT
import scalaz.syntax.copointed._

import org.specs2.ScalaCheck
import org.specs2.mutable._

trait DistinctSpec[M[+_]] extends ColumnarTableModuleTestSupport[M] with Specification with ScalaCheck {
  import SampleData._
  import trans._

  def testDistinctIdentity = {
    implicit val gen = sort(distinct(sample(schema)))
    check { (sample: SampleData) =>
      val table = fromSample(sample)

      val distinctTable = table.distinct(Leaf(Source))

      val result = toJson(distinctTable)

      result.copoint must_== sample.data
    }
  }

  def testDistinctAcrossSlices = {
    val array: JValue = JParser.parse("""
      [{
        "value":{

        },
        "key":[1.0,1.0]
      },
      {
        "value":{

        },
        "key":[1.0,1.0]
      },
      {
        "value":{

        },
        "key":[2.0,1.0]
      },
      {
        "value":{

        },
        "key":[2.0,2.0]
      },
      {
        "value":{
          "fzz":false,
          "em":[{
            "l":210574764564691785.5,
            "fbk":-1.0
          },[[],""]],
          "z3y":[{
            "wd":null,
            "tv":false,
            "o":[]
          },{
            "sry":{

            },
            "in0":[]
          }]
        },
        "key":[1.0,2.0]
      },
      {
        "value":{
          "fzz":false,
          "em":[{
            "l":210574764564691785.5,
            "fbk":-1.0
          },[[],""]],
          "z3y":[{
            "wd":null,
            "tv":false,
            "o":[]
          },{
            "sry":{

            },
            "in0":[]
          }]
        },
        "key":[1.0,2.0]
      }]""")

    val data: Stream[JValue] = (array match {
      case JArray(li) => li
      case _ => sys.error("Expected a JArray")
    }).toStream

    val sample = SampleData(data)
    val table = fromSample(sample, Some(5))

    val result = toJson(table.distinct(Leaf(Source)))

    result.copoint must_== sample.data.toSeq.distinct
  }

  def testDistinctAcrossSlices2 = {
    val array: JValue = JParser.parse("""
      [{
        "value":{
          "elxk7vv":-8.988465674311579E+307
        },
        "key":[1.0,1.0]
      },
      {
        "value":{
          "elxk7vv":-8.988465674311579E+307
        },
        "key":[1.0,1.0]
      },
      {
        "value":{
          "elxk7vv":-6.465000919622952E+307
        },
        "key":[2.0,4.0]
      },
      {
        "value":{
          "elxk7vv":-2.2425006462798597E+307
        },
        "key":[4.0,3.0]
      },
      {
        "value":{
          "elxk7vv":-1.0
        },
        "key":[5.0,8.0]
      },
      {
        "value":{
          "elxk7vv":-1.0
        },
        "key":[5.0,8.0]
      },
      {
        "value":[[]],
        "key":[3.0,1.0]
      },
      {
        "value":[[]],
        "key":[3.0,8.0]
      },
      {
        "value":[[]],
        "key":[6.0,7.0]
      },
      {
        "value":[[]],
        "key":[7.0,2.0]
      },
      {
        "value":[[]],
        "key":[8.0,1.0]
      },
      {
        "value":[[]],
        "key":[8.0,1.0]
      },
      {
        "value":[[]],
        "key":[8.0,4.0]
      }]""")

    val data: Stream[JValue] = (array match {
      case JArray(li) => li
      case _ => sys.error("Expected JArray")
    }).toStream

    val sample = SampleData(data)
    val table = fromSample(sample, Some(5))

    val result = toJson(table.distinct(Leaf(Source)))

    result.copoint must_== sample.data.toSeq.distinct
  }

  def removeUndefined(jv: JValue): JValue = jv match {
      case JObject(jfields) => JObject(jfields collect { case (s, v) if v != JUndefined => JField(s, removeUndefined(v)) })
      case JArray(jvs) => JArray(jvs map { jv => removeUndefined(jv) })
      case v => v
    }

  def testDistinct = {
    implicit val gen = sort(duplicateRows(sample(schema)))
    check { (sample: SampleData) =>
      val table = fromSample(sample)

      val distinctTable = table.distinct(Leaf(Source))

      val result = toJson(distinctTable).copoint
      val expected = sample.data.toSeq.distinct

      result must_== expected
    }.set(minTestsOk -> 2000)
  }
}
