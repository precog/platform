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
import com.precog.common._
import com.precog.util._

import blueeyes.json._
import blueeyes.json.JsonAST._

import com.weiglewilczek.slf4s.Logging

import scala.annotation.tailrec
import scala.util.Random
import scalaz._
import scalaz.effect._
import scalaz.std.anyVal._
import scalaz.std.list._
import scalaz.syntax.copointed._
import scalaz.syntax.monad._

import org.specs2.ScalaCheck
import org.specs2.mutable._

import org.scalacheck._
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._
import SampleData._

import TableModule._

trait BlockSortSpec[M[+_]] extends BlockStoreTestSupport[M] with Specification with ScalaCheck { self =>
  implicit def M: Monad[M] with Copointed[M]

  def testSortDense(sample: SampleData, sortKeys: JPath*) = {
    object module extends BlockStoreTestModule {
      val projections = Map.empty[ProjectionDescriptor, Projection]
    }

    //println("testing for sample: " + sample)
    val jvalueOrdering: scala.math.Ordering[JValue] = new scala.math.Ordering[JValue] {
      import blueeyes.json.xschema.DefaultOrderings.JValueOrdering

      def compare(a: JValue, b: JValue): Int = (a,b) match {
        case (JNum(ai), JNum(bd)) => ai.compare(bd)
        case _                    => JValueOrdering.compare(a, b)
      } 
    }

    val resultM = for {
      sorted <- module.fromSample(sample).sort(module.sortTransspec(sortKeys: _*), SortAscending)
      // Remove the sortkey namespace for the purposes of this spec (simplifies comparisons)
      withoutSortKey = sorted.transform(module.deleteSortKeySpec)
      json <- withoutSortKey.toJson
    } yield json

    val result = resultM.copoint.toList

    val original = sample.data.sortBy({
      v => JArray(sortKeys.map(_.extract(v \ "value")).toList).asInstanceOf[JValue]
    })(jvalueOrdering).toList

    //if (result != original) {
    //  println("Original = " + original)
    //  println("Result   = " + result)
    //}

    result must_== original
  }

  def checkSortDense = {
    import TableModule.paths.Value

    implicit val gen = sample(objectSchema(_, 3))
    check { (sample: SampleData) => {
      val Some((_, schema)) = sample.schema

      println("Sorting on " + schema.map(_._1).head)
      testSortDense(sample, schema.map(_._1).head)
    }}
  }

  // Simple test of sorting on homogeneous data
  def homogeneousSortSample = {
    val sampleData = SampleData(
      (JsonParser.parse("""[
        {
          "value":{
            "uid":"joe",
            "u":false,
            "md":"t",
            "l":[]
          },
          "key":[1]
        },
        {
          "value":{
            "uid":"al",
            "u":false,
            "md":"t",
            "l":[]
          },
          "key":[2]
        }
      ]""") --> classOf[JArray]).elements.toStream,
      Some(
        (1 , List(JPath(".uid") -> CString, JPath(".u") -> CBoolean, JPath(".md") -> CString, JPath(".l") -> CEmptyArray))
      )
    )

    testSortDense(sampleData, JPath(".uid"))
  }

  // Simple test of partially undefined sort key data
  def partiallyUndefinedSortSample = {
    val sampleData = SampleData(
      (JsonParser.parse("""[
        {
          "value":{
            "uid":"ted",
            "rzp":{ },
            "hW":1.0,
            "fa":null
          },
          "key":[1]
        },
        {
          "value":{
            "rzp":{ },
            "hW":2.0,
            "fa":null
          },
          "key":[1]
        }
      ]""") --> classOf[JArray]).elements.toStream,
      Some(
        (2, List(JPath(".uid") -> CString, JPath(".fa") -> CNull, JPath(".hW") -> CDouble, JPath(".rzp") -> CEmptyObject))
      )
    )

    testSortDense(sampleData, JPath(".uid"), JPath(".hW"))
  }

  def heterogeneousBaseValueTypeSample = {
    val sampleData = SampleData(
      (JsonParser.parse("""[
        {
          "value": [0, 1],
          "key":[1]
        },
        {
          "value":{
            "uid": "tom",
            "abc": 2
          },
          "key":[2]
        }
      ]""") --> classOf[JArray]).elements.toStream,
      Some(
        (2, List(JPath("[0]") -> CLong, JPath("[1]") -> CLong, JPath(".uid") -> CString, JPath("abc") -> CLong))
      )
    )

    testSortDense(sampleData, JPath(".uid"))
  }

  def badSchemaSortSample = {
    val sampleData = SampleData(
      (JsonParser.parse("""[
        {
          "value":{
            "vxu":[],
            "q":-103811160446995821.5,
            "u":5.548109504404496E+307
          },
          "key":[1.0,1.0]
        },
        {
          "value":{
            "vxu":[],
            "q":-8.40213736307813554E+18,
            "u":8.988465674311579E+307
          },
          "key":[1.0,2.0]
        },
        {
          "value":{
            "m":[],
            "f":false
          },
          "key":[2.0,1.0]
        }
      ]""") --> classOf[JArray]).elements.toStream,
      Some((2,List(
        JPath(".m") -> CEmptyArray,
        JPath(".f") -> CBoolean,
        JPath(".u") -> CDouble,
        JPath(".q") -> CNum,
        JPath(".vxu") -> CEmptyArray))))
    testSortDense(sampleData, JPath("q"))
  }

  // Simple test of heterogeneous sort keys
  def heterogeneousSortSample = {
    val sampleData = SampleData(
      (JsonParser.parse("""[
         {
           "value":{
            "uid": 12,
             "f":{
               "bn":[null],
               "wei":1.0
             },
             "ljz":[null,["W"],true],
             "jmy":4.639428637939817E307
           },
           "key":[1,2,2]
         },
         {
           "value":{
            "uid": 1.5,
             "f":{
               "bn":[null],
               "wei":5.615997508833152E307
             },
             "ljz":[null,[""],false],
             "jmy":-2.612503123965922E307
           },
           "key":[2,1,1]
         }
      ]""") --> classOf[JArray]).elements.toStream,
      Some(
        (3, List(JPath(".uid") -> CLong,
                 JPath(".uid") -> CDouble,
                 JPath(".f.bn[0]") -> CNull, 
                 JPath(".f.wei") -> CDouble, 
                 JPath(".ljz[0]") -> CNull,
                 JPath(".ljz[1][0]") -> CString,
                 JPath(".ljz[2]") -> CBoolean,
                 JPath(".jmy") -> CDouble))
      )
    )

    testSortDense(sampleData, JPath(".uid"))
  }

  def secondHetSortSample = {
    val sampleData = SampleData(
      (JsonParser.parse("""[
      {
        "value":[1.0,0,{
          
        }],
        "key":[3.0]
      }, {
        "value":{
          "e":null,
          "chl":-1.0,
          "zw1":-4.611686018427387904E-27271
        },
        "key":[1.0]
      }, {
        "value":{
          "e":null,
          "chl":-8.988465674311579E+307,
          "zw1":81740903825956729.9
        },
        "key":[2.0]
      }]""") --> classOf[JArray]).elements.toStream,
      Some(
        (1, List(JPath(".e") -> CNull,
                 JPath(".chl") -> CNum,
                 JPath(".zw1") -> CNum,
                 JPath("[0]") -> CLong,
                 JPath("[1]") -> CLong,
                 JPath("[2]") -> CEmptyObject))
      )
    )

    testSortDense(sampleData, JPath(".zw1"))
  }
}

// vim: set ts=4 sw=4 et:
