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

trait BlockSortSpec[M[+_]] extends Specification with ScalaCheck { self =>
  implicit def M: Monad[M]
  implicit def coM: Copointed[M]

  def checkSortDense = {
    import TableModule.paths.Value

    implicit val gen = sample(objectSchema(_, 3))
    check { (sample: SampleData) => {
      val Some((_, schema)) = sample.schema

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
            "hW":1.0,
            "fa":null
          },
          "key":[1]
        }
      ]""") --> classOf[JArray]).elements.toStream,
      Some(
        (2, List(JPath(".uid") -> CString, JPath(".fa") -> CNull, JPath(".hW") -> CDouble, JPath(".rzp") -> CEmptyObject))
      )
    )

    testSortDense(sampleData, JPath(".uid"))
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
         ]
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


  def testSortDense(sample: SampleData, sortKey: JPath) = {
    //println("testing for sample: " + sample)
    val Some((idCount, schema)) = sample.schema

    def compliesWithSchema(jv: JValue, ctype: CType): Boolean = (jv, ctype) match {
      case (_: JNum, CNum | CLong | CDouble) => true
      case (JNothing, CUndefined) => true
      case (JNull, CNull) => true
      case (_: JBool, CBoolean) => true
      case (_: JString, CString) => true
      case (JObject(Nil), CEmptyObject) => true
      case (JArray(Nil), CEmptyArray) => true
      case _ => false
    }
    
    val actualSchema = inferSchema(sample.data map { _ \ "value" })

    val module = new BlockLoadTestSupport[M] with BlockStoreColumnarTableModule[M] {
      def M = self.M
      def coM = self.coM

      val projections = {
        actualSchema.grouped(1) map { subschema =>
          val descriptor = ProjectionDescriptor(
            idCount, 
            subschema flatMap {
              case (jpath, CNum | CLong | CDouble) =>
                List(CNum, CLong, CDouble) map { ColumnDescriptor(Path("/test"), jpath, _, Authorities.None) }
              
              case (jpath, ctype) =>
                List(ColumnDescriptor(Path("/test"), jpath, ctype, Authorities.None))
            } toList
          )

          descriptor -> Projection( 
            descriptor, 
            sample.data flatMap { jv =>
              val back = subschema.foldLeft[JValue](JObject(JField("key", jv \ "key") :: Nil)) {
                case (obj, (jpath, ctype)) => { 
                  val vpath = JPath(JPathField("value") :: jpath.nodes)
                  val valueAtPath = jv.get(vpath)
                  
                  if (compliesWithSchema(valueAtPath, ctype)) {
                    val result = obj.set(vpath, valueAtPath)
                    println("result in compliesWithSchema: %s\n".format(result))
                    result
                  } else
                    obj
                }
              }
              
              if (back \ "value" == JNothing)
                None
              else
                Some(back)
            }
          )
        } toMap
      }

      object storage extends Storage
    }

    import module.trans._
    import TableModule.paths._

    val derefTransspec: TransSpec1 = sortKey.nodes.foldLeft[TransSpec1](DerefObjectStatic(Leaf(Source), JPathField("value"))) {
      case (innerSpec, field: JPathField) => DerefObjectStatic(innerSpec, field)
      case (innerSpec, index: JPathIndex) => DerefArrayStatic(innerSpec, index)
    }

    val sortTransspec = WrapArray(derefTransspec)

    val jvalueOrdering: scala.math.Ordering[JValue] = new scala.math.Ordering[JValue] {
      import blueeyes.json.xschema.DefaultOrderings.JValueOrdering

      def compare(a: JValue, b: JValue): Int = (a,b) match {
        case (JNum(ai), JNum(bd)) => ai.compare(bd)
        case _                    => JValueOrdering.compare(a, b)
      } 
    }

    try {
      val result = module.ops.constString(Set(CString("/test"))).load("", Schema.mkType(schema).get).flatMap {
        _.sort(sortTransspec, SortAscending)
      }.flatMap {
        // Remove the sortkey namespace for the purposes of this spec (simplifies comparisons)
        table => M.point(table.transform(ObjectDelete(Leaf(Source), Set(SortKey))))
      }.flatMap {
        _.toJson
      }.copoint.toList

      val original = sample.data.sortBy({
        v => sortKey.extract(v \ "value")
      })(jvalueOrdering).toList

      if (result != original) {
        println("Original = " + original)
        println("Result   = " + result)
      }

      result must_== original
    } catch {
      case e: AssertionError => e.printStackTrace; true mustEqual false
    }
  }
}

// vim: set ts=4 sw=4 et:
