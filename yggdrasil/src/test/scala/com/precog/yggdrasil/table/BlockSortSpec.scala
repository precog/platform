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
    implicit val gen = sample(objectSchema(_, 3))
    check { (sample: SampleData) => testSortDense(sample) }
  }

  // Simple test of sorting on homogeneous data
  def testSortSample1 = {
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

    testSortDense(sampleData)
  }

  // Simple test of partially undefined sort key data
  def testSortSample2 = {
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

    testSortDense(sampleData)
  }

  def testSortSample3 = {
    val sampleData = SampleData(
      (JsonParser.parse("""[
         {
           "value":{
            "uid":"hank",
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
            "uid":"fred",
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
        (3, List(JPath(".uid") -> CString,
                 JPath(".f.bn[0]") -> CNull, 
                 JPath(".f.wei") -> CDouble, 
                 JPath(".ljz[0]") -> CNull,
                 JPath(".ljz[1][0]") -> CString,
                 JPath(".ljz[2]") -> CBoolean,
                 JPath(".jmy") -> CDouble))
      )
    )

    testSortDense(sampleData)
  }

  def testSortSample4 = {
    val sampleData = SampleData(
      (JsonParser.parse("""[
        {
          "value":{
            "uid":"fred",
            "dV":{
              "d":true,
              "l":false,
              "vq":{
                
              }
            },
            "oy":{
              "nm":false
            },
            "uR":-6.41847178802919E307
          },
          "key":[1,1]
        }
      ]""") --> classOf[JArray]).elements.toStream,
      Some(
        (2, List(JPath(".uid") -> CString,
                 JPath(".dV.d") -> CBoolean, 
                 JPath(".dV.l") -> CBoolean, 
                 JPath(".dV.vq") -> CEmptyObject, 
                 JPath(".oy.nm") -> CBoolean, 
                 JPath(".uR") -> CDouble))
      )
    )   

    testSortDense(sampleData)
  } 

  def testSortSample5 = {
    val sampleData = SampleData(
      (JsonParser.parse("""[
        {
          "value":{
            "uid":"fred",
            "cfnYTg92dg":"gu",
            "fg":[false,8.988465674311579E307,-1],
            "o8agyghfjxe":[]
          },
          "key":[1]
        },
        {
          "value":{
            "uid":"ted",
            "cfnYTg92dg":"yoqmrz",
            "fg":[false,0.0,0],
            "o8agyghfjxe":[]
          },
          "key":[1]
        },
        {
          "value":{
            "uid":"joe",
            "cfnYTg92dg":"bzjhpndgoY",
            "fg":[true,5.899727648511153E307,0],
            "o8agyghfjxe":[]
          },
          "key":[2]
        },
        {
          "value":{
            "uid":"al",
            "cfnYTg92dg":"ztDcxy",
            "fg":[false,-1.0,-1],
            "o8agyghfjxe":[]
          },
          "key":[2]
        },
        {
          "value":{
            "uid":"brent",
            "cfnYTg92dg":"jeuHxunPdg",
            "fg":[true,3.3513345026993237E307,0],
            "o8agyghfjxe":[]
          },
          "key":[3]
        },
        {
          "value":{
            "uid":"tony",
            "cfnYTg92dg":"evxnIfv",
            "fg":[false,-5.295630177665229E307,1],
            "o8agyghfjxe":[]
          },
          "key":[3]
        },
        {
          "value":{
            "uid":"jerome",
            "cfnYTg92dg":"v",
            "fg":[true,-6.98151882908554E307,3047586736114377501],
            "o8agyghfjxe":[]
          },
          "key":[6]
        },
        {
          "value":{
            "uid":"mike",
            "cfnYTg92dg":"ontecesf",
            "fg":[false,5.647795622045506E307,-1],
            "o8agyghfjxe":[]
          },
          "key":[6]
        },
        {
          "value":{
            "uid":"george",
            "cfnYTg92dg":"",
            "fg":[true,1.0,-4341538468449353975],
            "o8agyghfjxe":[]
          },
          "key":[7]
        },
        {
          "value":{
            "uid":"dan",
            "cfnYTg92dg":"Hwpqxk",
            "fg":[true,-4.38879797446784E307,4611686018427387903],
            "o8agyghfjxe":[]
          },
          "key":[9]
        },
        {
          "value":{
            "uid":"gary",
            "cfnYTg92dg":"mkkhV",
            "fg":[true,-1.0,3724086638589828262],
            "o8agyghfjxe":[]
          },
          "key":[9]
        }
      ]""") --> classOf[JArray]).elements.toStream,
      Some((1, List(JPath(".uid") -> CString,
                    (JPath(".o8agyghfjxe") -> CEmptyArray), 
                    (JPath(".fg[0]") -> CBoolean), 
                    (JPath(".fg[1]") -> CDouble), 
                    (JPath(".fg[2]") -> CLong), 
                    (JPath(".cfnYTg92dg") -> CString))))
    )

    testSortDense(sampleData)
  }

  def testSortDense(sample: SampleData) = {
    //println("testing for sample: " + sample)
    val Some((idCount, schema)) = sample.schema

    val module = new BlockLoadTestSupport[M] with BlockStoreColumnarTableModule[M] {
      def M = self.M
      def coM = self.coM

      val projections = {
        schema.grouped(2) map { subschema =>
          val descriptor = ProjectionDescriptor(
            idCount, 
            subschema map {
              case (jpath, ctype) => ColumnDescriptor(Path("/test"), jpath, ctype, Authorities.None)
            } toList
          )

          descriptor -> Projection( 
            descriptor, 
            sample.data map { jv =>
              subschema.foldLeft[JValue](JObject(JField("key", jv \ "key") :: Nil)) {
                case (obj, (jpath, _)) => 
                  val vpath = JPath(JPathField("value") :: jpath.nodes)
                  obj.set(vpath, jv.get(vpath))
              }
            }
          )
        } toMap
      }

      object storage extends Storage
    }

    import module.trans._
    import TableModule.paths._

    val sortTransspec = WrapObject(DerefObjectStatic(DerefObjectStatic(Leaf(Source), JPathField("value")), JPathField("uid")), "uid")
    module.ops.constString(Set(CString("/test"))).load("", Schema.mkType(schema).get).flatMap {
      _.sort(sortTransspec, SortAscending)
    }.flatMap {
      // Remove the sortkey namespace for the purposes of this spec (simplifies comparisons)
      table => M.point(table.transform(ObjectDelete(Leaf(Source), Set(SortKey))))
    }.flatMap {
      _.toJson
    }.copoint.toStream must_== sample.data.sortBy {
      v => (v \ "value" \? "uid").getOrElse(JString(""))
    }
  }
}

// vim: set ts=4 sw=4 et:
