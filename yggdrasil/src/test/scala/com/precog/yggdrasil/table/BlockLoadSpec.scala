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
import com.precog.yggdrasil.util._

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

trait BlockLoadTestSupport[M[+_]] extends
  TestColumnarTableModule[M] with
  StubStorageModule[M] with
  IdSourceScannerModule[M] with
  TableModuleTestSupport[M] {
  
  type YggConfig = IdSourceConfig
  val yggConfig = new IdSourceConfig {
    val idSource = new IdSource {
      private val source = new java.util.concurrent.atomic.AtomicLong
      def nextId() = source.getAndIncrement
    }
  }
  
  type Key = JArray
  case class Projection(descriptor: ProjectionDescriptor, data: Stream[JValue]) extends BlockProjectionLike[JArray, Slice] {
    val slices = fromJson(data).slices.toStream.copoint

    def insert(id : Identities, v : Seq[CValue], shouldSync: Boolean = false): IO[Unit] = IO(sys.error("Insert not supported."))

    implicit val keyOrder: Order[JArray] = Order[List[JValue]].contramap((_: JArray).elements)

    def getBlockAfter(id: Option[JArray], colSelection: Set[ColumnDescriptor] = Set()): Option[BlockProjectionData[JArray, Slice]] = {
      @tailrec def findBlockAfter(id: JArray, blocks: Stream[Slice]): Option[Slice] = {
        blocks.filterNot(_.isEmpty) match {
          case x #:: xs =>
            if ((x.toJson(x.size - 1).getOrElse(JNothing) \ "key") > id) Some(x) else findBlockAfter(id, xs)

          case _ => None
        }
      }

      val slice = id map { key =>
        findBlockAfter(key, slices) 
      } getOrElse {
        slices.headOption
      }
      
      slice map { s => 
        val s0 = new Slice {
          val size = s.size
          val columns = s.columns filter {
            case (ColumnRef(jpath, ctype), _) =>
              colSelection.isEmpty || 
              jpath.nodes.head == JPathField("key") ||
              colSelection.exists { desc => (JPathField("value") \ desc.selector) == jpath && desc.valueType == ctype }
          }
        }

        BlockProjectionData[JArray, Slice](s0.toJson(0).getOrElse(JNothing) \ "key" --> classOf[JArray], s0.toJson(s0.size - 1).getOrElse(JNothing) \ "key" --> classOf[JArray], s0)
      }
    }
  }
}


trait BlockLoadSpec[M[+_]] extends Specification with ScalaCheck { self =>
  implicit def M: Monad[M] with Copointed[M]

  def checkLoadDense = {
    implicit val gen = sample(objectSchema(_, 3))
    check { (sample: SampleData) => testLoadDense(sample) }
  }

  def testLoadSample1 = {
    val sampleData = SampleData(
      (JsonParser.parse("""[
        {
          "value":{
            "u":false,
            "md":"t",
            "l":[]
          },
          "key":[1]
        }
      ]""") --> classOf[JArray]).elements.toStream,
      Some(
        (1 , List(JPath(".u") -> CBoolean, JPath(".md") -> CString, JPath(".l") -> CEmptyArray))
      )
    )

    testLoadDense(sampleData)
  }

  def testLoadSample2 = {
    val sampleData = SampleData(
      (JsonParser.parse("""[
        {
          "value":{
            "rzp":{ },
            "hW":1.0,
            "fa":null
          },
          "key":[2,1]
        }
      ]""") --> classOf[JArray]).elements.toStream,
      Some(
        (2, List(JPath(".fa") -> CNull, JPath(".hW") -> CLong, JPath(".rzp") -> CEmptyObject))
      )
    )

    testLoadDense(sampleData)
  }

  def testLoadSample3 = {
    val sampleData = SampleData(
      (JsonParser.parse("""[
         {
           "value":{
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
        (3, List(JPath(".f.bn[0]") -> CNull, 
                 JPath(".f.wei") -> CLong, 
                 JPath(".f.wei") -> CDouble, 
                 JPath(".ljz[0]") -> CNull,
                 JPath(".ljz[1][0]") -> CString,
                 JPath(".ljz[2]") -> CBoolean,
                 JPath(".jmy") -> CDouble))
      )
    )

    testLoadDense(sampleData)
  }

  def testLoadSample4 = {
    val sampleData = SampleData(
      (JsonParser.parse("""[
        {
          "value":{
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
        (2, List(JPath(".dV.d") -> CBoolean, 
                 JPath(".dV.l") -> CBoolean, 
                 JPath(".dV.vq") -> CEmptyObject, 
                 JPath(".oy.nm") -> CBoolean, 
                 JPath(".uR") -> CDouble))
      )
    )   

    testLoadDense(sampleData)
  } 

  def testLoadSample5 = {
    val sampleData = SampleData(
      (JsonParser.parse("""[
        {
          "value":{
            "cfnYTg92dg":"gu",
            "fg":[false,8.988465674311579E307,-1],
            "o8agyghfjxe":[]
          },
          "key":[1]
        },
        {
          "value":{
            "cfnYTg92dg":"yoqmrz",
            "fg":[false,0.0,0],
            "o8agyghfjxe":[]
          },
          "key":[1]
        },
        {
          "value":{
            "cfnYTg92dg":"bzjhpndgoY",
            "fg":[true,5.899727648511153E307,0],
            "o8agyghfjxe":[]
          },
          "key":[2]
        },
        {
          "value":{
            "cfnYTg92dg":"ztDcxy",
            "fg":[false,-1.0,-1],
            "o8agyghfjxe":[]
          },
          "key":[2]
        },
        {
          "value":{
            "cfnYTg92dg":"jeuHxunPdg",
            "fg":[true,3.3513345026993237E307,0],
            "o8agyghfjxe":[]
          },
          "key":[3]
        },
        {
          "value":{
            "cfnYTg92dg":"evxnIfv",
            "fg":[false,-5.295630177665229E307,1],
            "o8agyghfjxe":[]
          },
          "key":[3]
        },
        {
          "value":{
            "cfnYTg92dg":"v",
            "fg":[true,-6.98151882908554E307,3047586736114377501],
            "o8agyghfjxe":[]
          },
          "key":[6]
        },
        {
          "value":{
            "cfnYTg92dg":"ontecesf",
            "fg":[false,5.647795622045506E307,-1],
            "o8agyghfjxe":[]
          },
          "key":[6]
        },
        {
          "value":{
            "cfnYTg92dg":"",
            "fg":[true,1.0,-4341538468449353975],
            "o8agyghfjxe":[]
          },
          "key":[7]
        },
        {
          "value":{
            "cfnYTg92dg":"Hwpqxk",
            "fg":[true,-4.38879797446784E307,4611686018427387903],
            "o8agyghfjxe":[]
          },
          "key":[9]
        },
        {
          "value":{
            "cfnYTg92dg":"mkkhV",
            "fg":[true,-1.0,3724086638589828262],
            "o8agyghfjxe":[]
          },
          "key":[9]
        }
      ]""") --> classOf[JArray]).elements.toStream,
      Some((1, List((JPath(".o8agyghfjxe") -> CEmptyArray), 
                    (JPath(".fg[0]") -> CBoolean), 
                    (JPath(".fg[1]") -> CNum), 
                    (JPath(".fg[1]") -> CLong), 
                    (JPath(".fg[2]") -> CNum), 
                    (JPath(".fg[2]") -> CLong), 
                    (JPath(".cfnYTg92dg") -> CString))))
    )

    testLoadDense(sampleData)
  }

  def testLoadDense(sample: SampleData) = {
    //println("testing for sample: " + sample)
    val Some((idCount, schema)) = sample.schema

    val module = new BlockLoadTestSupport[M] with BlockStoreColumnarTableModule[M] {
      def M = self.M

      trait TableCompanion extends BlockStoreColumnarTableCompanion
      object ops extends TableCompanion

      val projections = {
        schema.grouped(2) map { subschema =>
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

    module.ops.constString(Set(CString("/test"))).load("", Schema.mkType(schema).get).flatMap(_.toJson).copoint.toStream must_== sample.data
  }
}

// vim: set ts=4 sw=4 et:
