package com.precog.yggdrasil
package table

import com.precog.common._
import com.precog.common.json._
import com.precog.util._
import com.precog.yggdrasil.util._

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
import scalaz.syntax.std.boolean._

import org.specs2.ScalaCheck
import org.specs2.mutable._

import org.scalacheck._
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._

import SampleData._
import CValueGenerators._


trait BlockLoadSpec[M[+_]] extends BlockStoreTestSupport[M] with Specification with ScalaCheck { self =>
  class BlockStoreLoadTestModule(sampleData: SampleData) extends BlockStoreTestModule[M] {
    val M = self.M
    val Some((idCount, schema)) = sampleData.schema
    val actualSchema = inferSchema(sampleData.data map { _ \ "value" })

    val projections = actualSchema.grouped(1) map { subschema =>
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
        sampleData.data flatMap { jv =>
          val back = subschema.foldLeft[JValue](JObject(JField("key", jv \ "key") :: Nil)) {
            case (obj, (jpath, ctype)) => { 
              val vpath = CPath(CPathField("value") :: jpath.nodes)
              val valueAtPath = jv.get(vpath)
              
              if (compliesWithSchema(valueAtPath, ctype)) {
                obj.set(vpath, valueAtPath)
              } else { 
                obj
              }
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

  def testLoadDense(sample: SampleData) = {
    val module = new BlockStoreLoadTestModule(sample)
    
    val expected = sample.data flatMap { jv =>
      val back = module.schema.foldLeft[JValue](JObject(JField("key", jv \ "key") :: Nil)) {
        case (obj, (jpath, ctype)) => { 
          val vpath = CPath(CPathField("value") :: jpath.nodes)
          val valueAtPath = jv.get(vpath)
          
          if (module.compliesWithSchema(valueAtPath, ctype)) {
            obj.set(vpath, valueAtPath)
          } else {
            obj
          }
        }
      }
      
      (back \ "value" != JNothing).option(back)
    }

    module.Table.constString(Set(CString("/test"))).load("", Schema.mkType(module.schema).get).flatMap(_.toJson).copoint.toStream must_== expected
  }

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
        (1 , List(CPath(".u") -> CBoolean, CPath(".md") -> CString, CPath(".l") -> CEmptyArray))
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
        (2, List(CPath(".fa") -> CNull, CPath(".hW") -> CLong, CPath(".rzp") -> CEmptyObject))
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
        (3, List(CPath(".f.bn[0]") -> CNull, 
                 CPath(".f.wei") -> CLong, 
                 CPath(".f.wei") -> CDouble, 
                 CPath(".ljz[0]") -> CNull,
                 CPath(".ljz[1][0]") -> CString,
                 CPath(".ljz[2]") -> CBoolean,
                 CPath(".jmy") -> CDouble))
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
        (2, List(CPath(".dV.d") -> CBoolean, 
                 CPath(".dV.l") -> CBoolean, 
                 CPath(".dV.vq") -> CEmptyObject, 
                 CPath(".oy.nm") -> CBoolean, 
                 CPath(".uR") -> CDouble))
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
      Some((1, List((CPath(".o8agyghfjxe") -> CEmptyArray), 
                    (CPath(".fg[0]") -> CBoolean), 
                    (CPath(".fg[1]") -> CNum), 
                    (CPath(".fg[1]") -> CLong), 
                    (CPath(".fg[2]") -> CNum), 
                    (CPath(".fg[2]") -> CLong), 
                    (CPath(".cfnYTg92dg") -> CString))))
    )

    testLoadDense(sampleData)
  }

}

// vim: set ts=4 sw=4 et:
