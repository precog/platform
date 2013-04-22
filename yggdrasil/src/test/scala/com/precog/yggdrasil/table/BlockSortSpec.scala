package com.precog.yggdrasil
package table


import com.precog.bytecode._
import com.precog.common._
import com.precog.util._

import blueeyes.json._

import com.weiglewilczek.slf4s.Logging

import scala.annotation.tailrec
import scala.util.Random
import scalaz._
import scalaz.effect._
import scalaz.std.anyVal._
import scalaz.std.list._
import scalaz.syntax.comonad._
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

import PrecogJValueOrder._

trait BlockSortSpec[M[+_]] extends BlockStoreTestSupport[M] with Specification with ScalaCheck { self =>
  def testSortDense(sample: SampleData, sortOrder: DesiredSortOrder, unique: Boolean, sortKeys: JPath*) = {
    val module = BlockStoreTestModule.empty[M]

    val jvalueOrdering = Order[JValue].toScalaOrdering
    val desiredJValueOrder = if (sortOrder.isAscending) jvalueOrdering else jvalueOrdering.reverse

    val globalIdPath = JPath(".globalId")

    val original = if (unique) {
      sample.data.map { jv => JArray(sortKeys.map(_.extract(jv \ "value")).toList) -> jv }.toMap.toList.unzip._2.toStream
    } else {
      sample.data
    }

    // We have to add in and then later remove the global Id (insert
    // order) to match real sort semantics for disambiguation of equal
    // values
    val sorted = original.zipWithIndex.map {
      case (jv, i) => JValue.unsafeInsert(jv, globalIdPath, JNum(i))
    }.sortBy { v =>
      JArray(sortKeys.map(_.extract(v \ "value")).toList ::: List(v \ "globalId")).asInstanceOf[JValue]
    }(desiredJValueOrder).map(_.delete(globalIdPath).get).toList

    //def xyz(v: JValue): JValue = {
    //  JArray(sortKeys.map(_.extract(v \ "value")).toList ::: List(v \ "globalId"))
    //}

    val cSortKeys = sortKeys map { CPath(_) }

    val resultM = for {
      sorted <- module.fromSample(sample).sort(module.sortTransspec(cSortKeys: _*), sortOrder)
      json <- sorted.toJson
    } yield (json, sorted)

    val (result, resultTable) = resultM.copoint

    result.toList must_== sorted

    resultTable.size mustEqual ExactSize(sorted.size)
  }

  def checkSortDense(sortOrder: DesiredSortOrder) = {
    import TableModule.paths.Value

    implicit val gen = sample(objectSchema(_, 3))
    check { (sample: SampleData) => {
      val Some((_, schema)) = sample.schema

      testSortDense(sample, sortOrder, false, schema.map(_._1).head)
    }}
  }

  // Simple test of sorting on homogeneous data
  def homogeneousSortSample = {
    val sampleData = SampleData(
      (JParser.parseUnsafe("""[
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

    testSortDense(sampleData, SortDescending, false, JPath(".uid"))
  }

  // Simple test of sorting on homogeneous data with objects
  def homogeneousSortSampleWithNonexistentSortKey = {
    val sampleData = SampleData(
      (JParser.parseUnsafe("""[
        {"key":[2],"value":6},
        {"key":[1],"value":5}  
      ]""") --> classOf[JArray]).elements.toStream,
      Some(
        (1 , List(JPath(".") -> CString))
      )
    )

    testSortDense(sampleData, SortDescending, false, JPath(".uid"))
  }

  // Simple test of partially undefined sort key data
  def partiallyUndefinedSortSample = {
    val sampleData = SampleData(
      (JParser.parseUnsafe("""[
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
        (1, List(JPath(".uid") -> CString, JPath(".fa") -> CNull, JPath(".hW") -> CDouble, JPath(".rzp") -> CEmptyObject))
      )
    )

    testSortDense(sampleData, SortAscending, false, JPath(".uid"), JPath(".hW"))
  }

  def heterogeneousBaseValueTypeSample = {
    val sampleData = SampleData(
      (JParser.parseUnsafe("""[
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
        (1, List(JPath("[0]") -> CLong, JPath("[1]") -> CLong, JPath(".uid") -> CString, JPath("abc") -> CLong))
      )
    )

    testSortDense(sampleData, SortAscending, false, JPath(".uid"))
  }

  def badSchemaSortSample = {
    val sampleData = SampleData(
      (JParser.parseUnsafe("""[
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
    testSortDense(sampleData, SortAscending, false, JPath("q"))
  }

  // Simple test of sorting on heterogeneous data
  def heterogeneousSortSample2 = {
    val sampleData = SampleData(
      (JParser.parseUnsafe("""[
        {"key":[1,4,3],"value":{"b0":["",{"alxk":-1},-5.170005125478374E+307],"y":{"pvbT":[-1458654748381439976,{}]}}},
        {"key":[1,4,4],"value":{"y":false,"qvd":[],"aden":{}}},
        {"key":[3,3,3],"value":{"b0":["gxy",{"alxk":-1},6.614267528783459E+307],"y":{"pvbT":[1,{}]}}}
      ]""") --> classOf[JArray]).elements.toStream,
      None)

    testSortDense(sampleData, SortDescending, false, JPath(".y"))
  }

  // Simple test of sorting on heterogeneous data
  def heterogeneousSortSampleDescending = {
    val sampleData = SampleData(
      (JParser.parseUnsafe("""[
        {"key":[2],"value":{"y":false}},
        {"key":[3],"value":{"y":{"pvbT":1}}}
      ]""") --> classOf[JArray]).elements.toStream,
      None)

    testSortDense(sampleData, SortDescending, false, JPath(".y"))
  }

  // Simple test of sorting on heterogeneous data
  def heterogeneousSortSampleAscending = {
    val sampleData = SampleData(
      (JParser.parseUnsafe("""[
        {"key":[2],"value":{"y":false}},
        {"key":[3],"value":{"y":{"pvbT":1}}}
      ]""") --> classOf[JArray]).elements.toStream,
      None)

    testSortDense(sampleData, SortAscending, false, JPath(".y"))
  }

  // Simple test of heterogeneous sort keys
  def heterogeneousSortSample = {
    val sampleData = SampleData(
      (JParser.parseUnsafe("""[
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

    testSortDense(sampleData, SortAscending, false, JPath(".uid"))
  }

  def secondHetSortSample = {
    val sampleData = SampleData(
      (JParser.parseUnsafe("""[
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

    testSortDense(sampleData, SortAscending, false, JPath(".zw1"))
  }

  /* The following data set results in three separate JDBM
   * indices due to formats. This exposed a bug in mergeProjections
   * where we weren't properly inverting the cell matrix reorder
   * once one of the index slices expired. See commit
   * a253d47f3f6d09fd39afc2986c529e84e5443e7f for details
   */
  def threeCellMerge = {
    val sampleData = SampleData(
      (JParser.parseUnsafe("""[
        {
          "value":-2355162409801206381,
          "key":[1.0,1.0,11.0]
        }, {
          "value":416748368221569769,
          "key":[12.0,10.0,5.0]
        }, {
          "value":1,
          "key":[9.0,13.0,2.0]
        }, {
          "value":4220813543874929309,
          "key":[13.0,10.0,11.0]
        }, {
          "value":{
            "viip":8.988465674311579E+307,
            "ohvhwN":-1.911181119089705905E+11774,
            "zbtQhnpnun":-4364598680493823671
          },
          "key":[8.0,12.0,6.0]
        }, {
          "value":{
            "viip":-8.610170336058498E+307,
            "ohvhwN":0.0,
            "zbtQhnpnun":-3072439692643750408
          },
          "key":[3.0,1.0,12.0]
        }, {
          "value":{
            "viip":1.0,
            "ohvhwN":1.255850949484045134E-25873,
            "zbtQhnpnun":-2192537798839555684
          },
          "key":[12.0,10.0,4.0]
        }, {
          "value":{
            "viip":-1.0,
            "ohvhwN":1E-18888,
            "zbtQhnpnun":-1
          },
          "key":[2.0,4.0,11.0]
        }, {
          "value":{
            "viip":1.955487389945603E+307,
            "ohvhwN":-2.220603033978414186E+19,
            "zbtQhnpnun":-1
          },
          "key":[6.0,11.0,5.0]
        }, {
          "value":{
            "viip":-4.022335964233546E+307,
            "ohvhwN":0E+1,
            "zbtQhnpnun":-1
          },
          "key":[8.0,7.0,13.0]
        }, {
          "value":{
            "viip":1.0,
            "ohvhwN":-4.611686018427387904E+50018,
            "zbtQhnpnun":0
          },
          "key":[1.0,13.0,12.0]
        }, {
          "value":{
            "viip":0.0,
            "ohvhwN":4.611686018427387903E+26350,
            "zbtQhnpnun":0
          },
          "key":[2.0,7.0,7.0]
        }, {
          "value":{
            "viip":-6.043665565176412E+307,
            "ohvhwN":-4.611686018427387904E+27769,
            "zbtQhnpnun":0
          },
          "key":[2.0,11.0,6.0]
        }, {
          "value":{
            "viip":-1.0,
            "ohvhwN":-1E+36684,
            "zbtQhnpnun":0
          },
          "key":[6.0,4.0,8.0]
        }, {
          "value":{
            "viip":-1.105552122908816E+307,
            "ohvhwN":6.78980055408249814E-41821,
            "zbtQhnpnun":1
          },
          "key":[13.0,6.0,11.0]
        }, {
          "value":{
            "viip":1.0,
            "ohvhwN":3.514965842146513368E-43185,
            "zbtQhnpnun":1133522166006977485
          },
          "key":[13.0,11.0,13.0]
        }, {
          "value":{
            "viip":8.988465674311579E+307,
            "ohvhwN":2.129060503704072469E+45099,
            "zbtQhnpnun":1232928328066014683
          },
          "key":[11.0,3.0,6.0]
        }, {
          "value":{
            "viip":6.651090528711015E+307,
            "ohvhwN":-1.177821034245149979E-49982,
            "zbtQhnpnun":2406980638624125853
          },
          "key":[4.0,5.0,7.0]
        }, {
          "value":{
            "viip":4.648002254349813E+307,
            "ohvhwN":4.611686018427387903E-42682,
            "zbtQhnpnun":2658995085512919727
          },
          "key":[12.0,2.0,8.0]
        }, {
          "value":{
            "viip":0.0,
            "ohvhwN":4.611686018427387903E-33300,
            "zbtQhnpnun":3464601040437655780
          },
          "key":[8.0,10.0,4.0]
        }, {
          "value":{
            "viip":-8.988465674311579E+307,
            "ohvhwN":1E-42830,
            "zbtQhnpnun":3709226396529427859
          },
          "key":[10.0,1.0,4.0]
        }
      ]""") --> classOf[JArray]).elements.toStream,
      Some(
        (3, List(JPath(".zbtQhnpnun") -> CLong,
                 JPath(".ohvhwN") -> CNum,
                 JPath(".viip") -> CNum))
      )
    )

    testSortDense(sampleData, SortAscending, false, JPath(".zbtQhnpnun"))
  }

  def uniqueSort = {
    val sampleData = SampleData(
      (JParser.parseUnsafe("""[
        { "key" : [2], "value" : { "foo" : 10 } },
        { "key" : [1], "value" : { "foo" : 10 } }
       ]""") --> classOf[JArray]).elements.toStream,
      Some(
        (1 , List())
      )
    )

    testSortDense(sampleData, SortAscending, false, JPath(".foo"))
  }  

  def emptySort = {
    val sampleData = SampleData(
      (JParser.parseUnsafe("""[]""") --> classOf[JArray]).elements.toStream,
      Some(
        (1 , List())
      )
    )

    testSortDense(sampleData, SortAscending, false, JPath(".foo"))
  }

}
// vim: set ts=4 sw=4 et:
