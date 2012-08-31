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
package leveldb

import org.scalacheck.{Arbitrary, Gen}
import Gen._
import Arbitrary.arbitrary
import org.scalacheck.Prop
import org.scalacheck.Test.Params
import org.specs2.ScalaCheck
import org.specs2.mutable.Specification


import com.precog.yggdrasil._
import com.precog.yggdrasil.actor._
import com.precog.common._
import com.precog.common.util._

import LevelDBByteProjectionSpec._

import blueeyes.json.JPath

import scalaz._


object LevelDBByteProjectionSpec {
  val cvLong = CLong(5)
  val cvString = CString("string")
  val cvBoolean = CBoolean(true)
  val cvDouble = CDouble(7)
  val cvDouble2 = CDouble(9)
  val cvNum = CNum(8)
  val cvEmptyArray = CEmptyArray
  val cvEmptyObject = CEmptyObject
  val cvNull = CNull

  val colDesStringArbitrary: ColumnDescriptor = ColumnDescriptor(Path("path6"), JPath("key6"), CString, Authorities(Set()))
  val colDesBoolean: ColumnDescriptor = ColumnDescriptor(Path("path0"), JPath("key0"), CBoolean, Authorities(Set()))
  val colDesLong: ColumnDescriptor = ColumnDescriptor(Path("path2"), JPath("key2"), CLong, Authorities(Set()))
  val colDesDouble: ColumnDescriptor = ColumnDescriptor(Path("path4"), JPath("key4"), CDouble, Authorities(Set()))
  val colDesDouble2: ColumnDescriptor = ColumnDescriptor(Path("path8"), JPath("key8"), CDouble, Authorities(Set()))  
  val colDesDecimal: ColumnDescriptor = ColumnDescriptor(Path("path7"), JPath("key7"), CNum, Authorities(Set()))

/*
  def byteProjectionInstance(indexedColumns: ListMap[ColumnDescriptor, Int], sorting: Seq[(ColumnDescriptor, SortBy)]) = { 
    ProjectionDescriptor(indexedColumns, sorting) map { d => 
      new LevelDBByteProjection {
        val descriptor: ProjectionDescriptor = d
      }
    }
  }

  def constructByteProjection(columns: ListMap[ColumnDescriptor, Int], sorting: Seq[(ColumnDescriptor, SortBy)]) = {
    byteProjectionInstance(columns, sorting) ||| { errorMessage => sys.error("problem constructing projection descriptor: " + errorMessage) }
  }
  
  def constructByteProjection2(desc: ProjectionDescriptor) = {
    byteProjectionInstance2(desc) ///||| { errorMessage => sys.error("problem constructing projection descriptor: " + errorMessage) }
  }
  */

  def byteProjectionInstance2(desc: ProjectionDescriptor) = {
    desc map { desc =>
      new LevelDBByteProjection {
        val descriptor: ProjectionDescriptor = desc
      }
    }
  }

  def constructIds(uniqueIndices: Int): Vector[Long] = {  
    Gen.listOfN(uniqueIndices, Arbitrary.arbitrary[Long]).sample.get.foldLeft(Vector.empty: Vector[Long])((v, id) => v :+ id)
  }

  def constructValues(listOfColDes: List[ColumnDescriptor]): Seq[CValue] = {
    val listOfTypes = listOfColDes.foldLeft(List.empty: List[CType])((l,col) => l :+ col.valueType)
    listOfTypes.foldLeft(Seq.empty: Seq[CValue]) {
      case (seq, CBoolean) => seq :+ cvBoolean
      case (seq, CDouble) => seq :+ cvDouble
      case (seq, CLong) => seq :+ cvLong
      case (seq, CNum) => seq :+ cvNum
      case (seq, CString) => seq :+ cvString
      case (seq, CEmptyArray) => seq :+ cvEmptyArray
      case (seq, CEmptyObject) => seq :+ cvEmptyObject
      case (seq, CNull) => seq :+ cvNull
      case _ => throw new MatchError("Unexpected type")
    }
  }
}

class LevelDBByteProjectionSpec extends Specification with ScalaCheck {
  "a byte projection generated from sample data" should {
    "return the arguments of project, when fromBytes is applied to project" in {
      val routingTable: RoutingTable = new SingleColumnProjectionRoutingTable
      val dataPath = Path("/test")
      implicit val (sampleData, _) = DistributedSampleSet.sample(5, 0)

      val projDes: List[ProjectionDescriptor] = {
        sampleData.zipWithIndex.foldLeft(List.empty[ProjectionDescriptor]) {
          case (acc, (jobj, i)) => routingTable.routeEvent(EventMessage(EventId(0, i), Event(dataPath, "", jobj, Map()))).foldLeft(acc) {
            case (acc, ProjectionData(descriptor, values, _)) =>
              acc :+ descriptor
          }
        }
      }

      val byteProj = projDes.foldLeft(List.empty[LevelDBByteProjection]) {
        case (l, des) => l :+ byteProjectionInstance2(des).sample.get
      }
      
      projDes.zip(byteProj).map { 
        case (des, byte) =>
          val identities: Vector[Long] = constructIds(des.identities)
          val values: Seq[CValue] = constructValues(des.columns)
          val (projectedIds, projectedValues) = byte.toBytes(VectorCase.fromSeq(identities), values)

          byte.fromBytes(projectedIds, projectedValues) must_== (identities, values)
      }
    }
  }

  "a byte projection of an empty array" should {
    "return the arguments of project, when fromBytes is applied to project" in {
      val routingTable: RoutingTable = new SingleColumnProjectionRoutingTable
      val dataPath = Path("/test")
      implicit val (sampleData, _) = DistributedSampleSet.sample(5, 0, AdSamples.emptyArraySample)

      val projDes: List[ProjectionDescriptor] = {
        sampleData.zipWithIndex.foldLeft(List.empty[ProjectionDescriptor]) {
          case (acc, (jobj, i)) => routingTable.routeEvent(EventMessage(EventId(0, i), Event(dataPath, "", jobj, Map()))).foldLeft(acc) {
            case (acc, ProjectionData(descriptor, values, _)) =>
              acc :+ descriptor
          }
        }
      }
      val byteProj = projDes.foldLeft(List.empty[LevelDBByteProjection]) {
        case (l, des) => l :+ byteProjectionInstance2(des).sample.get
      }
      
      projDes.zip(byteProj).map { 
        case (des, byte) =>
          val identities: Vector[Long] = constructIds(des.identities)
          val values: Seq[CValue] = constructValues(des.columns)
          val (projectedIds, projectedValues) = byte.toBytes(VectorCase.fromSeq(identities), values)

          byte.fromBytes(projectedIds, projectedValues) must_== (identities, values)
      }
    }.pendingUntilFixed
  }

  "a byte projection of an empty object" should {
    "return the arguments of project, when fromBytes is applied to project" in {
      val routingTable: RoutingTable = new SingleColumnProjectionRoutingTable
      val dataPath = Path("/test")
      implicit val (sampleData, _) = DistributedSampleSet.sample(5, 0, AdSamples.emptyObjectSample)

      val projDes: List[ProjectionDescriptor] = {
        sampleData.zipWithIndex.foldLeft(List.empty[ProjectionDescriptor]) {
          case (acc, (jobj, i)) => routingTable.routeEvent(EventMessage(EventId(0, i), Event(dataPath, "", jobj, Map()))).foldLeft(acc) {
            case (acc, ProjectionData(descriptor, values, _)) =>
              acc :+ descriptor
          }
        }
      }
      val byteProj = projDes.foldLeft(List.empty[LevelDBByteProjection]) {
        case (l, des) => l :+ byteProjectionInstance2(des).sample.get
      }
      
      projDes.zip(byteProj).map { 
        case (des, byte) =>
          val identities: Vector[Long] = constructIds(des.identities)
          val values: Seq[CValue] = constructValues(des.columns)
          val (projectedIds, projectedValues) = byte.toBytes(VectorCase.fromSeq(identities), values)

          byte.fromBytes(projectedIds, projectedValues) must_== (identities, values)
      }
    }.pendingUntilFixed
  }

  "a byte projection of null" should {
    "return the arguments of project, when fromBytes is applied to project" in {
      val routingTable: RoutingTable = new SingleColumnProjectionRoutingTable
      val dataPath = Path("/test")
      implicit val (sampleData, _) = DistributedSampleSet.sample(5, 0, AdSamples.nullSample)

      val projDes: List[ProjectionDescriptor] = {
        sampleData.zipWithIndex.foldLeft(List.empty[ProjectionDescriptor]) {
          case (acc, (jobj, i)) => routingTable.routeEvent(EventMessage(EventId(0, i), Event(dataPath, "", jobj, Map()))).foldLeft(acc) {
            case (acc, ProjectionData(descriptor, values, _)) =>
              acc :+ descriptor
          }
        }
      }
      val byteProj = projDes.foldLeft(List.empty[LevelDBByteProjection]) {
        case (l, des) => l :+ byteProjectionInstance2(des).sample.get
      }
      
      projDes.zip(byteProj).map { 
        case (des, byte) =>
          val identities: Vector[Long] = constructIds(des.identities)
          val values: Seq[CValue] = constructValues(des.columns)
          val (projectedIds, projectedValues) = byte.toBytes(VectorCase.fromSeq(identities), values)

          byte.fromBytes(projectedIds, projectedValues) must_== (identities, values)
      }
    }.pendingUntilFixed
  }

  /* 
  "a scalacheck-generated byte projection" should {
    "return the arguments of project, when fromBytes is applied to project" in {
          
      check { (byteProjection: LevelDBByteProjection) =>
        val identities: Vector[Long] = constructIds(byteProjection.descriptor.indexedColumns.values.toList.distinct.size)
        val values: Seq[CValue] = constructValues(byteProjection.descriptor.columns)

        val (projectedIds, projectedValues) = byteProjection.toBytes(identities, values)

        byteProjection.fromBytes(projectedIds, projectedValues) must_== (identities, values) 
      }.set(maxDiscarded -> 500, minTestsOk -> 200)
    }
  }

  "a byte projection" should {
    "project to the expected key format when a single id matches two sortings, ById and ByValue (test1)" in {
      val columns = List(colDesLong -> 0, colDesBoolean -> 0, colDesFloat -> 1)
      val sorting: Seq[(ColumnDescriptor, SortBy)] = Seq((colDesLong, ById),(colDesBoolean, ByValue),(colDesFloat, ByValueThenId))
      val byteProjection = byteProjectionInstance(columns, sorting) ||| { errorMessage => sys.error("problem constructing projection descriptor: " + errorMessage) }

      val expectedKey: Array[Byte] = Array(0,0,0,0,0,0,0,1,1,64,-64,0,0,0,0,0,0,0,0,0,2)
      val expectedValue: Array[Byte] = Array(0,0,0,0,0,0,0,5)

      val (key, value) = byteProjection.toBytes(VectorCase(1L,2L), Seq(cvLong, cvBoolean, cvFloat))
      key must_== expectedKey
      value must_== expectedValue
    }

    "project to the expected key format another when an id is only sorted ByValue (test2)" in {
      val columns: List[ColumnDescriptor, Int] = List(colDesInt -> 0, colDesDouble -> 1, colDesBoolean -> 1)
      val sorting: Seq[(ColumnDescriptor, SortBy)] = Seq((colDesInt, ByValue),(colDesDouble, ByValue),(colDesBoolean, ByValue))
      val byteProjection = byteProjectionInstance(columns, sorting) ||| { errorMessage => sys.error("problem constructing projection descriptor: " + errorMessage) }

      val expectedKey: Array[Byte] = Array(0,0,0,4,64,28,0,0,0,0,0,0,1,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,2)
      val expectedValue: Array[Byte] = Array()

      val (key, value) = byteProjection.toBytes(VectorCase(1L,2L), Seq(cvInt, cvDouble, cvBoolean))
      key must_== expectedKey
      value must_== expectedValue
    } 

    "project to the expected key format (case with five columns) (test3)" in {
      val columns: List[ColumnDescriptor, Int] = List(colDesInt -> 0, colDesLong -> 0, colDesFloat -> 1, colDesDouble -> 2, colDesBoolean -> 2)
      val sorting: Seq[(ColumnDescriptor, SortBy)] = Seq((colDesInt, ByValue),(colDesLong, ByValue), (colDesFloat, ById), (colDesDouble, ByValue), (colDesBoolean, ById)) 
      val byteProjection = byteProjectionInstance(columns, sorting) ||| { errorMessage => sys.error("problem constructing projection descriptor: " + errorMessage) }

      val expectedKey: Array[Byte] = Array(0,0,0,4,0,0,0,0,0,0,0,5,0,0,0,0,0,0,0,2,64,28,0,0,0,0,0,0,0,0,0,0,0,0,0,3,0,0,0,0,0,0,0,1)
      val expectedValue: Array[Byte] = Array(64,-64,0,0,1)

      val (key, value) = byteProjection.toBytes(VectorCase(1L,2L,3L), Seq(cvInt, cvLong, cvFloat, cvDouble, cvBoolean))
      key must_== expectedKey
      value must_== expectedValue
    }
  }

  "when applied to the project function, the fromBytes function" should {
    "return the arguments of the project function (test1)" in {
      val columns: List[ColumnDescriptor, Int] = List(colDesLong -> 0, colDesBoolean -> 0, colDesFloat -> 1)
      val sorting: Seq[(ColumnDescriptor, SortBy)] = Seq((colDesLong, ById),(colDesBoolean, ByValue),(colDesFloat, ByValueThenId))
      val byteProjection = byteProjectionInstance(columns, sorting) ||| { errorMessage => sys.error("problem constructing projection descriptor: " + errorMessage) }

      val (projectedIds, projectedValues) = byteProjection.toBytes(VectorCase(1L,2L), Seq(cvLong, cvBoolean, cvFloat))

      byteProjection.fromBytes(projectedIds, projectedValues) must_== (VectorCase(1L,2L), Seq(cvLong, cvBoolean, cvFloat)) 
    }

    "return the arguments of the project function (test2)" in {
      val columns: List[ColumnDescriptor, Int] = List(colDesInt -> 0, colDesDouble -> 1, colDesBoolean -> 1)
      val sorting: Seq[(ColumnDescriptor, SortBy)] = Seq((colDesInt, ByValue),(colDesDouble, ByValue),(colDesBoolean, ByValue))
      val byteProjection = byteProjectionInstance(columns, sorting) ||| { errorMessage => sys.error("problem constructing projection descriptor: " + errorMessage) }

      val (projectedIds, projectedValues) = byteProjection.toBytes(VectorCase(1L,2L), Seq(cvInt, cvDouble, cvBoolean))

      byteProjection.fromBytes(projectedIds, projectedValues) must_== (VectorCase(1L,2L), Seq(cvInt, cvDouble, cvBoolean)) 
    }

    "return the arguments of the project function (test3)" in {
      val columns: List[ColumnDescriptor, Int] = List(colDesFloat -> 0, colDesInt -> 1, colDesDouble -> 1, colDesBoolean -> 2, colDesDouble2 -> 2)
      val sorting: Seq[(ColumnDescriptor, SortBy)] = Seq((colDesFloat, ById),(colDesInt, ByValue), (colDesDouble, ById), (colDesBoolean, ByValue), (colDesDouble2, ById)) 
      val byteProjection = byteProjectionInstance(columns, sorting) ||| { errorMessage => sys.error("problem constructing projection descriptor: " + errorMessage) }
 
      val (projectedIds, projectedValues) = byteProjection.toBytes(VectorCase(1L,2L,3L), Seq(cvFloat, cvInt, cvDouble, cvBoolean, cvDouble2))

      byteProjection.fromBytes(projectedIds, projectedValues) must_== (VectorCase(1L,2L,3L), Seq(cvFloat, cvInt, cvDouble, cvBoolean, cvDouble2)) 
    }
  }
  */

}







