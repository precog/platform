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
import org.specs2.ScalaCheck
import org.specs2.mutable.Specification

import scala.collection.immutable.ListMap

import com.precog.yggdrasil._
import com.precog.common._
import com.precog.analytics.Path

import LevelDBByteProjectionSpec._

import blueeyes.json.JPath

import scalaz._


object LevelDBByteProjectionSpec {
  val cvInt = CInt(4)
  val cvLong = CLong(5)
  val cvString = CString("string")
  val cvBoolean = CBoolean(true)
  val cvFloat = CFloat(6)
  val cvDouble = CDouble(7)
  val cvNum = CNum(8)

  val colDesStringFixed: ColumnDescriptor = ColumnDescriptor(Path("path5"), JPath("key5"), SStringFixed(1), Ownership(Set()))
  val colDesStringArbitrary: ColumnDescriptor = ColumnDescriptor(Path("path6"), JPath("key6"), SStringArbitrary, Ownership(Set()))
  val colDesBoolean: ColumnDescriptor = ColumnDescriptor(Path("path0"), JPath("key0"), SBoolean, Ownership(Set()))
  val colDesInt: ColumnDescriptor = ColumnDescriptor(Path("path1"), JPath("key1"), SInt, Ownership(Set()))
  val colDesLong: ColumnDescriptor = ColumnDescriptor(Path("path2"), JPath("key2"), SLong, Ownership(Set()))
  val colDesFloat: ColumnDescriptor = ColumnDescriptor(Path("path3"), JPath("key3"), SFloat, Ownership(Set()))
  val colDesDouble: ColumnDescriptor = ColumnDescriptor(Path("path4"), JPath("key4"), SDouble, Ownership(Set()))
  val colDesDecimal: ColumnDescriptor = ColumnDescriptor(Path("path7"), JPath("key7"), SDecimalArbitrary, Ownership(Set()))

  def byteProjectionInstance(columns: ListMap[ColumnDescriptor, Int], sorting: Seq[(ColumnDescriptor, SortBy)]) = { 
    ProjectionDescriptor(columns, sorting) map { d => 
      new LevelDBByteProjection {
        val descriptor: ProjectionDescriptor = d
      }
    }
  }

  def identityFunction(identities: Identities, values: Seq[CValue]): (Identities, Seq[CValue]) = (identities, values)


  //ef constructEverything = {
  //  def constructColumnDescriptor: Gen[ColumnDescriptor] = {
  //    def genPath: Gen[Path] = oneOf(Seq(Path("path1")))
  //    def genJPath: Gen[JPath] = oneOf(Seq(JPath("path1"),JPath("path2"),JPath("path3")))
  //    def genColumnType: Gen[ColumnType] = oneOf(Seq(SInt,SFloat))
  //    def genOwnership: Gen[Ownership] = oneOf(Seq(Ownership(Set())))
  //  
  //    val genColumnDescriptor = for {
  //      path      <- genPath
  //      selector  <- genJPath
  //      valueType <- genColumnType
  //      ownership <- genOwnership
  //    } yield ColumnDescriptor(path, selector, valueType, ownership)
  //  
  //    genColumnDescriptor
  //  }
  //      
  //  val setOfColDes = Gen.listOfN(3, constructColumnDescriptor).sample.get.toSet
  //  
  //  val listOfIndices: List[Int] = Gen.listOfN(setOfColDes.size, oneOf(0.to(setOfColDes.size / 3))).sample.get.sorted
  //  val uniqueIndices: Int = listOfIndices.toSet.size

  //  val columns = setOfColDes.zip(listOfIndices).foldLeft(ListMap.empty: ListMap[ColumnDescriptor, Int]) {
  //    case (lm, (colDes, index)) => lm + ((colDes, index))
  //  }

  //  def genSortBy: Gen[SortBy] = oneOf(Seq(ByValue, ById, ByValueThenId))
  //  val sorting = setOfColDes.foldLeft(Seq.empty: Seq[(ColumnDescriptor, SortBy)])((seq, colDes) => seq :+ ((colDes, genSortBy.sample.get)))
  //  
  //  println(columns)
  //  println(sorting)
  //  println(setOfColDes)

  //  val listOfTypes = setOfColDes.foldLeft(List.empty: List[ColumnType])((l,col) => l :+ col.valueType)
  //  val x = listOfTypes.foldLeft(Seq.empty: Seq[String]) {
  //    case (seq, SInt)  => seq :+ "SInt"
  //    case (seq, SFloat) => seq :+ "SFloat"
  //  }

  //  println(x)


  //  (uniqueIndices, columns, sorting, setOfColDes)
  //}  
  
    
  def constructByteProjection(columns: ListMap[ColumnDescriptor, Int], sorting: Seq[(ColumnDescriptor, SortBy)]) = {
    byteProjectionInstance(columns, sorting) ||| { errorMessage => sys.error("problem constructing projection descriptor: " + errorMessage) }
  }

  def constructIds(uniqueIndices: Int): Vector[Long] = {  
    Gen.listOfN(uniqueIndices, Arbitrary.arbitrary[Long]).sample.get.foldLeft(Vector.empty: Vector[Long])((v, id) => v :+ id)
  }

  def constructValues(setOfColDes: Set[ColumnDescriptor]): Seq[CValue] = {
    val listOfTypes = setOfColDes.foldLeft(List.empty: List[ColumnType])((l,col) => l :+ col.valueType)
    listOfTypes.foldLeft(Seq.empty: Seq[CValue]) {
      case (seq, SInt)  => seq :+ cvInt
      case (seq, SFloat) => seq :+ cvFloat
    }
  }
}

class LevelDBByteProjectionSpec extends Specification with ScalaCheck {
  "a scalacheck generated byte projection" should {
    "pass preliminary tests" in {
      implicit lazy val arbColumnsSorting: Arbitrary[(Set[ColumnDescriptor], Int, ListMap[ColumnDescriptor,Int], Seq[(ColumnDescriptor,SortBy)])] = {
        def constructColumnDescriptor: Gen[ColumnDescriptor] = {
          def genPath: Gen[Path] = Gen.oneOf(Seq(Path("path1")))
          def genJPath: Gen[JPath] = Gen.oneOf(Seq(JPath("path1"),JPath("path2"),JPath("path3")))
          def genColumnType: Gen[ColumnType] = Gen.oneOf(Seq(SInt,SFloat))
          def genOwnership: Gen[Ownership] = Gen.oneOf(Seq(Ownership(Set())))
    
          val genColumnDescriptor = for {
            path      <- genPath
            selector  <- genJPath
            valueType <- genColumnType
            ownership <- genOwnership
          } yield ColumnDescriptor(path, selector, valueType, ownership)
    
          genColumnDescriptor
        }
        val setOfColDes: Set[ColumnDescriptor] = Gen.listOfN(3, constructColumnDescriptor).sample.get.toSet
        
        val listOfIndices: List[Int] = Gen.listOfN(setOfColDes.size, Gen.oneOf(0.to(setOfColDes.size / 3))).sample.get.sorted
        val uniqueIndices: Int = listOfIndices.toSet.size

        val columns = setOfColDes.zip(listOfIndices).foldLeft(ListMap.empty: ListMap[ColumnDescriptor, Int]) { 
          case (lm, (colDes, index)) => lm + ((colDes, index))
        }

        def genSortBy: Gen[SortBy] = Gen.oneOf(Seq(ByValue, ById, ByValueThenId))
        val sorting = setOfColDes.foldLeft(Seq.empty: Seq[(ColumnDescriptor, SortBy)])((seq, colDes) => seq :+ ((colDes, genSortBy.sample.get)))

        Arbitrary(setOfColDes, uniqueIndices, columns, sorting)
      }
      
        check { (x: (Set[ColumnDescriptor], Int, ListMap[ColumnDescriptor,Int], Seq[(ColumnDescriptor,SortBy)])) =>
          val colDes = ColumnDescriptor(Path(""),JPath(""),SInt,Ownership(Set()))
          val newByteProjection: Option[LevelDBByteProjection] = {
            try {
              Some(constructByteProjection(x._3, x._4))
            } catch {
              case e => None
            }
          }
          if (!(newByteProjection == None)) {
            val newIdentities: Vector[Long] = constructIds(x._2)
            val newValues: Seq[CValue] = constructValues(x._1)
            newByteProjection.get.unproject(newByteProjection.get.project(newIdentities, newValues)._1, newByteProjection.get.project(newIdentities, newValues)._2)(identityFunction) must_== (newIdentities, newValues)
          }
          else {
            val byteProjection: LevelDBByteProjection = constructByteProjection(ListMap(colDes -> 0), Seq((colDes, ByValue)))
            val newIdentities: Vector[Long] = Vector(1L)
            val newValues: Seq[CValue] = Seq(CInt(2))
            byteProjection.unproject(byteProjection.project(newIdentities, newValues)._1, byteProjection.project(newIdentities, newValues)._2)(identityFunction) must_== (newIdentities, newValues)
          } 
        }
    }
  }
  

  //"a generated byte projection" should {
  //  "unproject of project" in {
  //    val (uniqueIndices, columns, sorting, setOfColDes) = constructEverything
  //    val newByteProjection: LevelDBByteProjection = constructByteProjection(columns, sorting)
  //    val newIdentities: Vector[Long] = constructIds(uniqueIndices)
  //    val newValues: Seq[CValue] = constructValues(setOfColDes)
  //    newByteProjection.unproject(newByteProjection.project(newIdentities, newValues)._1, newByteProjection.project(newIdentities, newValues)._2)(identityFunction) must_== (newIdentities, newValues)
  //  }
  //}

  "a byte projection" should {
    "project to the expected key format when a single id matches two sortings, ById and ByValue (test1)" in {
      val columns: ListMap[ColumnDescriptor, Int] = ListMap(colDesLong -> 0, colDesBoolean -> 0, colDesFloat -> 1)
      val sorting: Seq[(ColumnDescriptor, SortBy)] = Seq((colDesLong, ById),(colDesBoolean, ByValue),(colDesFloat, ByValueThenId))
      val byteProjection = byteProjectionInstance(columns, sorting) ||| { errorMessage => sys.error("problem constructing projection descriptor: " + errorMessage) }

      val expectedKey: Array[Byte] = Array(0,0,0,0,0,0,0,1,1,64,-64,0,0,0,0,0,0,0,0,0,2)
      val expectedValue: Array[Byte] = Array(0,0,0,0,0,0,0,5)
      byteProjection.project(Vector(1L,2L), Seq(cvLong, cvBoolean, cvFloat))._1 must_== expectedKey
      byteProjection.project(Vector(1L,2L), Seq(cvLong, cvBoolean, cvFloat))._2 must_== expectedValue

    }
    "project to the expected key format another when an id is only sorted ByValue (test2)" in {
      val columns: ListMap[ColumnDescriptor, Int] = ListMap(colDesInt -> 0, colDesDouble -> 1, colDesBoolean -> 1)
      val sorting: Seq[(ColumnDescriptor, SortBy)] = Seq((colDesInt, ByValue),(colDesDouble, ByValueThenId),(colDesBoolean, ByValue))
      val byteProjection = byteProjectionInstance(columns, sorting) ||| { errorMessage => sys.error("problem constructing projection descriptor: " + errorMessage) }

      val expectedKey: Array[Byte] = Array(0,0,0,4,64,28,0,0,0,0,0,0,0,0,0,0,0,0,0,2,1,0,0,0,0,0,0,0,1)
      val expectedValue: Array[Byte] = Array()
      byteProjection.project(Vector(1L,2L), Seq(cvInt, cvDouble, cvBoolean))._1 must_== expectedKey
      byteProjection.project(Vector(1L,2L), Seq(cvInt, cvDouble, cvBoolean))._2 must_== expectedValue
    } 

    "project to the expected key format (case with five columns) (test3)" in {
      val columns: ListMap[ColumnDescriptor, Int] = ListMap(colDesInt -> 0, colDesLong -> 0, colDesFloat -> 1, colDesDouble -> 2, colDesBoolean -> 2)
      val sorting: Seq[(ColumnDescriptor, SortBy)] = Seq((colDesInt, ByValue),(colDesLong, ByValue), (colDesFloat, ById), (colDesDouble, ByValue), (colDesBoolean, ById)) 
      val byteProjection = byteProjectionInstance(columns, sorting) ||| { errorMessage => sys.error("problem constructing projection descriptor: " + errorMessage) }

      val expectedKey: Array[Byte] = Array(0,0,0,4,0,0,0,0,0,0,0,5,0,0,0,0,0,0,0,2,64,28,0,0,0,0,0,0,0,0,0,0,0,0,0,3,0,0,0,0,0,0,0,1)
      val expectedValue: Array[Byte] = Array(64,-64,0,0,1)
      byteProjection.project(Vector(1L,2L,3L), Seq(cvInt, cvLong, cvFloat, cvDouble, cvBoolean))._1 must_== expectedKey
      byteProjection.project(Vector(1L,2L,3L), Seq(cvInt, cvLong, cvFloat, cvDouble, cvBoolean))._2 must_== expectedValue
    }
  }

  "when applied to the project function, the unproject function" should {
    "return the arguments of the project function (test1)" in {
      val columns: ListMap[ColumnDescriptor, Int] = ListMap(colDesLong -> 0, colDesBoolean -> 0, colDesFloat -> 1)
      val sorting: Seq[(ColumnDescriptor, SortBy)] = Seq((colDesLong, ById),(colDesBoolean, ByValue),(colDesFloat, ByValueThenId))
      val byteProjection = byteProjectionInstance(columns, sorting) ||| { errorMessage => sys.error("problem constructing projection descriptor: " + errorMessage) }

      byteProjection.unproject(byteProjection.project(Vector(1L,2L), Seq(cvLong, cvBoolean, cvFloat))._1, byteProjection.project(Vector(1L,2L), Seq(cvLong, cvBoolean, cvFloat))._2)(identityFunction) must_== (Vector(1L,2L), Seq(cvLong, cvBoolean, cvFloat))
    }

    "return the arguments of the project function (test2)" in {
      val columns: ListMap[ColumnDescriptor, Int] = ListMap(colDesInt -> 0, colDesDouble -> 1, colDesBoolean -> 1)
      val sorting: Seq[(ColumnDescriptor, SortBy)] = Seq((colDesInt, ByValue),(colDesDouble, ByValueThenId),(colDesBoolean, ByValue))
      val byteProjection = byteProjectionInstance(columns, sorting) ||| { errorMessage => sys.error("problem constructing projection descriptor: " + errorMessage) }

      byteProjection.unproject(byteProjection.project(Vector(1L,2L), Seq(cvInt, cvDouble, cvBoolean))._1, byteProjection.project(Vector(1L,2L), Seq(cvInt, cvDouble, cvBoolean))._2)(identityFunction) must_== (Vector(1L,2L), Seq(cvInt, cvDouble, cvBoolean))
    }

    "return the arguments of the project function (test3)" in {
      val columns: ListMap[ColumnDescriptor, Int] = ListMap(colDesInt -> 0, colDesLong -> 0, colDesFloat -> 1, colDesDouble -> 2, colDesBoolean -> 2)
      val sorting: Seq[(ColumnDescriptor, SortBy)] = Seq((colDesInt, ByValue),(colDesLong, ByValue), (colDesFloat, ById), (colDesDouble, ByValue), (colDesBoolean, ById)) 
      val byteProjection = byteProjectionInstance(columns, sorting) ||| { errorMessage => sys.error("problem constructing projection descriptor: " + errorMessage) }
 
      byteProjection.unproject(byteProjection.project(Vector(1L,2L,3L), Seq(cvInt, cvLong, cvFloat, cvDouble, cvBoolean))._1, byteProjection.project(Vector(1L,2L,3L), Seq(cvInt, cvLong, cvFloat, cvDouble, cvBoolean))._2)(identityFunction) must_== (Vector(1L,2L,3L), Seq(cvInt, cvLong, cvFloat, cvDouble, cvBoolean))
    }

    
  }

}







