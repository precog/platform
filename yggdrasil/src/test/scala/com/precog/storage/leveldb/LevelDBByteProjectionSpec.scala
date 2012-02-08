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
  val cvDouble2 = CDouble(9)
  val cvNum = CNum(8)

  val colDesStringFixed: ColumnDescriptor = ColumnDescriptor(Path("path5"), JPath("key5"), SStringFixed(1), Ownership(Set()))
  val colDesStringArbitrary: ColumnDescriptor = ColumnDescriptor(Path("path6"), JPath("key6"), SStringArbitrary, Ownership(Set()))
  val colDesBoolean: ColumnDescriptor = ColumnDescriptor(Path("path0"), JPath("key0"), SBoolean, Ownership(Set()))
  val colDesInt: ColumnDescriptor = ColumnDescriptor(Path("path1"), JPath("key1"), SInt, Ownership(Set()))
  val colDesLong: ColumnDescriptor = ColumnDescriptor(Path("path2"), JPath("key2"), SLong, Ownership(Set()))
  val colDesFloat: ColumnDescriptor = ColumnDescriptor(Path("path3"), JPath("key3"), SFloat, Ownership(Set()))
  val colDesDouble: ColumnDescriptor = ColumnDescriptor(Path("path4"), JPath("key4"), SDouble, Ownership(Set()))
  val colDesDouble2: ColumnDescriptor = ColumnDescriptor(Path("path8"), JPath("key8"), SDouble, Ownership(Set()))  
  val colDesDecimal: ColumnDescriptor = ColumnDescriptor(Path("path7"), JPath("key7"), SDecimalArbitrary, Ownership(Set()))

  def byteProjectionInstance(columns: ListMap[ColumnDescriptor, Int], sorting: Seq[(ColumnDescriptor, SortBy)]) = { 
    ProjectionDescriptor(columns, sorting) map { d => 
      new LevelDBByteProjection {
        val descriptor: ProjectionDescriptor = d
      }
    }
  }

  def identityFunction(identities: Identities, values: Seq[CValue]): (Identities, Seq[CValue]) = (identities, values)

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
      case (seq, SBoolean) => seq :+ cvBoolean
      case (seq, SDouble) => seq :+ cvDouble
      case (seq, SLong) => seq :+ cvLong
    }
  }
}

class LevelDBByteProjectionSpec extends Specification with ScalaCheck {
  //"a scalacheck-generated byte projection" should {
  //  "return the arguments of project, when unproject is applied to project" in {
  //    implicit lazy val arbColumns: Arbitrary[List[ColumnDescriptor]] = {
  //      def constructColumnDescriptor: Gen[ColumnDescriptor] = {
  //        def genPath: Gen[Path] = Gen.oneOf(Seq(Path("path1"),Path("path2"),Path("path3")))
  //        def genJPath: Gen[JPath] = Gen.oneOf(Seq(JPath("jpath1"),JPath("jpath2"),JPath("jpath3")))
  //        def genColumnType: Gen[ColumnType] = Gen.oneOf(Seq(SInt,SFloat,SBoolean,SDouble,SLong))
  //        def genOwnership: Gen[Ownership] = Gen.oneOf(Seq(Ownership(Set())))
  //  
  //        val genColumnDescriptor = for {
  //          path      <- genPath
  //          selector  <- genJPath
  //          valueType <- genColumnType
  //          ownership <- genOwnership
  //        } yield ColumnDescriptor(path, selector, valueType, ownership)
  //  
  //        genColumnDescriptor
  //      }
  //      val genListColDes: Gen[List[ColumnDescriptor]] = Gen.listOfN(6, constructColumnDescriptor)
  //      
  //      Arbitrary(genListColDes)
  //    }
  //    
  //      check { (x: List[ColumnDescriptor]) =>
  //        val setOfColDes = x.toSet

  //        val listOfIndices: List[Int] = Gen.listOfN(setOfColDes.size, Gen.oneOf(0.to(setOfColDes.size / 3))).sample.get.sorted
  //        val uniqueIndices: Int = listOfIndices.toSet.size

  //        val columns = setOfColDes.zip(listOfIndices).foldLeft(ListMap.empty: ListMap[ColumnDescriptor, Int]) { 
  //          case (lm, (colDes, index)) => lm + ((colDes, index))
  //        }

  //        def genSortBy: Gen[SortBy] = Gen.oneOf(Seq(ByValue, ById, ByValueThenId))
  //        val sorting = setOfColDes.foldLeft(Seq.empty: Seq[(ColumnDescriptor, SortBy)])((seq, colDes) => seq :+ ((colDes, genSortBy.sample.get)))


  //        val newByteProjection: Option[LevelDBByteProjection] = {
  //          try {
  //            Some(constructByteProjection(columns, sorting))
  //          } catch {
  //            case e => None
  //          }
  //        }
  //        if (!(newByteProjection == None)) {
  //          print("!")
  //          val newIdentities: Vector[Long] = constructIds(uniqueIndices)
  //          val newValues: Seq[CValue] = constructValues(setOfColDes)
  //          print(newIdentities, newValues)
  //          if (!(newByteProjection.get.unproject(newByteProjection.get.project(newIdentities, newValues)._1, newByteProjection.get.project(newIdentities, newValues)._2)(identityFunction) == (newIdentities, newValues))) println(newByteProjection.get.descriptor)
  //          else println("okay")
  //          newByteProjection.get.unproject(newByteProjection.get.project(newIdentities, newValues)._1, newByteProjection.get.project(newIdentities, newValues)._2)(identityFunction) must_== (newIdentities, newValues)

  //        }
  //        else {
  //          print("?")
  //          val noneColDes = ColumnDescriptor(Path(""),JPath(""),SInt,Ownership(Set()))
  //          val byteProjection: LevelDBByteProjection = constructByteProjection(ListMap(noneColDes -> 0), Seq((noneColDes, ByValue)))
  //          val newIdentities: Vector[Long] = Vector(1L)
  //          val newValues: Seq[CValue] = Seq(CInt(2))
  //          byteProjection.unproject(byteProjection.project(newIdentities, newValues)._1, byteProjection.project(newIdentities, newValues)._2)(identityFunction) must_== (newIdentities, newValues)
  //        } 
  //      }
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
      val sorting: Seq[(ColumnDescriptor, SortBy)] = Seq((colDesInt, ByValue),(colDesDouble, ByValue),(colDesBoolean, ByValue))
      val byteProjection = byteProjectionInstance(columns, sorting) ||| { errorMessage => sys.error("problem constructing projection descriptor: " + errorMessage) }

      val expectedKey: Array[Byte] = Array(0,0,0,4,64,28,0,0,0,0,0,0,1,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,2)
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
      val sorting: Seq[(ColumnDescriptor, SortBy)] = Seq((colDesInt, ByValue),(colDesDouble, ByValue),(colDesBoolean, ByValue))
      val byteProjection = byteProjectionInstance(columns, sorting) ||| { errorMessage => sys.error("problem constructing projection descriptor: " + errorMessage) }

      byteProjection.unproject(byteProjection.project(Vector(1L,2L), Seq(cvInt, cvDouble, cvBoolean))._1, byteProjection.project(Vector(1L,2L), Seq(cvInt, cvDouble, cvBoolean))._2)(identityFunction) must_== (Vector(1L,2L), Seq(cvInt, cvDouble, cvBoolean))
    }

    "return the arguments of the project function (test3)" in {
      val columns: ListMap[ColumnDescriptor, Int] = ListMap(colDesFloat -> 0, colDesInt -> 1, colDesDouble -> 1, colDesBoolean -> 2, colDesDouble2 -> 2)
      val sorting: Seq[(ColumnDescriptor, SortBy)] = Seq((colDesFloat, ById),(colDesInt, ByValue), (colDesDouble, ById), (colDesBoolean, ByValue), (colDesDouble2, ById)) 
      val byteProjection = byteProjectionInstance(columns, sorting) ||| { errorMessage => sys.error("problem constructing projection descriptor: " + errorMessage) }
 
      byteProjection.unproject(byteProjection.project(Vector(1L,2L,3L), Seq(cvFloat, cvInt, cvDouble, cvBoolean, cvDouble2))._1, byteProjection.project(Vector(1L,2L,3L), Seq(cvFloat, cvInt, cvDouble, cvBoolean, cvDouble2))._2)(identityFunction) must_== (Vector(1L,2L,3L), Seq(cvFloat, cvInt, cvDouble, cvBoolean, cvDouble2))
    }

    
  }

}







