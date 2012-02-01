package com.reportgrid.yggdrasil
package leveldb

import org.scalacheck.{Arbitrary,Gen}
import org.specs2.ScalaCheck
import org.specs2.mutable.Specification

import scala.collection.immutable.ListMap

import com.reportgrid.yggdrasil._
import com.reportgrid.common._
import com.reportgrid.analytics.Path

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

  val path0: Path = Path("path0")
  val selector0: JPath = JPath("key0")
  val valueTypeStringFixed: ColumnType = SStringFixed(1)
  val valueTypeStringArbitrary: ColumnType = SStringArbitrary
  val valueTypeBoolean: ColumnType = SBoolean
  val valueTypeInt: ColumnType = SInt
  val valueTypeLong: ColumnType = SLong
  val valueTypeFloat: ColumnType = SFloat
  val valueTypeDouble: ColumnType = SDouble
  val valueTypeDecimal: ColumnType = SDecimalArbitrary

  val colDesStringFixed: ColumnDescriptor = ColumnDescriptor(path0, selector0, valueTypeStringFixed, Ownership(Set()))
  val colDesStringArbitrary: ColumnDescriptor = ColumnDescriptor(path0, selector0, valueTypeStringArbitrary, Ownership(Set()))
  val colDesBoolean: ColumnDescriptor = ColumnDescriptor(Path("path0"), JPath("key0"), valueTypeBoolean, Ownership(Set()))
  val colDesInt: ColumnDescriptor = ColumnDescriptor(Path("path1"), JPath("key1"), valueTypeInt, Ownership(Set()))
  val colDesLong: ColumnDescriptor = ColumnDescriptor(Path("path2"), JPath("key2"), valueTypeLong, Ownership(Set()))
  val colDesFloat: ColumnDescriptor = ColumnDescriptor(Path("path3"), JPath("key3"), valueTypeFloat, Ownership(Set()))
  val colDesDouble: ColumnDescriptor = ColumnDescriptor(Path("path4"), JPath("key4"), valueTypeDouble, Ownership(Set()))
  val colDesDecimal: ColumnDescriptor = ColumnDescriptor(path0, selector0, valueTypeDecimal, Ownership(Set()))

  def byteProjectionInstance(columns: ListMap[ColumnDescriptor, Int], sorting: Seq[(ColumnDescriptor, SortBy)]) = { 
    ProjectionDescriptor(columns, sorting) map { d => 
      new LevelDBByteProjection {
        val descriptor: ProjectionDescriptor = d
      }
    }
  }

  def functionf(identities: Identities, values: Seq[CValue]): (Identities, Seq[CValue]) = (identities, values)

}


class LevelDBByteProjectionSpec extends Specification {
  "a byte projection" should {
    "project to the expected key format" in {
      val columns: ListMap[ColumnDescriptor, Int] = ListMap(colDesLong -> 0, colDesBoolean -> 0, colDesFloat -> 1)
      val sorting: Seq[(ColumnDescriptor, SortBy)] = Seq((colDesLong, ById),(colDesBoolean, ByValue),(colDesFloat, ByValueThenId))
      val byteProjection = byteProjectionInstance(columns, sorting) ||| { errorMessage => sys.error("problem constructing projection descriptor: " + errorMessage) }

      val expectedKey: Array[Byte] = Array(0,0,0,0,0,0,0,1,1,64,-64,0,0,0,0,0,0,0,0,0,2)
      val expectedValue: Array[Byte] = Array(0,0,0,0,0,0,0,5)
      byteProjection.project(Vector(1L,2L), Seq(cvLong, cvBoolean, cvFloat))._1 must_== expectedKey
      byteProjection.project(Vector(1L,2L), Seq(cvLong, cvBoolean, cvFloat))._2 must_== expectedValue

      //byteProjection.project(Vector(1L,2L), Seq(cvLong, cvBoolean, cvFloat)) must_== (expectedKey, expectedValue)

    }
    "project to the expected key format another" in {
      val columns: ListMap[ColumnDescriptor, Int] = ListMap(colDesInt -> 0, colDesDouble -> 1, colDesBoolean -> 1)
      val sorting: Seq[(ColumnDescriptor, SortBy)] = Seq((colDesInt, ByValue),(colDesDouble, ByValueThenId),(colDesBoolean, ByValue))
      val byteProjection = byteProjectionInstance(columns, sorting) ||| { errorMessage => sys.error("problem constructing projection descriptor: " + errorMessage) }

      val expectedKey: Array[Byte] = Array(0,0,0,4,64,28,0,0,0,0,0,0,0,0,0,0,0,0,0,2,1,0,0,0,0,0,0,0,1)
      val expectedValue: Array[Byte] = Array()
      byteProjection.project(Vector(1L,2L), Seq(cvInt, cvDouble, cvBoolean))._1 must_== expectedKey
      byteProjection.project(Vector(1L,2L), Seq(cvInt, cvDouble, cvBoolean))._2 must_== expectedValue
    } 

    "project to the expected key format yet another" in {
      val columns: ListMap[ColumnDescriptor, Int] = ListMap(colDesInt -> 0)
      val sorting: Seq[(ColumnDescriptor, SortBy)] = Seq((colDesInt, ByValue))
      val byteProjection = byteProjectionInstance(columns, sorting) ||| { errorMessage => sys.error("problem constructing projection descriptor: " + errorMessage) }

      val expectedKey: Array[Byte] = Array(0,0,0,4,0,0,0,0,0,0,0,1)
      val expectedValue: Array[Byte] = Array()
      byteProjection.project(Vector(1L), Seq(cvInt))._1 must_== expectedKey
      byteProjection.project(Vector(1L), Seq(cvInt))._2 must_== expectedValue

    }
  }

  "when applied to the project function, the unproject function" should {

    "return the arguments of the project function" in {
      val columns: ListMap[ColumnDescriptor, Int] = ListMap(colDesLong -> 0, colDesBoolean -> 0, colDesFloat -> 1)
      val sorting: Seq[(ColumnDescriptor, SortBy)] = Seq((colDesLong, ById),(colDesBoolean, ByValue),(colDesFloat, ByValueThenId))
      val byteProjection = byteProjectionInstance(columns, sorting) ||| { errorMessage => sys.error("problem constructing projection descriptor: " + errorMessage) }

      byteProjection.unproject(byteProjection.project(Vector(1L,2L), Seq(cvLong, cvBoolean, cvFloat))._1, byteProjection.project(Vector(1L,2L), Seq(cvLong, cvBoolean, cvFloat))._2)(functionf) must_== (Vector(1L,2L), Seq(cvLong, cvBoolean, cvFloat))
    }


//    "return the arguments of the project function another" in {
//      val columns: ListMap[ColumnDescriptor, Int] = ListMap(colDesInt -> 0, colDesLong -> 0/*, colDesBoolean -> 1, colDesFloat -> 2, colDesDouble -> 2*/)
//      val sorting: Seq[(ColumnDescriptor, SortBy)] = Seq((colDesInt, ByValue),(colDesLong, ByValueThenId)/*, (colDesBoolean, ById), (colDesFloat, ByValue), (colDesDouble, ById)*/) 
//      val byteProjection = byteProjectionInstance(columns, sorting) ||| { errorMessage => sys.error("problem constructing projection descriptor: " + errorMessage) }
//
//      byteProjection.unproject(byteProjection.project(Vector(1L), Seq(cvInt, cvLong/*, cvBoolean, cvFloat, cvDouble*/))._1, byteProjection.project(Vector(1L), Seq(cvInt, cvLong/*, cvBoolean, cvFloat, cvDouble*/))._2)(functionf) must_== (Vector(1L), Seq(cvInt, cvLong/*, cvBoolean, cvFloat, cvDouble*/))
//    }

    
  }

}

//case class ProjectionDescriptorInstance(columns, sorting) extends ProjectionDescriptor {




//empty projection descriptor

