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
  val testIdentity: Vector[Long] = Vector(1L)
  val testIdentity2: Vector[Long] = Vector(1L, 2L)
  val cvInt = CInt(4)
  val cvLong = CLong(5)
  val cvString = CString("string")
  val cvBoolean = CBoolean(true)
  val cvFloat = CFloat(6)
  val cvDouble = CDouble(7)
  val cvNum = CNum(8)
  val testValues: Seq[CValue] = Seq(cvLong, cvBoolean, cvFloat)
  val testValues2: Seq[CValue] = Seq(cvInt, cvDouble, cvBoolean)

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
  val colDesBoolean: ColumnDescriptor = ColumnDescriptor(path0, selector0, valueTypeBoolean, Ownership(Set()))
  val colDesInt: ColumnDescriptor = ColumnDescriptor(path0, selector0, valueTypeInt, Ownership(Set()))
  val colDesLong: ColumnDescriptor = ColumnDescriptor(path0, selector0, valueTypeLong, Ownership(Set()))
  val colDesFloat: ColumnDescriptor = ColumnDescriptor(path0, selector0, valueTypeFloat, Ownership(Set()))
  val colDesDouble: ColumnDescriptor = ColumnDescriptor(path0, selector0, valueTypeDouble, Ownership(Set()))
  val colDesDecimal: ColumnDescriptor = ColumnDescriptor(path0, selector0, valueTypeDecimal, Ownership(Set()))
            

  val index0: Int = 0 //must be 0 so that identity indexes are 0-based
  val index1: Int = 1
  val index2: Int = 2
  val index3: Int = 3
  val index4: Int = 4
  val index5: Int = 5

  val columns: ListMap[ColumnDescriptor, Int] = 
        ListMap(colDesLong -> index0, colDesBoolean -> index0, colDesFloat -> index0)
    val sorting: Seq[(ColumnDescriptor, SortBy)] = Seq((colDesLong, ById),(colDesBoolean, ById),(colDesFloat, ByValueThenId))

    val byteProjectionV = ProjectionDescriptor(columns, sorting) map { d => 
      new LevelDBByteProjection {
        val descriptor: ProjectionDescriptor = d
      }
    }
  private val byteProjection = byteProjectionV ||| { errorMessage => sys.error("problem constructing projection descriptor: " + errorMessage) } 

  val columns2: ListMap[ColumnDescriptor, Int] = 
        ListMap(colDesInt -> index0, colDesDouble -> index1, colDesBoolean -> index1)
    val sorting2: Seq[(ColumnDescriptor, SortBy)] = Seq((colDesInt, ByValue),(colDesDouble, ByValueThenId),(colDesBoolean, ById))

    val byteProjectionV2 = ProjectionDescriptor(columns2, sorting2) map { d => 
      new LevelDBByteProjection {
        val descriptor: ProjectionDescriptor = d
      }
    }

  private val byteProjection2 = byteProjectionV2 ||| { errorMessage => sys.error("problem constructing projection descriptor: " + errorMessage) } 
}

class LevelDBByteProjectionSpec extends Specification {
  "a byte projection" should {
    "project to the expected key format" in {
  
      //start of first test
      val expectedKey: Array[Byte] = Array(0,0,0,0,0,0,0,1,64,-64,0,0)
      val expectedValue: Array[Byte] = Array(0,0,0,0,0,0,0,5,1)
      byteProjection.project(testIdentity, testValues)._1 must_== expectedKey
      byteProjection.project(testIdentity, testValues)._2 must_== expectedValue


      //start of second test
      val expectedKey2: Array[Byte] = Array(0,0,0,4,64,28,0,0,0,0,0,0,0,0,0,0,0,0,0,2,0,0,0,0,0,0,0,1)
      val expectedValue2: Array[Byte] = Array(1)
      byteProjection2.project(testIdentity2, testValues2)._1 must_== expectedKey2
      byteProjection2.project(testIdentity2, testValues2)._2 must_== expectedValue2

    }
  }
/*
  "when applied to the project function, the unproject function" should {
    "return the arguments of the project function" in {

      def functionf(identities: Identities, values: Seq[CValue]): (Identities, Seq[CValue]) = (identities, values)
      byteProjection2.unproject(byteProjection2.project(testIdentity2, testValues2)._1, byteProjection2.project(testIdentity2, testValues2)._2)(functionf) must_== (testIdentity2, testValues2)
    }
  }
*/
}



//empty projection descriptor

