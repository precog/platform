package com.reportgrid.yggdrasil
package leveldb

import org.scalacheck.{Arbitrary,Gen}
import org.specs2.ScalaCheck
import org.specs2.mutable.Specification

import scala.collection.immutable.ListMap

import com.reportgrid.yggdrasil._

import com.reportgrid.common._

import com.reportgrid.analytics.Path

import blueeyes.json.JPath

import scalaz._

class LevelDBByteProjectionSpec extends Specification {
  "a byte projection" should {
    "project to the expected key format" in {
      val testIdentity: Vector[Long] = Vector(1L)
      val cvInt = CInt(4)
      val cvLong = CLong(5)
      val cvString = CString("string")
      val cvBoolean = CBoolean(true)
      val cvFloat = CFloat(6)
      val cvDouble = CDouble(7)
      val cvNum = CNum(8)
      val testValues: Seq[CValue] = Seq(cvLong, cvBoolean/*, cvFloat*/)

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
        ListMap(colDesLong -> index0, colDesBoolean -> index0/*, colDesFloat -> index0*/)
      val sorting: Seq[(ColumnDescriptor, SortBy)] = Seq((colDesLong, ById),(colDesBoolean, ById)/*,(colDesFloat, ByValueThenId)*/)

      val byteProjectionV = ProjectionDescriptor(columns, sorting) map { d => 
        new LevelDBByteProjection {
          val descriptor: ProjectionDescriptor = d
        }
      }

      val byteProjection = byteProjectionV ||| { errorMessage => sys.error("problem constructing projection descriptor: " + errorMessage) } 

      
      val expectedKey: Array[Byte] = Array(0,0,0,0,0,0,0,1)
      val expectedValue: Array[Byte] = Array(0,0,0,0,0,0,0,5,1)
      byteProjection.project(testIdentity, testValues)._1 must_== sys.error("todo")//expectedKey
      byteProjection.project(testIdentity, testValues)._2 must_== sys.error("todo")//expectedValue


      //val expectedValueWidths = List(4,4)
      //byteProjection.listWidths(testValues) must_== expectedValueWidths

      //val expectedAllocateWidth = (Set(), Set(0,1), 8)
      //byteProjection.allocateWidth(expectedValueWidths) must_== expectedAllocateWidth

    }
  }
}



//empty projection descriptor

