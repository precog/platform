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
      val cvInt5 = CInt(5)
      val cvInt6 = CInt(6) 
      val cvFloat = CFloat(77)
      val cvBoolean = CBoolean(true)
      val testValues: Seq[CValue] = Seq(cvInt5, cvInt6)

      val path0: Path = Path("path0")
      val selector0: JPath = JPath("key0")
      val selector1: JPath = JPath("key1")
      val valueTypeInt: ColumnType = SInt
      val valueTypeFloat: ColumnType = SFloat

      val colDesInt1: ColumnDescriptor = ColumnDescriptor(path0, selector0, valueTypeInt, Ownership(Set()))
      val colDesInt2: ColumnDescriptor = ColumnDescriptor(path0, selector1, valueTypeInt, Ownership(Set()))
      //val colDesFloat: ColumnDescriptor = ColumnDescriptor(path0, selector0, valueTypeFloat, Ownership(Set()))

      val index0: Int = 0 //must be 0 so that identity indexes are 0-based
      val index1: Int = 1

      val columns: ListMap[ColumnDescriptor, Int] = ListMap(colDesInt1 -> index0, colDesInt2 -> index1)
      val sortingA: Seq[(ColumnDescriptor, SortBy)] = Seq((colDesInt1, ByValue),(colDesInt2, ByValue))
      //val sortingB: Seq[(ColumnDescriptor, SortBy)] = Seq((colDesInt, ById))
      //val sortingC: Seq[(ColumnDescriptor, SortBy)] = Seq((colDesInt, ByValueThenId)) 

      val byteProjectionV = ProjectionDescriptor(columns, sortingA) map { d => 
        new LevelDBByteProjection {
          val descriptor: ProjectionDescriptor = d
        }
      }

      val byteProjection = byteProjectionV ||| { errorMessage => sys.error("problem constructing projection descriptor: " + errorMessage) } 

      

      val expectedKey: Array[Byte] = Array(0, 0, 0, 5,0,0,0,6,0, 0, 0, 0,0,0,0,1,0,0,0,0,0,0,0,2) 
      val expectedValue: Array[Byte] = Array()
      byteProjection.project(testIdentity, testValues)._1 must_== expectedKey
      byteProjection.project(testIdentity, testValues)._2 must_== expectedValue

    }
  }
}

