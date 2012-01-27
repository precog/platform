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
      val testIdentity: Vector[Long] = Vector(2L)
      val cv1 = CInt(5)
      val testValues: Seq[CValue] = Seq(cv1)

      val path0: Path = Path("path")
      val selector0: JPath = JPath("jpath")
      val valueType0: ColumnType = SInt 

      val listmap0: ColumnDescriptor = ColumnDescriptor(path0, selector0, valueType0, Ownership(Set()))
      //val listmap1: ColumnDescriptor = ColumnDescriptor(path0, selector1, valueType1, Ownership(Set()))

      val int0: Int = 0 //must be 0 so that identity indexes are 0-based
      val int1: Int = 1

      val columns: ListMap[ColumnDescriptor, Int] = ListMap(listmap0 -> int0)
      val sorting: Seq[(ColumnDescriptor, SortBy)] = Seq((listmap0, ByValue))
       
      val byteProjectionV = ProjectionDescriptor(columns, sorting) map { d => 
        new LevelDBByteProjection {
          val descriptor: ProjectionDescriptor = d
        }
      }

      val byteProjection = byteProjectionV ||| { errorMessage => sys.error("problem constructing projection descriptor: " + errorMessage) } 

      

      val expectedKey: Array[Byte] = Array(0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 2) 
      val expectedValue: Array[Byte] = Array()
      byteProjection.project(testIdentity, testValues)._1 must_== expectedKey
      byteProjection.project(testIdentity, testValues)._2 must_== expectedValue

    }
  }
}

