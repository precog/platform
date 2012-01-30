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
      val cvInt7 = CInt(7)
      val cvFloat = CFloat(7)
      val cvBoolean = CBoolean(true)
      val testValues: Seq[CValue] = Seq(cvInt5)

      val path0: Path = Path("path0")
      val selector0: JPath = JPath("key0")
      val selector1: JPath = JPath("key1")
      val valueTypeInt: ColumnType = SInt
      val valueTypeFloat: ColumnType = SFloat

      val colDesInt1: ColumnDescriptor = ColumnDescriptor(path0, selector0, valueTypeInt, Ownership(Set()))
      val colDesInt2: ColumnDescriptor = ColumnDescriptor(path0, selector1, valueTypeInt, Ownership(Set()))
      val colDesFloat: ColumnDescriptor = ColumnDescriptor(path0, selector0, valueTypeFloat, Ownership(Set()))

      val index0: Int = 0 //must be 0 so that identity indexes are 0-based
      val index1: Int = 1

      val columns1: ListMap[ColumnDescriptor, Int] = ListMap(colDesInt1 -> index0, colDesInt2 -> index0, colDesFloat -> index1)
      val columns2: ListMap[ColumnDescriptor, Int] = ListMap(colDesInt1 -> index0)
      //val sortingA: Seq[(ColumnDescriptor, SortBy)] = Seq((colDesInt1, ByValue),(colDesInt2, ByValue),(colDesFloat, ByValue))
      val sortingB: Seq[(ColumnDescriptor, SortBy)] = Seq((colDesInt1, ById)) //if we only have one columndescriptor in listmap then should both values be written?
      //val sortingC: Seq[(ColumnDescriptor, SortBy)] = Seq((colDesInt, ByValueThenId)) 

      val byteProjectionV = ProjectionDescriptor(columns2, sortingB) map { d => 
        new LevelDBByteProjection {
          val descriptor: ProjectionDescriptor = d
        }
      }

      val byteProjection = byteProjectionV ||| { errorMessage => sys.error("problem constructing projection descriptor: " + errorMessage) } 

      

      //val expectedKey: Array[Byte] = Array(0,0,0,5,0,0,0,6,64,-32,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,2)  
      val expectedKey: Array[Byte] = Array(0,0,0,0,0,0,0,1)
      val expectedValue: Array[Byte] = Array(0,0,0,5)
      byteProjection.project(testIdentity, testValues)._1 must_== expectedKey
      byteProjection.project(testIdentity, testValues)._2 must_== expectedValue


      //val expectedValueWidths = List(4,4)
      //byteProjection.listWidths(testValues) must_== expectedValueWidths

      //val expectedAllocateWidth = (Set(), Set(0,1), 8)
      //byteProjection.allocateWidth(expectedValueWidths) must_== expectedAllocateWidth

    }
  }
}

