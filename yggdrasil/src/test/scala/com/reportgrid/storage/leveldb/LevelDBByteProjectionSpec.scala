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

      val byteProjection = byteProjectionV ||| { errorMessage => sys.error("problem constructing projection descriptor: " + errorMessage) } 
 
      val expectedKey: Array[Byte] = Array(0,0,0,0,0,0,0,1,64,-64,0,0)
      val expectedValue: Array[Byte] = Array(0,0,0,0,0,0,0,5,1)
      byteProjection.project(testIdentity, testValues)._1 must_== expectedKey
      byteProjection.project(testIdentity, testValues)._2 must_== expectedValue



      val columns2: ListMap[ColumnDescriptor, Int] = 
        ListMap(colDesInt -> index0, colDesDouble -> index1, colDesBoolean -> index1)
      val sorting2: Seq[(ColumnDescriptor, SortBy)] = Seq((colDesInt, ByValue),(colDesDouble, ByValueThenId),(colDesBoolean, ById))

      val byteProjectionV2 = ProjectionDescriptor(columns2, sorting2) map { d => 
        new LevelDBByteProjection {
          val descriptor: ProjectionDescriptor = d
        }
      }

      val byteProjection2 = byteProjectionV2 ||| { errorMessage => sys.error("problem constructing projection descriptor: " + errorMessage) } 
 
      val expectedKey2: Array[Byte] = Array(0,0,0,4,64,28,0,0,0,0,0,0,0,0,0,0,0,0,0,2,0,0,0,0,0,0,0,1)
      val expectedValue2: Array[Byte] = Array(1)
      byteProjection2.project(testIdentity2, testValues2)._1 must_== expectedKey2
      byteProjection2.project(testIdentity2, testValues2)._2 must_== expectedValue2


    }
  }
}



//empty projection descriptor

