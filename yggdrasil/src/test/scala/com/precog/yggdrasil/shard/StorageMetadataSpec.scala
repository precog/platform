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
package shard

import com.precog.common._
import com.precog.analytics._

import org.specs2.mutable.Specification

import org.scalacheck._

import blueeyes.json.JsonAST._
import blueeyes.json.JPath

import scala.collection.mutable
import scala.collection.immutable.ListMap

import scalaz._
import Scalaz._

class MetadataUpdateHelperSpec extends Specification {

  "metadata update helper" should {
    val colDesc1 = ColumnDescriptor(Path("/"), JPath(".foo"), SInt, Ownership(Set()))
    val descriptor1 = ProjectionDescriptor(ListMap[ColumnDescriptor, Int]((colDesc1 -> 0)), Seq[(ColumnDescriptor, SortBy)]((colDesc1 -> ById))).toOption.get

    def emptyProjections = mutable.Map[ProjectionDescriptor, Seq[MetadataMap]]()

    "add initial metadata for the first value inserted" in {
      val projections = emptyProjections
      val value = CInt(10)
   
      val valueStats = MetadataUpdateHelper.valueStats(value).get
      val expectedMetadata = List(mutable.Map[MetadataType, Metadata]((valueStats.metadataType, valueStats)))
      
      val result = MetadataUpdateHelper.applyMetadata(descriptor1, List(value), List(Set()), projections) 

      result must_== expectedMetadata
    }

    "update existing metadata for values other than the first inserted" in {
      val initialValue = CInt(10)
   
      val initialValueStats = MetadataUpdateHelper.valueStats(initialValue).get
      val initialMetadata = List(mutable.Map[MetadataType, Metadata]((initialValueStats.metadataType, initialValueStats)))
      
      val projections = emptyProjections
      
      projections += (descriptor1 -> initialMetadata)

      val value = CInt(20)
   
      val valueStats = MetadataUpdateHelper.valueStats(value).flatMap{ _.merge(initialValueStats) }.get
      val expectedMetadata = List(mutable.Map[MetadataType, Metadata]((valueStats.metadataType, valueStats)))
     
      val result = MetadataUpdateHelper.applyMetadata(descriptor1, List(value), List(Set()), projections) 

      result must_== expectedMetadata
    }

    "metadata is correctly combined" in {
      val firstValue = CInt(10)
      val firstValueStats = MetadataUpdateHelper.valueStats(firstValue).get
      val firstMetadata = List(mutable.Map[MetadataType, Metadata]((firstValueStats.metadataType, firstValueStats)))

      val secondValue = CInt(20)
   
      val secondValueStats = MetadataUpdateHelper.valueStats(secondValue).get
      val secondMetadata = List(mutable.Map[MetadataType, Metadata]((secondValueStats.metadataType, secondValueStats)))

      val result = MetadataUpdateHelper.combineMetadata(secondMetadata)(firstMetadata) 

      val expectedStats = firstValueStats.merge(secondValueStats).get
      val expected = List(mutable.Map[MetadataType, Metadata]((expectedStats.metadataType, expectedStats)))

      result must_== expected
    }
  }
}
