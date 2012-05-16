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
package metadata 

import actor._

import com.precog.common._

import org.specs2.mutable.Specification

import org.scalacheck._

import blueeyes.json.JsonAST._
import blueeyes.json.JPath

import scala.collection.immutable.ListMap

import scalaz._
import Scalaz._

class MetadataUpdateHelperSpec extends Specification {

  "metadata update helper" should {
    val colDesc1 = ColumnDescriptor(Path("/"), JPath(".foo"), CInt, Authorities(Set()))
    val descriptor1 = ProjectionDescriptor(1, List(colDesc1))

    def emptyProjections = Map[ProjectionDescriptor, (Boolean, ColumnMetadata)]()

    "add initial metadata for the first value inserted" in {
      val projections = emptyProjections
      val value = CInt(10)
   
      val valueStats = MetadataUpdateHelper.valueStats(value).get
      val expectedMetadata = Map((colDesc1 -> Map[MetadataType, Metadata]((valueStats.metadataType, valueStats))))
      
      val result = MetadataUpdateHelper.applyMetadata(descriptor1, List(value), List(Set()), projections) 

      result must_== expectedMetadata
    }

    "update existing metadata for values other than the first inserted" in {
      val initialValue = CInt(10)
   
      val initialValueStats = MetadataUpdateHelper.valueStats(initialValue).get
      val initialMetadata = Map[MetadataType, Metadata]((initialValueStats.metadataType -> initialValueStats))
      val initialColumnMetadata = Map[ColumnDescriptor, MetadataMap]((colDesc1 -> initialMetadata))
      
      val projections = emptyProjections + (descriptor1 -> (true, initialColumnMetadata))

      val value = CInt(20)
   
      val valueStats = MetadataUpdateHelper.valueStats(value).flatMap{ _.merge(initialValueStats) }.get
      val expectedMetadata = Map((colDesc1 -> Map[MetadataType, Metadata]((valueStats.metadataType -> valueStats))))
     
      val result = MetadataUpdateHelper.applyMetadata(descriptor1, List(value), List(Set()), projections) 

      result must_== expectedMetadata
    }

    "metadata is correctly combined" in {
      val firstValue = CInt(10)
      val firstValueStats = MetadataUpdateHelper.valueStats(firstValue).get
      val firstMetadata = List[Map[MetadataType, Metadata]](Map(firstValueStats.metadataType -> firstValueStats))

      val secondValue = CInt(20)
   
      val secondValueStats = MetadataUpdateHelper.valueStats(secondValue).get
      val secondMetadata = Map[MetadataType, Metadata]((secondValueStats.metadataType -> secondValueStats))
      val secondColumnMetadata = Map[ColumnDescriptor, MetadataMap]((colDesc1 -> secondMetadata))

      val result = MetadataUpdateHelper.combineMetadata(descriptor1, secondColumnMetadata, firstMetadata) 

      val expectedStats = firstValueStats.merge(secondValueStats).get
      val expected = Map((colDesc1 -> Map[MetadataType, Metadata]((expectedStats.metadataType -> expectedStats))))

      result must_== expected
    }
  }
}
