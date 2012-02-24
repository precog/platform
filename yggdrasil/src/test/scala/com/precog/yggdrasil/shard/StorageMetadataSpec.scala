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
    val colDesc1 = ColumnDescriptor(Path("/"), JPath(".foo"), SInt, Authorities(Set()))
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
