package com.precog.pandora

import com.precog.bytecode._
import com.precog.common._
import com.precog.common.json.CPath
import com.precog.yggdrasil.{CString, StorageModule}
import com.precog.yggdrasil.{EstimateSize, ExactSize}
import com.precog.yggdrasil.table.jdbm3._

import org.specs2.mutable._

import scalaz._
import scalaz.syntax.copointed._

trait ProjectionMetadataSpecs[M[+_]] 
    extends Specification 
    with StorageModule[M] 
    with JDBMColumnarTableModule[M] {

  implicit def M: Monad[M] with Copointed[M]

  include(
    "projection metadata" should {
      "provide exact counts for single-projection tables" in {
        val metadata = storage.userMetadataView("fred-key")
        
        val usersAgeMetadata = metadata.findProjections(Path("/users"), CPath(".age")).copoint

        usersAgeMetadata.size mustEqual 1

        val (projectionDescriptor, columnMetadata) = usersAgeMetadata.toList.head
        
        // First, make sure the stored metadata is correct
        columnMetadata.head._2(LongValueStats).asInstanceOf[MetadataStats].count mustEqual 100

        // Now, load a table from the projection and verify size
        val usersAgeTable = Table.load(Table.constString(Set(CString("/users"))), "fred-key", JObjectFixedT(Map("age" -> JNumberT))).copoint

        usersAgeTable.size mustEqual ExactSize(100)
      }

      "provide estimate counts for multi-projection tables" in {
        val usersFullTable = Table.load(Table.constString(Set(CString("/users"))), "fred-key", JType.JUniverseT).copoint

        usersFullTable.size mustEqual EstimateSize(100, 100)

        val tweetsTable = Table.load(Table.constString(Set(CString("/election/tweets"))), "fred-key", JType.JUniverseT).copoint

        tweetsTable.size mustEqual EstimateSize(36, 11714) // 36 is Long "score" projection
      }
    }
  )

  
}