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
package com.precog.surtr

//import com.precog.bytecode._
//import com.precog.common._
//import com.precog.common.json.CPath
//import com.precog.yggdrasil._
//import com.precog.yggdrasil.table._
//
//import org.specs2.mutable._
//
//import scalaz._
//import scalaz.syntax.copointed._
//
//trait ProjectionMetadataSpecs[M[+_]] extends Specification 
//    with StorageMetadataSource[M] 
//    with SliceColumnarTableModule[M, Identities] {
//
//  implicit def M: Monad[M] with Copointed[M]
//
//  include(
//    "projection metadata" should {
//      "provide exact counts for single-projection tables" in {
//        val metadata = userMetadataView("fred-key")
//        
//        val usersAgeMetadata = metadata.findProjections(Path("/users"), CPath(".age")).copoint
//
//        usersAgeMetadata.size mustEqual 1
//
//        val (projectionDescriptor, columnMetadata) = usersAgeMetadata.toList.head
//        
//        // First, make sure the stored metadata is correct
//        columnMetadata.head._2(LongValueStats).asInstanceOf[MetadataStats].count mustEqual 100
//
//        // Now, load a table from the projection and verify size
//        val usersAgeTable = Table.load(Table.constString(Set("/users")), "fred-key", JObjectFixedT(Map("age" -> JNumberT))).copoint
//
//        usersAgeTable.size mustEqual ExactSize(100)
//      }
//
//      "provide estimate counts for multi-projection tables" in {
//        val usersFullTable = Table.load(Table.constString(Set("/users")), "fred-key", JType.JUniverseT).copoint
//
//        usersFullTable.size mustEqual EstimateSize(100, 100)
//
//        val tweetsTable = Table.load(Table.constString(Set("/election/tweets")), "fred-key", JType.JUniverseT).copoint
//
//        tweetsTable.size mustEqual EstimateSize(36, 11714) // 36 is Long "score" projection
//      }
//    }
//  )
//
//  
//}
