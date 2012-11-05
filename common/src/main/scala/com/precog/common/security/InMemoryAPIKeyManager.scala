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
package com.precog.common
package security

import org.joda.time.DateTime

import scala.collection.mutable

import scalaz._
import scalaz.std.option._
import scalaz.syntax.monad._

class InMemoryAPIKeyManager[M[+_]](implicit val M: Monad[M]) extends APIKeyManager[M] {
  
  val (rootAPIKeyRecord, grants, apiKeys) = { 
    val rootGrantId = newGrantID()
    val rootGrant = {
      def mkPerm(p: (Path, Set[AccountID]) => Permission) = p(Path("/"), Set())
      
      Grant(
        rootGrantId, some("root-grant"), some("The root grant"), None, Set(),
        Set(mkPerm(ReadPermission), mkPerm(ReducePermission), mkPerm(WritePermission), mkPerm(DeletePermission)),
        None
      )
    }
    
    val rootAPIKey = newAPIKey()
    val rootAPIKeyRecord = 
      APIKeyRecord(rootAPIKey, some("root-apiKey"), some("The root API key"), None, Set(rootGrantId), true)
      
    (rootAPIKeyRecord, mutable.Map(rootGrantId -> rootGrant), mutable.Map(rootAPIKey -> rootAPIKeyRecord))
  }

  def rootAPIKey: M[APIKey] = rootAPIKeyRecord.apiKey.point[M]
  def rootGrantId: M[GrantID] = rootAPIKeyRecord.grants.head.point[M] 
    
  private val deletedAPIKeys = mutable.Map.empty[APIKey, APIKeyRecord]
  private val deletedGrants = mutable.Map.empty[GrantID, Grant]

  def newAPIKey(name: Option[String], description: Option[String], issuerKey: APIKey, grants: Set[GrantID]): M[APIKeyRecord] = {
    val record = APIKeyRecord(newAPIKey(), name, description, some(issuerKey), grants, false)
    apiKeys.put(record.apiKey, record)
    record.point[M]
  }

  def newGrant(name: Option[String], description: Option[String], issuerKey: APIKey, parentIds: Set[GrantID], perms: Set[Permission], expiration: Option[DateTime]): M[Grant] = {
    val newGrant = Grant(newGrantID(), name, description, some(issuerKey), parentIds, perms, expiration)
    grants.put(newGrant.grantId, newGrant)
    newGrant.point[M]
  }

  def listAPIKeys() = apiKeys.values.toList.point[M] 
  def listGrants() = grants.values.toList.point[M]

  def findAPIKey(apiKey: APIKey) = apiKeys.get(apiKey).point[M]

  def findGrant(gid: GrantID) = grants.get(gid).point[M]
  def findGrantChildren(gid: GrantID) =
    grants.values.filter(_.parentIds.contains(gid)).toSet.point[M]

  def addGrants(apiKey: APIKey, grants: Set[GrantID]) = {
    apiKeys.get(apiKey).map { record =>
      val updated = record.copy(grants = record.grants ++ grants)
      apiKeys.put(apiKey, updated)
      updated
    }.point[M]
  }

  def listDeletedAPIKeys() = deletedAPIKeys.values.toList.point[M] 

  def listDeletedGrants() = deletedGrants.values.toList.point[M] 

  def findDeletedAPIKey(apiKey: APIKey) = deletedAPIKeys.get(apiKey).point[M] 

  def findDeletedGrant(gid: GrantID) = deletedGrants.get(gid).point[M] 

  def findDeletedGrantChildren(gid: GrantID) =
    deletedGrants.values.filter(_.parentIds.contains(gid)).toSet.point[M]

  def removeGrants(apiKey: APIKey, grants: Set[GrantID]) = {
    apiKeys.get(apiKey).flatMap { record =>
      if(grants.subsetOf(record.grants)) {
        val updated = record.copy(grants = record.grants -- grants)
        apiKeys.put(apiKey, updated)
        Some(updated)
      } else None
    }.point[M]
  }

  def deleteAPIKey(apiKey: APIKey) =
    apiKeys.get(apiKey).flatMap { record =>
      deletedAPIKeys.put(apiKey, record)
      apiKeys.remove(apiKey)
    }.point[M]

  def deleteGrant(gid: GrantID) = {
    def deleteGrantAux(gid: GrantID): Set[Grant] = {
      grants.remove(gid).map { grant =>
        val children = grants.values.filter(_.parentIds.contains(gid))
        Set(grant) ++ children.flatMap(grant => deleteGrantAux(grant.grantId))
      }.getOrElse(Set.empty)
    }
    deleteGrantAux(gid).point[M]
  }

  def close() = ().point[M]
}
