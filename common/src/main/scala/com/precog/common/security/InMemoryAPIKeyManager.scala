package com.precog.common
package security

import org.joda.time.DateTime

import scala.collection.mutable

import scalaz._
import scalaz.std.option._
import scalaz.syntax.monad._

class InMemoryAPIKeyManager[M[+_]](implicit val M: Monad[M]) extends APIKeyManager[M] {
  val (rootAPIKeyRecord, grants, apiKeys) = { 
    val rootGrantId = APIKeyManager.newGrantId()
    val rootGrant = {
      def mkPerm(p: (Path, Set[AccountId]) => Permission) = p(Path("/"), Set())
      
      Grant(
        rootGrantId, some("root-grant"), some("The root grant"), None, Set(),
        Set(mkPerm(ReadPermission), mkPerm(ReducePermission), mkPerm(WritePermission), mkPerm(DeletePermission)),
        None
      )
    }
    
    val rootAPIKey = APIKeyManager.newAPIKey()
    val rootAPIKeyRecord = 
      APIKeyRecord(rootAPIKey, some("root-apiKey"), some("The root API key"), None, Set(rootGrantId), true)
      
    (rootAPIKeyRecord, mutable.Map(rootGrantId -> rootGrant), mutable.Map(rootAPIKey -> rootAPIKeyRecord))
  }

  def rootAPIKey: M[APIKey] = rootAPIKeyRecord.apiKey.point[M]
  def rootGrantId: M[GrantId] = rootAPIKeyRecord.grants.head.point[M] 
    
  private val deletedAPIKeys = mutable.Map.empty[APIKey, APIKeyRecord]
  private val deletedGrants = mutable.Map.empty[GrantId, Grant]

  def newAPIKey(name: Option[String], description: Option[String], issuerKey: APIKey, grants: Set[GrantId]): M[APIKeyRecord] = {
    val record = APIKeyRecord(APIKeyManager.newAPIKey(), name, description, some(issuerKey), grants, false)
    apiKeys.put(record.apiKey, record)
    record.point[M]
  }

  def newGrant(name: Option[String], description: Option[String], issuerKey: APIKey, parentIds: Set[GrantId], perms: Set[Permission], expiration: Option[DateTime]): M[Grant] = {
    val newGrant = Grant(APIKeyManager.newGrantId(), name, description, some(issuerKey), parentIds, perms, expiration)
    grants.put(newGrant.grantId, newGrant)
    newGrant.point[M]
  }

  def listAPIKeys() = apiKeys.values.toList.point[M] 
  def listGrants() = grants.values.toList.point[M]

  def findAPIKey(apiKey: APIKey) = apiKeys.get(apiKey).point[M]

  def findGrant(gid: GrantId) = grants.get(gid).point[M]
  def findGrantChildren(gid: GrantId) =
    grants.values.filter(_.parentIds.contains(gid)).toSet.point[M]

  def addGrants(apiKey: APIKey, grants: Set[GrantId]) = {
    apiKeys.get(apiKey).map { record =>
      val updated = record.copy(grants = record.grants ++ grants)
      apiKeys.put(apiKey, updated)
      updated
    }.point[M]
  }

  def listDeletedAPIKeys() = deletedAPIKeys.values.toList.point[M] 

  def listDeletedGrants() = deletedGrants.values.toList.point[M] 

  def findDeletedAPIKey(apiKey: APIKey) = deletedAPIKeys.get(apiKey).point[M] 

  def findDeletedGrant(gid: GrantId) = deletedGrants.get(gid).point[M] 

  def findDeletedGrantChildren(gid: GrantId) =
    deletedGrants.values.filter(_.parentIds.contains(gid)).toSet.point[M]

  def removeGrants(apiKey: APIKey, grants: Set[GrantId]) = {
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

  def deleteGrant(gid: GrantId) = {
    def deleteGrantAux(gid: GrantId): Set[Grant] = {
      grants.remove(gid).map { grant =>
        val children = grants.values.filter(_.parentIds.contains(gid))
        Set(grant) ++ children.flatMap(grant => deleteGrantAux(grant.grantId))
      }.getOrElse(Set.empty)
    }
    deleteGrantAux(gid).point[M]
  }

  def close() = ().point[M]
}
