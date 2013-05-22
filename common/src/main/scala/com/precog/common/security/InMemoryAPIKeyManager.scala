package com.precog.common
package security

import accounts.AccountId
import org.joda.time.DateTime
import org.joda.time.Instant

import blueeyes.util.Clock

import scala.collection.mutable

import scalaz._
import scalaz.std.option._
import scalaz.syntax.monad._

class InMemoryAPIKeyManager[M[+_]](clock: Clock)(implicit val M: Monad[M]) extends APIKeyManager[M] {
  import Permission._

  val (rootAPIKeyRecord, grants, apiKeys) = {
    val rootAPIKey = APIKeyManager.newAPIKey()
    val rootGrantId = APIKeyManager.newGrantId()

    val rootGrant = Grant(
      rootGrantId, some("root-grant"), some("The root grant"), rootAPIKey, Set(),
      Set(
        ReadPermission(Path.Root, WrittenByAny), 
        WritePermission(Path.Root, WriteAsAny), 
        DeletePermission(Path.Root, WrittenByAny)
      ),
      new Instant(0L),
      None
    )

    val rootAPIKeyRecord = APIKeyRecord(rootAPIKey, some("root-apiKey"), some("The root API key"), rootAPIKey, Set(rootGrantId), true)

    (rootAPIKeyRecord, mutable.Map(rootGrantId -> rootGrant), mutable.Map(rootAPIKey -> rootAPIKeyRecord))
  }

  def rootAPIKey: M[APIKey] = rootAPIKeyRecord.apiKey.point[M]
  def rootGrantId: M[GrantId] = rootAPIKeyRecord.grants.head.point[M]

  private val deletedAPIKeys = mutable.Map.empty[APIKey, APIKeyRecord]
  private val deletedGrants = mutable.Map.empty[GrantId, Grant]

  def populateAPIKey(name: Option[String], description: Option[String], issuerKey: APIKey, apiKey: APIKey, grants: Set[GrantId]): M[APIKeyRecord] = {
    val record = APIKeyRecord(apiKey, name, description, issuerKey, grants, false)
    apiKeys.put(record.apiKey, record)
    record.point[M]
  }

  def createAPIKey(name: Option[String], description: Option[String], issuerKey: APIKey, grants: Set[GrantId]): M[APIKeyRecord] = {
    val record = APIKeyRecord(APIKeyManager.newAPIKey(), name, description, issuerKey, grants, false)
    apiKeys.put(record.apiKey, record)
    record.point[M]
  }

  def createGrant(name: Option[String], description: Option[String], issuerKey: APIKey, parentIds: Set[GrantId], perms: Set[Permission], expiration: Option[DateTime]): M[Grant] = {
    val grant = Grant(APIKeyManager.newGrantId(), name, description, issuerKey, parentIds, perms, clock.instant(), expiration)
    grants.put(grant.grantId, grant)
    grant.point[M]
  }

  def listAPIKeys() = apiKeys.values.toList.point[M]
  def listGrants() = grants.values.toList.point[M]

  def findAPIKey(apiKey: APIKey) = apiKeys.get(apiKey).point[M]
  def findAPIKeyChildren(parent: APIKey) = {
    apiKeys.values.filter(_.issuerKey == parent).toSet.point[M]
  }

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
