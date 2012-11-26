package com.precog.common
package security

import java.util.concurrent.TimeUnit._
import org.joda.time.DateTime

import blueeyes._
import blueeyes.persistence.cache._

import scalaz._
import scalaz.syntax.monad._

case class CachingAPIKeyManagerSettings(
  apiKeyCacheSettings: CacheSettings[APIKey, APIKeyRecord],
  grantCacheSettings: CacheSettings[GrantID, Grant])

class CachingAPIKeyManager[M[+_]](manager: APIKeyManager[M], settings: CachingAPIKeyManagerSettings = CachingAPIKeyManager.defaultSettings)
  (implicit val M: Monad[M]) extends APIKeyManager[M] {

  private val apiKeyCache = Cache.concurrent[APIKey, APIKeyRecord](settings.apiKeyCacheSettings)
  private val grantCache = Cache.concurrent[GrantID, Grant](settings.grantCacheSettings)

  def rootGrantId: M[GrantID] = manager.rootGrantId
  def rootAPIKey: M[APIKey] = manager.rootAPIKey
  
  def newAPIKey(name: Option[String], description: Option[String], issuerKey: APIKey, grants: Set[GrantID]) =
    manager.newAPIKey(name, description, issuerKey, grants).map { _ ->- add }

  def newGrant(name: Option[String], description: Option[String], issuerKey: APIKey, parentIds: Set[GrantID], perms: Set[Permission], expiration: Option[DateTime]) =
    manager.newGrant(name, description, issuerKey, parentIds, perms, expiration).map { _ ->- add }

  def listAPIKeys() = manager.listAPIKeys
  def listGrants() = manager.listGrants

  def listDeletedAPIKeys() = manager.listDeletedAPIKeys
  def listDeletedGrants() = manager.listDeletedGrants

  def findAPIKey(tid: APIKey) = apiKeyCache.get(tid) match {
    case None => manager.findAPIKey(tid).map { _.map { _ ->- add } }
    case t    => Monad[M].point(t)
  }
  def findGrant(gid: GrantID) = grantCache.get(gid) match {
    case None        => manager.findGrant(gid).map { _.map { _ ->- add } }
    case s @ Some(_) => Monad[M].point(s)
  }
  def findGrantChildren(gid: GrantID) = manager.findGrantChildren(gid)

  def findDeletedAPIKey(tid: APIKey) = manager.findDeletedAPIKey(tid)
  def findDeletedGrant(gid: GrantID) = manager.findDeletedGrant(gid)
  def findDeletedGrantChildren(gid: GrantID) = manager.findDeletedGrantChildren(gid)

  def addGrants(tid: APIKey, grants: Set[GrantID]) =
    manager.addGrants(tid, grants).map { _.map { _ ->- add } }
  def removeGrants(tid: APIKey, grants: Set[GrantID]) =
    manager.removeGrants(tid, grants).map { _.map { _ ->- add } }

  def deleteAPIKey(tid: APIKey) =
    manager.deleteAPIKey(tid) map { _.map { _ ->- remove } }
  def deleteGrant(gid: GrantID) =
    manager.deleteGrant(gid) map { _.map { _ ->- remove } }

  private def add(r: APIKeyRecord) = apiKeyCache.put(r.apiKey, r)
  private def add(g: Grant) = grantCache.put(g.grantId, g)

  private def remove(r: APIKeyRecord) = apiKeyCache.remove(r.apiKey)
  private def remove(g: Grant) = grantCache.remove(g.grantId)

  def close() = manager.close
}

object CachingAPIKeyManager {
  val defaultSettings = CachingAPIKeyManagerSettings(
    CacheSettings[APIKey, APIKeyRecord](ExpirationPolicy(Some(5), Some(5), MINUTES)),
    CacheSettings[GrantID, Grant](ExpirationPolicy(Some(5), Some(5), MINUTES))
  )
}
