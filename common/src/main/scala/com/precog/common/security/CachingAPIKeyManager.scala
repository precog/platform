package com.precog.common
package security

import com.precog.common.cache.Cache

import akka.util.Duration

import blueeyes._

import com.weiglewilczek.slf4s.Logging

import java.util.concurrent.TimeUnit._

import org.joda.time.DateTime

import scalaz._
import scalaz.syntax.monad._

case class CachingAPIKeyManagerSettings(
  apiKeyCacheSettings: Seq[Cache.CacheOption[APIKey, APIKeyRecord]],
  grantCacheSettings: Seq[Cache.CacheOption[GrantId, Grant]]
)

class CachingAPIKeyManager[M[+_]](manager: APIKeyManager[M], settings: CachingAPIKeyManagerSettings = CachingAPIKeyManager.defaultSettings) (implicit val M: Monad[M])
    extends APIKeyManager[M]
    with Logging {

  private val apiKeyCache = Cache.simple[APIKey, APIKeyRecord](settings.apiKeyCacheSettings: _*)
  private val grantCache = Cache.simple[GrantId, Grant](settings.grantCacheSettings: _*)

  def rootGrantId: M[GrantId] = manager.rootGrantId
  def rootAPIKey: M[APIKey] = manager.rootAPIKey

  def newAPIKey(name: Option[String], description: Option[String], issuerKey: APIKey, grants: Set[GrantId]) =
    manager.newAPIKey(name, description, issuerKey, grants).map { _ ->- add }

  def newGrant(name: Option[String], description: Option[String], issuerKey: APIKey, parentIds: Set[GrantId], perms: Set[Permission], expiration: Option[DateTime]) =
    manager.newGrant(name, description, issuerKey, parentIds, perms, expiration).map { _ ->- add }

  def listAPIKeys() = manager.listAPIKeys
  def listGrants() = manager.listGrants

  def listDeletedAPIKeys() = manager.listDeletedAPIKeys
  def listDeletedGrants() = manager.listDeletedGrants

  def findAPIKey(tid: APIKey) = apiKeyCache.get(tid) match {
    case None =>
      logger.debug("Cache miss on api key " + tid)
      manager.findAPIKey(tid).map { _.map { _ ->- add } }

    case t    => M.point(t)
  }
  def findGrant(gid: GrantId) = grantCache.get(gid) match {
    case None        =>
      logger.debug("Cache miss on grant " + gid)
      manager.findGrant(gid).map { _.map { _ ->- add } }

    case s @ Some(_) => M.point(s)
  }
  def findGrantChildren(gid: GrantId) = manager.findGrantChildren(gid)

  def findDeletedAPIKey(tid: APIKey) = manager.findDeletedAPIKey(tid)
  def findDeletedGrant(gid: GrantId) = manager.findDeletedGrant(gid)
  def findDeletedGrantChildren(gid: GrantId) = manager.findDeletedGrantChildren(gid)

  def addGrants(tid: APIKey, grants: Set[GrantId]) =
    manager.addGrants(tid, grants).map { _.map { _ ->- add } }
  def removeGrants(tid: APIKey, grants: Set[GrantId]) =
    manager.removeGrants(tid, grants).map { _.map { _ ->- add } }

  def deleteAPIKey(tid: APIKey) =
    manager.deleteAPIKey(tid) map { _.map { _ ->- remove } }
  def deleteGrant(gid: GrantId) =
    manager.deleteGrant(gid) map { _.map { _ ->- remove } }

  private def add(r: APIKeyRecord) = apiKeyCache.put(r.apiKey, r)
  private def add(g: Grant) = grantCache.put(g.grantId, g)

  private def remove(r: APIKeyRecord) = apiKeyCache.remove(r.apiKey)
  private def remove(g: Grant) = grantCache.remove(g.grantId)

  def close() = manager.close
}

object CachingAPIKeyManager {
  val defaultSettings = CachingAPIKeyManagerSettings(
    Seq(Cache.ExpireAfterWrite(Duration(5, MINUTES)), Cache.MaxSize(1000)),
    Seq(Cache.ExpireAfterWrite(Duration(5, MINUTES)), Cache.MaxSize(1000))
  )
}
