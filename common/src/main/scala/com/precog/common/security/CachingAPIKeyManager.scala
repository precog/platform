package com.precog.common
package security

import com.precog.common.accounts.AccountId
import com.precog.util.cache.Cache

import akka.util.Duration
import java.util.concurrent.TimeUnit._
import com.weiglewilczek.slf4s.Logging

import org.joda.time.DateTime

import scalaz._
import scalaz.effect._
import scalaz.std.option._
import scalaz.std.list._
import scalaz.syntax.effect.id._
import scalaz.syntax.monad._
import scalaz.syntax.traverse._

case class CachingAPIKeyManagerSettings(
  apiKeyCacheSettings: Seq[Cache.CacheOption[APIKey, APIKeyRecord]],
  childCacheSettings: Seq[Cache.CacheOption[APIKey, Set[APIKeyRecord]]],
  grantCacheSettings: Seq[Cache.CacheOption[GrantId, Grant]]
)

object CachingAPIKeyManagerSettings {
  val Default = CachingAPIKeyManagerSettings(
    Seq(Cache.ExpireAfterWrite(Duration(5, MINUTES)), Cache.MaxSize(1000)),
    Seq(Cache.ExpireAfterWrite(Duration(5, MINUTES)), Cache.MaxSize(1000)),
    Seq(Cache.ExpireAfterWrite(Duration(5, MINUTES)), Cache.MaxSize(1000))
  )
}

class CachingAPIKeyManager[M[+_]](manager: APIKeyManager[M], settings: CachingAPIKeyManagerSettings = CachingAPIKeyManagerSettings.Default) extends APIKeyManager[M]
    with Logging {
  implicit val M = manager.M

  private val apiKeyCache = Cache.simple[APIKey, APIKeyRecord](settings.apiKeyCacheSettings: _*)
  private val childCache = Cache.simple[APIKey, Set[APIKeyRecord]](settings.childCacheSettings: _*)
  private val grantCache = Cache.simple[GrantId, Grant](settings.grantCacheSettings: _*)

  protected def add(r: APIKeyRecord) = IO {
    @inline def addChildren(k: APIKey, c: Set[APIKeyRecord]) = 
      childCache.put(k, childCache.get(k).getOrElse(Set()) union c)

    apiKeyCache.put(r.apiKey, r)
    addChildren(r.issuerKey, Set(r))
  }

  protected def add(g: Grant) = IO {
    grantCache.put(g.grantId, g)
  }

  protected def remove(r: APIKeyRecord) = IO {
    @inline def removeChildren(k: APIKey, c: Set[APIKeyRecord]) = 
      childCache.put(k, childCache.get(k).getOrElse(Set()) diff c)

    apiKeyCache.remove(r.apiKey)
    removeChildren(r.issuerKey, Set(r))
  }

  protected def remove(g: Grant) = IO {
    grantCache.remove(g.grantId)
  }

  def rootGrantId: M[GrantId] = manager.rootGrantId
  def rootAPIKey: M[APIKey] = manager.rootAPIKey

  def createAPIKey(name: Option[String], description: Option[String], issuerKey: APIKey, grants: Set[GrantId]) =
    manager.createAPIKey(name, description, issuerKey, grants) map { _ tap add unsafePerformIO }

  def createGrant(name: Option[String], description: Option[String], issuerKey: APIKey, parentIds: Set[GrantId], perms: Set[Permission], expiration: Option[DateTime]) =
    manager.createGrant(name, description, issuerKey, parentIds, perms, expiration) map { _ tap add unsafePerformIO }

  def findAPIKey(tid: APIKey) = apiKeyCache.get(tid) match {
    case None =>
      logger.debug("Cache miss on api key " + tid)
      manager.findAPIKey(tid) map { _.traverse(_ tap add).unsafePerformIO }

    case t    => M.point(t)
  }

  def findGrant(gid: GrantId) = grantCache.get(gid) match {
    case None        =>
      logger.debug("Cache miss on grant " + gid)
      manager.findGrant(gid) map { _.traverse(_ tap add).unsafePerformIO }

    case s @ Some(_) => M.point(s)
  }

  def findAPIKeyChildren(apiKey: APIKey): M[Set[APIKeyRecord]] = childCache.get(apiKey) match {
    case None    => manager.findAPIKeyChildren(apiKey) map { _.toList.traverse(_ tap add).unsafePerformIO.toSet }
    case Some(s) => M.point(s)
  }

  def listAPIKeys = manager.listAPIKeys
  def listGrants = manager.listGrants

  def listDeletedAPIKeys = manager.listDeletedAPIKeys
  def listDeletedGrants = manager.listDeletedGrants

  def findGrantChildren(gid: GrantId) = manager.findGrantChildren(gid)

  def findDeletedAPIKey(tid: APIKey) = manager.findDeletedAPIKey(tid)
  def findDeletedGrant(gid: GrantId) = manager.findDeletedGrant(gid)
  def findDeletedGrantChildren(gid: GrantId) = manager.findDeletedGrantChildren(gid)

  def addGrants(tid: APIKey, grants: Set[GrantId]) =
    manager.addGrants(tid, grants) map { _.traverse(_ tap add).unsafePerformIO } 
  def removeGrants(tid: APIKey, grants: Set[GrantId]) =
    manager.removeGrants(tid, grants) map { _.traverse(_ tap remove).unsafePerformIO }

  def deleteAPIKey(tid: APIKey) =
    manager.deleteAPIKey(tid) map { _.traverse(_ tap remove).unsafePerformIO }
  def deleteGrant(gid: GrantId) =
    manager.deleteGrant(gid) map { _.toList.traverse(_ tap remove).unsafePerformIO.toSet }
}

// vim: set ts=4 sw=4 et:
