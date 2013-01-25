package com.precog.common
package security

import com.precog.common.accounts.AccountId
import com.precog.common.cache.Cache

import akka.util.Duration
import java.util.concurrent.TimeUnit._
import com.weiglewilczek.slf4s.Logging
import org.joda.time.DateTime

import scalaz._
import scalaz.std.option._
import scalaz.std.set._
import scalaz.syntax.id._
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

class CachingAPIKeyManager[M[+_]](manager: APIKeyManager[M], settings: CachingAPIKeyManagerSettings = CachingAPIKeyManagerSettings.Default) extends APIKeyManager[M] {
  implicit val M = manager.M

  private val apiKeyCache = Cache.simple[APIKey, APIKeyRecord](settings.apiKeyCacheSettings: _*)
  private val childCache = Cache.simple[APIKey, Set[APIKeyRecord]](settings.childCacheSettings: _*)
  private val grantCache = Cache.simple[GrantId, Grant](settings.grantCacheSettings: _*)

  protected def add(r: APIKeyRecord) = {
    @inline def addChildren(k: APIKey, c: Set[APIKeyRecord]) = 
      childCache.put(k, childCache.get(k).getOrElse(Set()) union c)

    apiKeyCache.put(r.apiKey, r)
    addChildren(r.issuerKey, Set(r))
  }

  protected def add(g: Grant) = grantCache.put(g.grantId, g)

  protected def remove(r: APIKeyRecord) = {
    @inline def removeChildren(k: APIKey, c: Set[APIKeyRecord]) = 
      childCache.put(k, childCache.get(k).getOrElse(Set()) diff c)

    apiKeyCache.remove(r.apiKey)
    removeChildren(r.issuerKey, Set(r))
  }

  protected def remove(g: Grant) = grantCache.remove(g.grantId)

  def rootGrantId: M[GrantId] = manager.rootGrantId
  def rootAPIKey: M[APIKey] = manager.rootAPIKey
  
  def newAPIKey(name: Option[String], description: Option[String], issuerKey: APIKey, grants: Set[GrantId]) =
    manager.newAPIKey(name, description, issuerKey, grants) map { _ tap add }

  def newGrant(name: Option[String], description: Option[String], issuerKey: APIKey, parentIds: Set[GrantId], perms: Set[Permission], expiration: Option[DateTime]) =
    manager.newGrant(name, description, issuerKey, parentIds, perms, expiration) map { _ tap add }

  def findAPIKey(tid: APIKey) = apiKeyCache.get(tid) match {
    case None => manager.findAPIKey(tid) map { _ tap { _ foreach add } }
    case t    => M.point(t)
  }

  def findGrant(gid: GrantId) = grantCache.get(gid) match {
    case None => manager.findGrant(gid) map { _ tap { _ foreach add } }
    case s    => M.point(s)
  }

  def findAPIKeyChildren(apiKey: APIKey): M[Set[APIKeyRecord]] = childCache.get(apiKey) match {
    case None    => manager.findAPIKeyChildren(apiKey) map { _ tap { _ foreach add } }
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
    manager.addGrants(tid, grants).map { _.map { _ tap add } }
  def removeGrants(tid: APIKey, grants: Set[GrantId]) =
    manager.removeGrants(tid, grants).map { _.map { _ tap add } }

  def deleteAPIKey(tid: APIKey) =
    manager.deleteAPIKey(tid) map { _ tap { _ foreach remove } }
  def deleteGrant(gid: GrantId) =
    manager.deleteGrant(gid) map { _ tap { _ foreach remove } }
}

// vim: set ts=4 sw=4 et:
