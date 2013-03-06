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

import com.precog.common.accounts.AccountId
import com.precog.util.cache.Cache

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

class CachingAPIKeyManager[M[+_]](manager: APIKeyManager[M], settings: CachingAPIKeyManagerSettings = CachingAPIKeyManagerSettings.Default) extends APIKeyManager[M]
    with Logging {
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
    case None =>
      logger.debug("Cache miss on api key " + tid)
      manager.findAPIKey(tid).map { _ tap { _ foreach add } }

    case t    => M.point(t)
  }

  def findGrant(gid: GrantId) = grantCache.get(gid) match {
    case None        =>
      logger.debug("Cache miss on grant " + gid)
      manager.findGrant(gid).map { _ tap { _ foreach add } }

    case s @ Some(_) => M.point(s)
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
