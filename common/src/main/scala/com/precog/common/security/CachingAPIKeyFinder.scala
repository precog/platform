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

import service.v1
import accounts.AccountId
import com.precog.util.cache.Cache

import akka.util.Duration

import java.util.concurrent.TimeUnit._
import org.joda.time.DateTime

import scalaz._
import scalaz.effect._
import scalaz.syntax.monad._
import scalaz.syntax.effect.id._

case class CachingAPIKeyFinderSettings(
  apiKeyCacheSettings: Seq[Cache.CacheOption[APIKey, v1.APIKeyDetails]]
)

object CachingAPIKeyFinderSettings {
  val Default = CachingAPIKeyFinderSettings(
    Seq(Cache.ExpireAfterWrite(Duration(5, MINUTES)), Cache.MaxSize(1000))
  )
}

class CachingAPIKeyFinder[M[+_]: Monad](delegate: APIKeyFinder[M], settings: CachingAPIKeyFinderSettings = CachingAPIKeyFinderSettings.Default) extends APIKeyFinder[M] {
  private val apiKeyCache = Cache.simple[APIKey, v1.APIKeyDetails](settings.apiKeyCacheSettings: _*)

  protected def add(r: v1.APIKeyDetails) = IO { apiKeyCache.put(r.apiKey, r) }

  def findAPIKey(tid: APIKey, rootKey: Option[APIKey]) = apiKeyCache.get(tid) match {
    case None => delegate.findAPIKey(tid, rootKey).map { _ map { _ tap add unsafePerformIO } }
    case t    => t.point[M]
  }

  def findAllAPIKeys(fromRoot: APIKey): M[Set[v1.APIKeyDetails]] = {
    sys.error("todo")
  }

  // TODO: Cache capability checks
  def hasCapability(apiKey: APIKey, perms: Set[Permission], at: Option[DateTime]): M[Boolean] =
    delegate.hasCapability(apiKey, perms, at)

  def createAPIKey(accountId: AccountId, keyName: Option[String] = None, keyDesc: Option[String] = None): M[v1.APIKeyDetails] =
    delegate.createAPIKey(accountId, keyName, keyDesc) map { _ tap add unsafePerformIO }

  def addGrant(accountKey: APIKey, grantId: GrantId): M[Boolean] =
    delegate.addGrant(accountKey, grantId) map { result =>
      // invalidate the cache on modification
      apiKeyCache.remove(accountKey)
      result
    }
}
