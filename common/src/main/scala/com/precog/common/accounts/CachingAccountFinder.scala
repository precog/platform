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
package accounts

import akka.util.Duration

import com.precog.common.Path
import com.precog.common.accounts._
import com.precog.common.security.APIKey
import com.precog.util.cache.Cache

import java.util.concurrent.TimeUnit._

import org.joda.time.DateTime

import scalaz._
import scalaz.effect.IO
import scalaz.syntax.effect.id._
import scalaz.syntax.monad._

case class CachingAccountFinderSettings(
  byKeyCacheSettings: Seq[Cache.CacheOption[APIKey, AccountId]],
  byAccountIdCacheSettings: Seq[Cache.CacheOption[AccountId, AccountDetails]]
)

object CachingAccountFinderSettings {
  val Default = CachingAccountFinderSettings(
    Seq(Cache.ExpireAfterWrite(Duration(5, MINUTES)), Cache.MaxSize(1000)),
    Seq(Cache.ExpireAfterWrite(Duration(5, MINUTES)), Cache.MaxSize(1000))
  )
}

class CachingAccountFinder[M[+_]: Monad](delegate: AccountFinder[M], settings: CachingAccountFinderSettings = CachingAccountFinderSettings.Default) extends AccountFinder[M] {
  private val byKeyCache = Cache.simple[APIKey, AccountId](settings.byKeyCacheSettings: _*)
  private val byAccountIdCache = Cache.simple[AccountId, AccountDetails](settings.byAccountIdCacheSettings: _*)

  protected def add(apiKey: APIKey, accountId: AccountId) = IO {
    byKeyCache.put(apiKey, accountId)
  }

  protected def add(details: AccountDetails) = IO {
    byAccountIdCache.put(details.accountId, details)
  }

  def findAccountByAPIKey(apiKey: APIKey) = byKeyCache.get(apiKey) match {
    case None =>
      delegate.findAccountByAPIKey(apiKey) map { _ map { _ tap(add(apiKey, _)) unsafePerformIO } }

    case idOpt =>
      idOpt.point[M]
  }

  def findAccountDetailsById(accountId: AccountId) = byAccountIdCache.get(accountId) match {
    case None =>
      delegate.findAccountDetailsById(accountId) map { _ map { _ tap add unsafePerformIO } }

    case detailsOpt =>
      detailsOpt.point[M]
  }
}
