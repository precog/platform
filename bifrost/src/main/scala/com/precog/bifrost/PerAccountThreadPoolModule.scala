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
package com.precog.bifrost

import com.precog.common._

import com.precog.common.accounts._
import com.precog.common.security._

import java.util.concurrent.{ ConcurrentHashMap, Executors }
import java.util.concurrent.{ ThreadPoolExecutor, TimeUnit, LinkedBlockingQueue }

import akka.actor.ActorSystem
import akka.dispatch.{ Future, ExecutionContext }

import com.google.common.util.concurrent.ThreadFactoryBuilder

import scalaz._

/**
 * Provides a mechanism for returning an account-specific threadpool. These
 * threadpools are tied to the account and can be used to monitor CPU usage.
 */
class PerAccountThreadPooling(accountFinder: AccountFinder[Future]) {
  private val executorCache = new ConcurrentHashMap[AccountId, ExecutionContext]()

  private def threadFactoryFor(accountId: AccountId) =
    (new ThreadFactoryBuilder().setNameFormat(accountId + "%04d")).build

  private def asyncContextFor(accountId: AccountId): ExecutionContext = {
    if (executorCache.contains(accountId)) {
      executorCache.get(accountId)
    } else {
      val executor = new ThreadPoolExecutor(16, 128,
        60000, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue[Runnable](),
        threadFactoryFor(accountId))
      val ec = ExecutionContext.fromExecutor(executor)

      // FIXME: Dummy pool for now
      executorCache.putIfAbsent(accountId, ec) match {
        case null => ec
        case ec0 => executor.shutdown(); ec0
      }
    }
  }

  def getAccountExecutionContext(apiKey: APIKey): EitherT[Future, String, ExecutionContext] = {
    EitherT.eitherT(accountFinder.findAccountByAPIKey(apiKey) map { 
      case None =>
        \/.left("Could not locate accountId for apiKey " + apiKey)
      case Some(accountId) =>
        \/.right(asyncContextFor(accountId)) // FIXME: Which account should we use if there's more than one?
    })
  }
}
