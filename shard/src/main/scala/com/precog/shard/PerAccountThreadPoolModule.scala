package com.precog.shard

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
