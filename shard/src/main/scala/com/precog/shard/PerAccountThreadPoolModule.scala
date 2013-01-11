package com.precog.shard

import com.precog.common._
import com.precog.common.json._
import com.precog.common.security._

import com.precog.accounts.BasicAccountManager

import java.util.concurrent.{ ConcurrentHashMap, Executors }

import akka.dispatch.{ Future, ExecutionContext }

import scalaz._

/**
 * Provides a mechanism for returning an account-specific threadpool. These
 * threadpools are tied to the account and can be used to monitor CPU usage.
 */
trait PerAccountThreadPoolModule { self =>

  def accountManager: BasicAccountManager[Future]

  implicit def defaultAsyncContext: ExecutionContext

  private val executorCache = new ConcurrentHashMap[AccountId, ExecutionContext]()
  
  private def asyncContextFor(accountId: AccountId): ExecutionContext = {
    if (executorCache.contains(accountId)) {
      executorCache.get(accountId)
    } else {
      // FIXME: Dummy pool for now
      executorCache.putIfAbsent(accountId, ExecutionContext.fromExecutor(Executors.newCachedThreadPool()))
    }
  }

  def getAccountExecutionContext(apiKey: APIKey): EitherT[Future, String, ExecutionContext] = {
    EitherT.eitherT(accountManager.listAccountIds(apiKey) map { accounts =>
      if (accounts.size < 1) {
        \/.left("Could not locate accountId for apiKey " + apiKey)
      } else {
        \/.right(asyncContextFor(accounts.head)) // FIXME: Which account should we use if there's more than one?
      }
    })
  }
}