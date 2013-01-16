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
  def apiKeyManager: APIKeyManager[Future]

  implicit def defaultAsyncContext: ExecutionContext

  private lazy val executorCache = new ConcurrentHashMap[AccountId, ExecutionContext]()
  
  private def asyncContextFor(accountId: AccountId): ExecutionContext = {
    if (executorCache.contains(accountId)) {
      executorCache.get(accountId)
    } else {
      // FIXME: Dummy pool for now
      executorCache.putIfAbsent(accountId, ExecutionContext.fromExecutor(Executors.newCachedThreadPool()))
    }
  }

  def getAccountExecutionContext(apiKey: APIKey): EitherT[Future, String, ExecutionContext] = {
    EitherT.eitherT(
      apiKeyManager.rootPath(apiKey) flatMap { keyPath =>
        accountManager.findControllingAccount(keyPath) map { 
          case Some(accountId) =>
            \/.right(asyncContextFor(accountId))
          case None =>
            \/.left("Could not locate accountId for apiKey " + apiKey)
        }
      }
    )
  }
}
