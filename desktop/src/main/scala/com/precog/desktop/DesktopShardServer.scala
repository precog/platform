package com.precog.shard
package desktop

import akka.actor.ActorSystem
import akka.dispatch.{ExecutionContext, Future, Promise}

import blueeyes.bkka._

import scalaz.Monad

import org.streum.configrity.Configuration

import com.precog.common.jobs.JobManager
import com.precog.common.accounts.StaticAccountFinder
import com.precog.common.security.APIKeyFinder
import com.precog.shard.nihdb.NIHDBQueryExecutorComponent
import com.precog.standalone.StandaloneShardServer

object DesktopShardServer
    extends StandaloneShardServer
    with NIHDBQueryExecutorComponent {
  val caveatMessage = None

  val actorSystem = ActorSystem("desktopExecutorActorSystem")
  implicit val executionContext = ExecutionContext.defaultExecutionContext(actorSystem)
  implicit val M: Monad[Future] = new FutureMonad(executionContext)

  def platformFor(config: Configuration, apiKeyFinder: APIKeyFinder[Future], jobManager: JobManager[Future]) = {
    val rootAPIKey = config[String]("security.masterAccount.apiKey")
    val accountFinder = new StaticAccountFinder("desktop", rootAPIKey, Some("/"))
    val platform = nihdbPlatform(config.detach("queryExecutor"), apiKeyFinder, accountFinder, jobManager)

    val stoppable = Stoppable.fromFuture {
      platform.shutdown
    }

    (platform, stoppable)
  }
}
