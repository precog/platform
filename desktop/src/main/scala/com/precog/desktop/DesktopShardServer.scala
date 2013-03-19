package com.precog.shard
package desktop

import akka.actor.ActorSystem
import akka.dispatch.{ExecutionContext, Future, Promise}

import blueeyes.bkka._

import scalaz.Monad

import org.streum.configrity.Configuration

import com.precog.common.jobs.InMemoryJobManager
import com.precog.common.accounts.StaticAccountFinder
import com.precog.common.security.StaticAPIKeyFinder
import com.precog.shard.nihdb.NIHDBQueryExecutorComponent
import com.precog.standalone.StandaloneShardServer

object DesktopShardServer
    extends StandaloneShardServer
    with NIHDBQueryExecutorComponent {
  val caveatMessage = None

  val actorSystem = ActorSystem("desktopExecutorActorSystem")
  implicit val executionContext = ExecutionContext.defaultExecutionContext(actorSystem)
  implicit val M: Monad[Future] = new FutureMonad(executionContext)

  def configureShardState(config: Configuration) = Future {
    val apiKey = config[String]("security.masterAccount.apiKey")
    val apiKeyFinder = new StaticAPIKeyFinder(apiKey)
    val accountFinder = new StaticAccountFinder("desktop", apiKey, Some("/"))
    val jobManager = new InMemoryJobManager
    val platform = platformFactory(config.detach("queryExecutor"), apiKeyFinder, accountFinder, jobManager)

    val stoppable = Stoppable.fromFuture {
      platform.shutdown
    }

    ManagedQueryShardState(platform, apiKeyFinder, jobManager, clock, ShardStateOptions.NoOptions, stoppable)
  } recoverWith {
    case ex: Throwable =>
      System.err.println("Could not start NIHDB Shard server!!!")
      ex.printStackTrace
      Promise.failed(ex)
  }
}
