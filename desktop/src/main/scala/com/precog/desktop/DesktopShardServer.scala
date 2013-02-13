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
import com.precog.shard.jdbm3.JDBMQueryExecutorComponent
import com.precog.standalone.StandaloneShardServer

object DesktopShardServer
    extends StandaloneShardServer
    with JDBMQueryExecutorComponent {

  val actorSystem = ActorSystem("desktopExecutorActorSystem")
  implicit val executionContext = ExecutionContext.defaultExecutionContext(actorSystem)
  implicit val M: Monad[Future] = new FutureMonad(executionContext)

  def configureShardState(config: Configuration) = Future {
    val accessControl = new StaticAPIKeyFinder(config[String]("security.masterAccount.apiKey"))
    val accountFinder = new StaticAccountFinder("desktop")
    val jobManager = new InMemoryJobManager
    val platform = platformFactory(config.detach("queryExecutor"), accessControl, accountFinder, jobManager)
    val stoppable = Stoppable.Noop

    ManagedQueryShardState(platform, apiKeyManager, accountManager, jobManager, clock, stoppable)
  } recoverWith {
    case ex: Throwable =>
      System.err.println("Could not start JDBM Shard server!!!")
      ex.printStackTrace
      Promise.failed(ex)
  }
}
