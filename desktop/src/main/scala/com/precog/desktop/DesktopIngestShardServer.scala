package com.precog.shard
package desktop

import akka.actor.ActorSystem
import akka.dispatch.{ExecutionContext, Future, Promise}

import blueeyes.bkka.{FutureMonad, Stoppable}

import scalaz.{EitherT, Monad}

import org.streum.configrity.Configuration

import com.precog.common.jobs.InMemoryJobManager
import com.precog.common.accounts.StaticAccountFinder
import com.precog.common.security.StaticAPIKeyFinder
import com.precog.shard.nihdb.NIHDBQueryExecutorComponent
import com.precog.standalone._
import com.precog.ingest.{EventServiceDeps, EventService}
import com.precog.ingest.kafka.KafkaEventStore

object DesktopIngestShardServer
    extends StandaloneShardServer
    with StandaloneIngestServer
    with NIHDBQueryExecutorComponent {
  val caveatMessage = None

  val actorSystem = ActorSystem("desktopExecutorActorSystem")
  implicit val executionContext = ExecutionContext.defaultExecutionContext(actorSystem)
  implicit val M: Monad[Future] = new FutureMonad(executionContext)

  // FIXME: We should embed ZK and Kafka via internal calls (as opposed to using scripts for startup/shutdown)

  def configureShardState(config: Configuration) = Future {
    println("Configuration at configure shard state=%s".format(config))
    val rootAPIKey = config[String]("security.masterAccount.apiKey")
    val apiKeyFinder = new StaticAPIKeyFinder(rootAPIKey)
    val accountFinder = new StaticAccountFinder(rootAPIKey, "desktop")
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
