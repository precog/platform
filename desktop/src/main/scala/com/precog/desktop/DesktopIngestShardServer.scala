package com.precog.shard
package desktop

import akka.actor.ActorSystem
import akka.dispatch.{ExecutionContext, Future, Promise}

import blueeyes.bkka._

import scalaz.{EitherT, Monad}

import org.streum.configrity.Configuration

import com.precog.common.jobs.InMemoryJobManager
import com.precog.common.accounts.StaticAccountFinder
import com.precog.common.security.StaticAPIKeyFinder
import com.precog.shard.nihdb.NIHDBQueryExecutorComponent
import com.precog.standalone.StandaloneShardServer
import com.precog.ingest.{EventServiceDeps, EventService}
import com.precog.ingest.kafka.KafkaEventStore

object DesktopIngestShardServer
    extends StandaloneShardServer
    with EventService
    with NIHDBQueryExecutorComponent {
  val caveatMessage = None

  val actorSystem = ActorSystem("desktopExecutorActorSystem")
  implicit val executionContext = ExecutionContext.defaultExecutionContext(actorSystem)
  implicit val M: Monad[Future] = new FutureMonad(executionContext)

  def configureShardState(config: Configuration) = Future {
    println("Configuration at configure shard state=%s".format(config))
    val apiKeyFinder = new StaticAPIKeyFinder(config[String]("security.masterAccount.apiKey"))
    val accountFinder = new StaticAccountFinder("desktop")
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

  def configure(config: Configuration): (EventServiceDeps[Future], Stoppable)  = {
    println("Configuration at configure=%s".format(config))
    val accountFinder0 = new StaticAccountFinder(config[String]("security.masterAccount.accountId"))
    val (eventStore0, stoppable) = KafkaEventStore(config, accountFinder0) getOrElse {
      sys.error("Invalid configuration: eventStore.central.zk.connect required")
    }

    val deps = EventServiceDeps[Future](
      apiKeyFinder = new StaticAPIKeyFinder[Future](config[String]("security.masterAccount.apiKey")),
      accountFinder = accountFinder0,
      eventStore = eventStore0,
      jobManager = new InMemoryJobManager[({ type λ[+α] = EitherT[Future, String, α] })#λ]()
    )

    (deps, stoppable)
  }
}
