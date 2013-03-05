package com.precog.shard
package desktop

import akka.actor.ActorSystem
import akka.dispatch.{ExecutionContext, Future, Promise}

import blueeyes.bkka.{FutureMonad, Stoppable}

import scalaz.{EitherT, Monad}

import org.apache.zookeeper.server._

import org.streum.configrity.Configuration

import com.precog.common.jobs.InMemoryJobManager
import com.precog.common.accounts.StaticAccountFinder
import com.precog.common.security.StaticAPIKeyFinder
import com.precog.shard.nihdb.NIHDBQueryExecutorComponent
import com.precog.standalone._
import com.precog.ingest.{EventServiceDeps, EventService}
import com.precog.ingest.kafka.KafkaEventStore

import scala.collection.JavaConversions

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

  case class EmbeddedServices(zookeeper: ZookeeperServerMain, kafka: (KafkaServerStartable, KafkaServerStartable))
A
  val embeddedService = this.service("embedded", "1.0") { context =>
    startup {
      val rootConfig = context.rootConfig

      EmbeddedServices(startZookeeperStandalone(rootConfig.detach("zookeeper")), startKafkaStandalone(rootConfig))
    } ->
    shutdown { (e: EmbeddedServices) =>
      e.zookeeper.shutdown()
      e.kafka._1.shutdown
      e.kafka._1.awaitShutdown
      e.kafka._2.shutdown
      e.kafka._2.awaitShutdown
    }
  }

  def startKafkaStandalone(config: Configuration): (KafkaServerStartable, KafkaServerStartable) = {
    val defaultProps = new java.util.Properties
    defaultProps.setProperty("brokerId", "0")
    defaultProps.setProperty("num.threads", "8")
    defaultProps.setProperty("socket.send.buffer", "1048576")
    defaultProps.setProperty("socket.receive.buffer", "1048576")
    defaultProps.setProperty("max.socket.request.bytes", "104857600")
    defaultProps.setProperty("num.partitions", "1")
    defaultProps.setProperty("log.flush.interval", "10000")
    defaultProps.setProperty("log.default.flush.interval.ms", "1000")
    defaultProps.setProperty("log.default.flush.scheduler.interval.ms", "1000")
    defaultProps.setProperty("log.retention.hours", "107374182")
    defaultProps.setProperty("log.file.size", "536870912")
    defaultProps.setProperty("log.cleanup.interval.mins", "1")
    defaultProps.setProperty("zk.connectiontimeout.ms", "1000000")

    defaultProps.setProperty("zk.connect", "localhost:" + config[String]("zookeeper.port"))

    val localProps = new Properties(defaultProps)
    localProps.putAll(config.detach("kafka.local").data.asJava)

    val centralProps = new Properties(defaultProps)
    centralProps.putAll(config.detach("kafka.central").data.asJava)
    centralProps.setProperty("enable.zookeeper", "true")

    val local = new KafkaServerStartable(new KafkaConfig(localProps))
    val central = new KafkaServerStartable(new KafkaConfig(centralProps))

    (new Thread {
      override def run() = {
        local.startup
        central.startup
      }
    }).start()

    (local, central)
  }

  def startZookeeperStandalone(config: Configuration): ZookeeperServerMain = {
    val config = new ServerConfig
    config.parse(Array[String](config[String]("port"), config[String]("dataDir")))

    val server = new ZookeeperServerMain

    (new Thread {
      override def run() = {
        server.runFromConfig(config)
      }
    }).start()

    server
  }
}
