package com.precog.yggdrasil
package actor

import metadata.ColumnMetadata
import com.precog.util._
import com.precog.common._
import com.precog.common.accounts.AccountFinder
import com.precog.common.kafka._

import akka.actor._
import akka.dispatch._
import akka.util._
import akka.util.duration._
import akka.pattern.ask
import akka.pattern.gracefulStop

import _root_.kafka.consumer._

import blueeyes.bkka._
import blueeyes.json._

import com.weiglewilczek.slf4s.Logging
import org.streum.configrity.converter.Extra._
import _root_.kafka.consumer._

import java.io.File
import java.net.InetAddress

import scalaz._
import scalaz.syntax.id._


trait KafkaIngestActorProjectionSystemConfig extends ShardConfig {
  case class IngestConfig(
    bufferSize: Int,
    maxParallel: Int, 
    batchTimeout: Timeout, 
    failureLogRoot: File,
    maxConsecutiveFailures: Int)

  def kafkaHost: String = config[String]("kafka.batch.host")
  def kafkaPort: Int = config[Int]("kafka.batch.port")
  def kafkaTopic: String = config[String]("kafka.batch.topic") 
  def kafkaSocketTimeout: Duration = config[Long]("kafka.socket_timeout", 5000) millis
  def kafkaBufferSize: Int = config[Int]("kafka.buffer_size", 64 * 1024)

  def ingestConfig = config.detach("ingest") |> { config =>
    for {
      failureLogRoot <- config.get[File]("failure_log_root") if config[Boolean]("enabled", false)
    } yield {
      IngestConfig(
        bufferSize = config[Int]("buffer_size", 1024 * 1024),
        maxParallel = config[Int]("max_parallel", 5),
        batchTimeout = config[Int]("timeout", 120) seconds,
        maxConsecutiveFailures = config[Int]("ingest.max_consecutive_failures", 3),
        failureLogRoot = failureLogRoot)
    }
  }

  def zookeeperHosts: String = config[String]("zookeeper.hosts")
  def zookeeperBase: List[String] = config[List[String]]("zookeeper.basepath")
  def zookeeperPrefix: String = config[String]("zookeeper.prefix")   

  def serviceUID: ServiceUID = ZookeeperSystemCoordination.extractServiceUID(config)

  lazy val shardId = {
    val suid = serviceUID
    serviceUID.hostId + serviceUID.serviceId
  }

  val logPrefix = "[Production Yggdrasil Shard]"
}

trait KafkaIngestActorProjectionSystem extends ShardSystemActorModule {
  type YggConfig <: KafkaIngestActorProjectionSystemConfig

  def executionContext: ExecutionContext

  def ingestFailureLog(checkpoint: YggCheckpoint): IngestFailureLog

  override def initIngestActor(actorSystem: ActorSystem, checkpoint: YggCheckpoint, metadataActor: ActorRef, accountFinder: AccountFinder[Future]) = {
    val consumer = new SimpleConsumer(yggConfig.kafkaHost, yggConfig.kafkaPort, yggConfig.kafkaSocketTimeout.toMillis.toInt, yggConfig.kafkaBufferSize)
    yggConfig.ingestConfig map { conf => actorSystem.actorOf(Props(
      new KafkaShardIngestActor(shardId = yggConfig.shardId, 
                                initialCheckpoint = checkpoint, 
                                consumer = consumer, 
                                topic = yggConfig.kafkaTopic, 
                                accountFinder = accountFinder,
                                ingestFailureLog = ingestFailureLog(checkpoint),
                                fetchBufferSize = conf.bufferSize,
                                ingestTimeout = conf.batchTimeout,
                                maxCacheSize = conf.maxParallel,
                                maxConsecutiveFailures = conf.maxConsecutiveFailures) {
        val M = new FutureMonad(executionContext)
        
        def handleBatchComplete(pendingCheckpoint: YggCheckpoint, updates: Seq[(ProjectionDescriptor, Option[ColumnMetadata])]) {
          logger.debug(pendingCheckpoint + " to be updated")
          metadataActor ! IngestBatchMetadata(updates, pendingCheckpoint.messageClock, Some(pendingCheckpoint.offset))
        }
      }), "ingest")
    }
  }

  override def checkpointCoordination = ZookeeperSystemCoordination(yggConfig.zookeeperHosts, yggConfig.serviceUID, yggConfig.ingestConfig.isDefined) 
}

trait StandaloneShardSystemConfig extends ShardConfig {
  def shardId = "standalone"
  def logPrefix = "[Standalone Yggdrasil Shard]"
}

trait StandaloneActorProjectionSystem extends ShardSystemActorModule {
  type YggConfig <: StandaloneShardSystemConfig
  override def initIngestActor(actorSystem: ActorSystem, checkpoint: YggCheckpoint, metadataActor: ActorRef, accountFinder: AccountFinder[Future]) = None
  override def checkpointCoordination = CheckpointCoordination.Noop
}

// vim: set ts=4 sw=4 et:
/* tmux
type ShardSystemActorConfigs */

