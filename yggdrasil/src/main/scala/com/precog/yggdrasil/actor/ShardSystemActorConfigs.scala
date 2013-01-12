package com.precog.yggdrasil
package actor

import metadata.ColumnMetadata
import com.precog.accounts.BasicAccountManager
import com.precog.util._
import com.precog.common._
import com.precog.common.kafka._

import akka.actor._
import akka.dispatch._
import akka.util._
import akka.util.duration._
import akka.pattern.ask
import akka.pattern.gracefulStop

import blueeyes.json._

import com.weiglewilczek.slf4s.Logging
import org.streum.configrity.converter.Extra._
import _root_.kafka.consumer._

import java.io.File
import java.net.InetAddress

import scalaz._
import scalaz.syntax.id._


trait ProductionShardSystemConfig extends ShardConfig {
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

trait ProductionShardSystemActorModule extends ShardSystemActorModule {
  type YggConfig <: ProductionShardSystemConfig

  def ingestFailureLog(checkpoint: YggCheckpoint): IngestFailureLog

  def initIngestActor(checkpoint: YggCheckpoint, metadataActor: ActorRef, accountManager: BasicAccountManager[Future]) = {
    val consumer = new SimpleConsumer(yggConfig.kafkaHost, yggConfig.kafkaPort, yggConfig.kafkaSocketTimeout.toMillis.toInt, yggConfig.kafkaBufferSize)
    yggConfig.ingestConfig map { conf => () => 
      new KafkaShardIngestActor(shardId = yggConfig.shardId, 
                                         initialCheckpoint = checkpoint, 
                                         consumer = consumer, 
                                         topic = yggConfig.kafkaTopic, 
                                         accountManager = accountManager,
                                         ingestFailureLog = ingestFailureLog(checkpoint),
                                         fetchBufferSize = conf.bufferSize,
                                         ingestTimeout = conf.batchTimeout,
                                         maxCacheSize = conf.maxParallel,
                                         maxConsecutiveFailures = conf.maxConsecutiveFailures) {
        def handleBatchComplete(pendingCheckpoint: YggCheckpoint, updates: Seq[(ProjectionDescriptor, Option[ColumnMetadata])]) {
          logger.debug(pendingCheckpoint + " to be updated")
          metadataActor ! IngestBatchMetadata(updates, pendingCheckpoint.messageClock, Some(pendingCheckpoint.offset))
        }
      }
    }
  }

  def checkpointCoordination = ZookeeperSystemCoordination(yggConfig.zookeeperHosts, yggConfig.serviceUID, yggConfig.ingestConfig.isDefined) 
}

trait StandaloneShardSystemConfig extends SystemActorStorageConfig {
  def shardId = "standalone"
  def logPrefix = "[Standalone Yggdrasil Shard]"
  def metadataServiceTimeout = metadataTimeout
}

trait StandaloneShardSystemActorModule extends ShardSystemActorModule {
  type YggConfig <: StandaloneShardSystemConfig
  def initIngestActor(checkpoint: YggCheckpoint, metadataActor: ActorRef, accountManager: BasicAccountManager[Future]) = None
  def checkpointCoordination = CheckpointCoordination.Noop
}

// vim: set ts=4 sw=4 et:
/* tmux
type ShardSystemActorConfigs */

