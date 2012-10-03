package com.precog.yggdrasil
package actor

import metadata.ColumnMetadata
import com.precog.util._
import com.precog.common._
import com.precog.common.kafka._

import akka.actor._
import akka.dispatch._
import akka.util._
import akka.util.duration._
import akka.pattern.ask
import akka.pattern.gracefulStop

import _root_.kafka.consumer._

import blueeyes.json.JsonAST._

import com.weiglewilczek.slf4s.Logging

import scalaz._

import java.net.InetAddress

trait ProductionShardSystemConfig extends ShardConfig {
  def kafkaHost: String = config[String]("kafka.batch.host")
  def kafkaPort: Int = config[Int]("kafka.batch.port")
  def kafkaTopic: String = config[String]("kafka.batch.topic") 
  def kafkaSocketTimeout: Duration = config[Long]("kafka.socket_timeout", 5000) millis
  def kafkaBufferSize: Int = config[Int]("kafka.buffer_size", 64 * 1024)

  def ingestBufferSize: Int = config[Int]("ingest.buffer_size", 1024 * 1024)
  def ingestTimeout: Timeout = config[Int]("ingest.timeout", 120) seconds
  def ingestMaxParallel: Int = config[Int]("ingest.max_parallel", 5)
  def ingestMaxConsecutiveFailures: Int = config[Int]("ingest.max_consecutive_failures", 3)

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

  def initIngestActor(checkpoint: YggCheckpoint, metadataActor: ActorRef) = {
    val consumer = new SimpleConsumer(yggConfig.kafkaHost, yggConfig.kafkaPort, yggConfig.kafkaSocketTimeout.toMillis.toInt, yggConfig.kafkaBufferSize)
    Some(() => new KafkaShardIngestActor(shardId = yggConfig.shardId, 
                                         initialCheckpoint = checkpoint, 
                                         consumer = consumer, 
                                         topic = yggConfig.kafkaTopic, 
                                         ingestEnabled = yggConfig.ingestEnabled, 
                                         fetchBufferSize = yggConfig.ingestBufferSize,
                                         ingestTimeout = yggConfig.ingestTimeout,
                                         maxCacheSize = yggConfig.ingestMaxParallel,
                                         maxConsecutiveFailures = yggConfig.ingestMaxConsecutiveFailures) {
      def handleBatchComplete(pendingCheckpoint: YggCheckpoint, updates: Seq[(ProjectionDescriptor, Option[ColumnMetadata])]) {
        logger.debug(pendingCheckpoint + " to be updated")
        metadataActor ! IngestBatchMetadata(updates, pendingCheckpoint.messageClock, Some(pendingCheckpoint.offset))
      }
    })
  }

  def checkpointCoordination = ZookeeperSystemCoordination(yggConfig.zookeeperHosts, yggConfig.serviceUID, yggConfig.ingestEnabled) 
}

trait StandaloneShardSystemConfig extends ShardConfig {
  val shardId = "standalone"
  val logPrefix = "[Standalone Yggdrasil Shard]"
}

trait StandaloneShardSystemActorModule extends ShardSystemActorModule {
  type YggConfig <: StandaloneShardSystemConfig
  def initIngestActor(checkpoint: YggCheckpoint, metadataActor: ActorRef) = None
  def checkpointCoordination = CheckpointCoordination.Noop
}

// vim: set ts=4 sw=4 et:
/* tmux
type ShardSystemActorConfigs */

