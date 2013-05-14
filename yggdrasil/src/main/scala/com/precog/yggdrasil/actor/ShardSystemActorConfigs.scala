/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
package com.precog.yggdrasil
package actor

import metadata.ColumnMetadata
import com.precog.util._
import com.precog.common._
import com.precog.common.security.PermissionsFinder
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

import org.joda.time.Instant

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

  def createYggCheckpointFlag = config.get[String]("ingest.createCheckpointFlag")

  def zookeeperHosts: String = config[String]("zookeeper.hosts")
  //def zookeeperBase: List[String] = config[List[String]]("zookeeper.basepath")
  //def zookeeperPrefix: String = config[String]("zookeeper.prefix")

  def serviceUID: ServiceUID = ZookeeperSystemCoordination.extractServiceUID(config)

  lazy val shardId = {
    val suid = serviceUID
    serviceUID.hostId + serviceUID.serviceId
  }

  val logPrefix = "[Production Yggdrasil Shard]"
}

trait KafkaIngestActorProjectionSystem extends ShardSystemActorModule {
  type YggConfig <: KafkaIngestActorProjectionSystemConfig

  def ingestFailureLog(checkpoint: YggCheckpoint, logRoot: File): IngestFailureLog

  override def initIngestActor(actorSystem: ActorSystem, routingActor: ActorRef, checkpoint: YggCheckpoint, checkpointCoordination: CheckpointCoordination, permissionsFinder: PermissionsFinder[Future]) = {
    yggConfig.ingestConfig map { conf =>
      val consumer = new SimpleConsumer(yggConfig.kafkaHost,
                                        yggConfig.kafkaPort,
                                        yggConfig.kafkaSocketTimeout.toMillis.toInt,
                                        yggConfig.kafkaBufferSize)

      actorSystem.actorOf(Props(
        new KafkaShardIngestActor( shardId = yggConfig.shardId,
                                   initialCheckpoint = checkpoint,
                                   consumer = consumer,
                                   topic = yggConfig.kafkaTopic,
                                   permissionsFinder = permissionsFinder,
                                   routingActor = routingActor, 
                                   ingestFailureLog = ingestFailureLog(checkpoint, conf.failureLogRoot),
                                   fetchBufferSize = conf.bufferSize,
                                   idleDelay = yggConfig.batchStoreDelay,
                                   ingestTimeout = conf.batchTimeout,
                                   maxCacheSize = conf.maxParallel,
                                   maxConsecutiveFailures = conf.maxConsecutiveFailures) {

        implicit val M = new FutureMonad(ExecutionContext.defaultExecutionContext(actorSystem))

        def handleBatchComplete(ck: YggCheckpoint) {
          logger.debug("Complete up to " + ck)
          checkpointCoordination.saveYggCheckpoint(yggConfig.shardId, ck)
          logger.info("Saved checkpoint: " + ck)
        }
      }), "ingest")
    }
  }

  override def checkpointCoordination = ZookeeperSystemCoordination(yggConfig.zookeeperHosts, yggConfig.serviceUID, yggConfig.ingestConfig.isDefined, yggConfig.createYggCheckpointFlag)
}

trait StandaloneShardSystemConfig extends ShardConfig {
  def shardId = "standalone"
  def logPrefix = "[Standalone Yggdrasil Shard]"
}

trait StandaloneActorProjectionSystem extends ShardSystemActorModule {
  type YggConfig <: StandaloneShardSystemConfig
  override def initIngestActor(actorSystem: ActorSystem, routingActor: ActorRef, checkpoint: YggCheckpoint, checkpointCoordination: CheckpointCoordination, permissionsFinder: PermissionsFinder[Future]) = None
  override def checkpointCoordination = CheckpointCoordination.Noop
}

// vim: set ts=4 sw=4 et:
/* tmux
type ShardSystemActorConfigs */
