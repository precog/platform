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

import akka.actor._
import akka.dispatch._
import akka.util._
import akka.util.duration._
import akka.pattern.ask
import akka.pattern.gracefulStop

import _root_.kafka.consumer._

import blueeyes.json.JsonAST._

import com.precog.common.util._
import com.precog.common.kafka._
import com.weiglewilczek.slf4s.Logging

import java.net.InetAddress

trait ProductionActorConfig extends ActorEcosystemConfig {
  val shardId: String = serviceUID.hostId + serviceUID.serviceId 
  val serviceUID: ServiceUID = ZookeeperSystemCoordination.extractServiceUID(config)

  val kafkaHost: String = config[String]("kafka.batch.host")
  val kafkaPort: Int = config[Int]("kafka.batch.port")
  val kafkaTopic: String = config[String]("kafka.batch.topic") 
  val kafkaSocketTimeout: Duration = config[Long]("kafka.socket_timeout", 5000) millis
  val kafkaBufferSize: Int = config[Int]("kafka.buffer_size", 64 * 1024)

  val zookeeperHosts: String = config[String]("zookeeper.hosts")
  val zookeeperBase: List[String] = config[List[String]]("zookeeper.basepath")
  val zookeeperPrefix: String = config[String]("zookeeper.prefix")   
  val systemCoordination = ZookeeperSystemCoordination(zookeeperHosts, serviceUID) 
}

trait ProductionActorEcosystem[Dataset[_]] extends BaseActorEcosystem[Dataset] with YggConfigComponent with Logging {
  type YggConfig <: ProductionActorConfig

  protected lazy val pre = "[Production Yggdrasil Shard]"

  lazy val actorSystem = ActorSystem("production_actor_system")

  protected lazy val actorsWithStatus = ingestActor :: 
                                        ingestSupervisor :: 
                                        metadataActor :: 
                                        metadataSerializationActor :: 
                                        projectionsActor :: Nil

  protected def actorsStopInternal: Future[Unit] = {
    for {
      _  <- actorStop(ingestActor, "ingest")
      _  <- actorStop(projectionsActor, "projection")
      _  <- actorStop(metadataActor, "metadata")
      _  <- actorStop(metadataSerializationActor, "flush")
    } yield ()
  }

  //
  // Internal only actors
  //
  
 
  protected lazy val checkpoints: YggCheckpoints = {
    SystemCoordinationYggCheckpoints.validated(yggConfig.shardId, yggConfig.systemCoordination) ||| {
      sys.error("Unable to create initial system coordination checkpoints.") 
    }
  }

  lazy val ingestActor = {
    val consumer = new SimpleConsumer(yggConfig.kafkaHost, yggConfig.kafkaPort, yggConfig.kafkaSocketTimeout.toMillis.toInt, yggConfig.kafkaBufferSize)
    actorSystem.actorOf(Props(new KafkaShardIngestActor(checkpoints.latestCheckpoint, metadataSerializationActor, consumer, yggConfig.kafkaTopic)), "shard_ingest")
  }
}

// vim: set ts=4 sw=4 et:
