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

import com.precog.common.util._
import com.precog.common.kafka._

import com.weiglewilczek.slf4s.Logging

import java.net.InetAddress

import blueeyes.json.JsonAST._

trait ProductionActorConfig extends ActorEcosystemConfig {
  def kafkaHost: String = config[String]("kafka.batch.host")
  def kafkaPort: Int = config[Int]("kafka.batch.port")
  def kafkaTopic: String = config[String]("kafka.batch.topic") 

  def zookeeperHosts: String = config[String]("zookeeper.hosts")
  def zookeeperBase: List[String] = config[List[String]]("zookeeper.basepath")
  def zookeeperPrefix: String = config[String]("zookeeper.prefix")   

}

trait ProductionActorEcosystem extends BaseActorEcosystem with YggConfigComponent with Logging {
  type YggConfig <: ProductionActorConfig

  protected lazy val pre = "[Production Yggdrasil Shard]"

  lazy val actorSystem = ActorSystem("production_actor_system")

  lazy val routingActor = actorSystem.actorOf(Props(new BatchStoreActor(routingDispatch, yggConfig.batchStoreDelay, Some(ingestActor), actorSystem.scheduler, yggConfig.batchShutdownCheckInterval)), "router")
  
  private lazy val actorsWithStatus = List(
    projectionActors,
    metadataActor,
    routingActor,
    ingestActor,
    metadataSerializationActor
  )

  def actorsStatus(): Future[JArray] = {
    implicit val to = Timeout(yggConfig.statusTimeout)

    for (statusResponses <- Future.sequence { actorsWithStatus map { actor => (actor ? Status).mapTo[JValue] } }) 
    yield JArray(statusResponses)
  }

  protected def actorsStopInternal: Future[Unit] = {
    for {
      _  <- actorStop(ingestActor, "ingest")
      _  <- actorStop(projectionActors, "projection")
      _  <- actorStop(metadataActor, "metadata")
      _  <- actorStop(metadataSerializationActor, "flush")
    } yield ()
  }

  //
  // Internal only actors
  //
  
  private lazy val ingestActor = {
    val ingestBatchConsumer = new KafkaBatchConsumer(yggConfig.kafkaHost, yggConfig.kafkaPort, yggConfig.kafkaTopic)
    actorSystem.actorOf(Props(new KafkaShardIngestActor(checkpoints, ingestBatchConsumer)), "shard_ingest")
  }
 
  protected lazy val checkpoints: YggCheckpoints = {
    val systemCoordination = ZookeeperSystemCoordination(yggConfig.zookeeperHosts, yggConfig.serviceUID) 
    new SystemCoordinationYggCheckpoints(yggConfig.shardId, systemCoordination)
  }
}

// vim: set ts=4 sw=4 et:
