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
package com.precog.yggdrasil.shard

import akka.actor._
import akka.dispatch._
import akka.util._
import akka.util.duration._
import akka.pattern.ask
import akka.pattern.gracefulStop

import com.precog.common.util._
import com.precog.common.kafka._
import com.precog.yggdrasil._
import com.precog.yggdrasil.kafka._

import com.weiglewilczek.slf4s.Logging

trait ActorEcosystem {
  def actorSystem(): ActorSystem
  def metadataActor(): ActorRef
  def projectionsActor(): ActorRef
  def routingActor(): ActorRef
  def actorsStart(): Future[Unit]
  def actorsStop(): Future[Unit]
}

trait ProductionActorConfig extends BaseConfig {
  def shardId(): String = "shard" + System.getProperty("precog.shard.suffix", "") 

  def kafkaHost(): String = config[String]("kafka.batch.host")
  def kafkaPort(): Int = config[Int]("kafka.batch.port")
  def kafkaTopic(): String = config[String]("kafka.batch.topic") 

  def zookeeperHosts(): String = config[String]("zookeeper.hosts")
  def zookeeperBase(): List[String] = config[List[String]]("zookeeper.basepath")
  def zookeeperPrefix(): String = config[String]("zookeeper.prefix")   
}

trait ProductionActorEcosystem extends ActorEcosystem with Logging {

  val pre = "[Yggdrasil Shard]"

  type YggConfig <: ProductionActorConfig

  def yggState(): YggState
  def yggConfig(): YggConfig

  lazy val actorSystem = ActorSystem("production_actor_system")
  private lazy implicit val executionContext = ExecutionContext.defaultExecutionContext(actorSystem)

  lazy val metadataActor = {
    actorSystem.actorOf(Props(new ShardMetadataActor(yggState.metadata, checkpoints.latestCheckpoint.messageClock)), "metadata") 
  }
  
  lazy val projectionsActor = {
    actorSystem.actorOf(Props(new ProjectionActors(yggState.descriptorLocator, yggState.descriptorIO, actorSystem.scheduler)), "projections")
  }
  
  lazy val routingActor = {
    val routingTable = new SingleColumnProjectionRoutingTable
    actorSystem.actorOf(Props(new RoutingActor(routingTable, Some(ingestActor), projectionsActor, metadataActor, actorSystem.scheduler)), "router")
  }
  
  def actorsStart() = Future[Unit] {
    this.metadataSyncCancel
    routingActor ! CheckMessages
  }

  def actorsStop(): Future[Unit] = {
    import logger._

    val defaultSystem = actorSystem
    val defaultTimeout = 300 seconds
    implicit val timeout: Timeout = defaultTimeout

    def actorStop(actor: ActorRef, name: String): Future[Unit] = { 
      for {
        _ <- Future(debug(pre + "Stopping " + name + " actor"))
        b <- gracefulStop(actor, defaultTimeout)(defaultSystem) 
      } yield {
        debug(pre + "Stop call for " + name + " actor returned " + b)  
      }   
    } recover { 
      case e => error("Error stopping " + name + " actor", e)  
    }   

    def routingActorStop = for {
      _ <- Future(debug(pre + "Sending controlled stop message to routing actor"))
      _ <- (routingActor ? ControlledStop) recover { case e => error("Controlled stop failed for routing actor.", e) }
      _ <- actorStop(routingActor, "routing")
    } yield () 

    def flushMetadata = {
      debug(pre + "Flushing metadata")
      (metadataActor ? FlushMetadata(metadataSerializationActor)) recover { case e => error("Error flushing metadata", e) }
    }

    for {
      _  <- Future(info(pre + "Stopping"))
      _  <- Future {
              debug(pre + "Stopping metadata sync")
              metadataSyncCancel.cancel
            }
      _  <- routingActorStop
      _  <- flushMetadata
      _  <- actorStop(ingestActor, "ingest")
      _  <- actorStop(projectionsActor, "projection")
      _  <- actorStop(metadataActor, "metadata")
      _  <- actorStop(metadataSerializationActor, "flush")
      _  <- Future {
              debug(pre + "Stopping actor system")
              actorSystem.shutdown
              info(pre + "Stopped")
            } recover { 
              case e => error("Error stopping actor system", e)
            }
    } yield ()
  }

  //
  // Internal only actors
  //
  
  private val metadataSyncPeriod = Duration(1, "minutes")
  
  
  private lazy val ingestBatchConsumer = {
    new KafkaBatchConsumer(yggConfig.kafkaHost, yggConfig.kafkaPort, yggConfig.kafkaTopic)
  }

  private lazy val ingestActor = {
    actorSystem.actorOf(Props(new KafkaShardIngestActor(checkpoints, ingestBatchConsumer)), "shard_ingest")
  }
  
  private lazy val metadataSerializationActor = {
    actorSystem.actorOf(Props(new MetadataSerializationActor(checkpoints, yggState.metadataIO)), "metadata_serializer")
  }

  
  private lazy val metadataSyncCancel = actorSystem.scheduler.schedule(metadataSyncPeriod, metadataSyncPeriod, metadataActor, FlushMetadata(metadataSerializationActor))
  
  private lazy val systemCoordination = {
    new ZookeeperSystemCoordination(yggConfig.zookeeperHosts, yggConfig.zookeeperBase, yggConfig.zookeeperPrefix) 
  }
  
  private lazy val checkpoints = new SystemCoordinationYggCheckpoints(yggConfig.shardId, systemCoordination)
}

trait StandaloneActorEcosystem extends ActorEcosystem with Logging {
  type YggConfig <: ProductionActorConfig
  
  val pre = "[Yggdrasil Shard]"

  def yggState(): YggState
  def yggConfig(): YggConfig

  lazy val actorSystem = ActorSystem("standalone_actor_system")
  private lazy implicit val executionContext = ExecutionContext.defaultExecutionContext(actorSystem)

  lazy val metadataActor = {
    actorSystem.actorOf(Props(new ShardMetadataActor(yggState.metadata, checkpoints.latestCheckpoint.messageClock)), "metadata") 
  }
  
  lazy val projectionsActor = {
    actorSystem.actorOf(Props(new ProjectionActors(yggState.descriptorLocator, yggState.descriptorIO, actorSystem.scheduler)), "projections")
  }
  
  lazy val routingActor = {
    val routingTable = new SingleColumnProjectionRoutingTable 
    actorSystem.actorOf(Props(new RoutingActor(routingTable, None, projectionsActor, metadataActor, actorSystem.scheduler)), "router")
  }
  
  def actorsStart() = Future[Unit] {
    this.metadataSyncCancel
    routingActor ! CheckMessages
  }

  def actorsStop(): Future[Unit] = {
    import logger._

    val defaultSystem = actorSystem
    val defaultTimeout = 300 seconds
    implicit val timeout: Timeout = defaultTimeout

    def actorStop(actor: ActorRef, name: String): Future[Unit] = { 
      for {
        _ <- Future(debug(pre + "Stopping " + name + " actor"))
        b <- gracefulStop(actor, defaultTimeout)(defaultSystem) 
      } yield {
        debug(pre + "Stop call for " + name + " actor returned " + b)  
      }   
    } recover { 
      case e => error("Error stopping " + name + " actor", e)  
    }   

    def routingActorStop = for {
      _ <- Future(debug(pre + "Sending controlled stop message to routing actor"))
      _ <- (routingActor ? ControlledStop) recover { case e => error("Controlled stop failed for routing actor.", e) }
      _ <- actorStop(routingActor, "routing")
    } yield () 

    def flushMetadata = {
      debug(pre + "Flushing metadata")
      (metadataActor ? FlushMetadata(metadataSerializationActor)) recover { case e => error("Error flushing metadata", e) }
    }

    for {
      _  <- Future(info(pre + "Stopping"))
      _  <- Future {
              debug(pre + "Stopping metadata sync")
              metadataSyncCancel.cancel
            }
      _  <- routingActorStop
      _  <- flushMetadata
      _  <- actorStop(projectionsActor, "projection")
      _  <- actorStop(metadataActor, "metadata")
      _  <- actorStop(metadataSerializationActor, "flush")
      _  <- Future {
              debug(pre + "Stopping actor system")
              actorSystem.shutdown
              info(pre + "Stopped")
            } recover { 
              case e => error("Error stopping actor system", e)
            }
    } yield ()
  }
  
  private val metadataSyncPeriod = Duration(1, "minutes")
  
  private lazy val metadataSerializationActor = {
    actorSystem.actorOf(Props(new MetadataSerializationActor(checkpoints, yggState.metadataIO)), "metadata_serializer")
  }

  
  private lazy val metadataSyncCancel = actorSystem.scheduler.schedule(metadataSyncPeriod, metadataSyncPeriod, metadataActor, FlushMetadata(metadataSerializationActor))
  
  private lazy val checkpoints = new YggCheckpoints {
    def saveRecoveryPoint(checkpoints: YggCheckpoint) { }
  }
}
