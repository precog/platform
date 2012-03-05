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

import com.precog.common.util._
import com.precog.common.kafka._
import com.precog.yggdrasil._
import com.precog.yggdrasil.kafka._

trait ProductionActorConfig extends 
    KafkaConsumerConfig with 
    ZookeeperSystemCoordinationConfig with
    ShardIdConfig with
    BaseConfig {
  def shardId(): String = "shard" + System.getProperty("precog.shard.suffix", "") 

  def kafkaHost(): String = config[String]("kafka.batch.host")
  def kafkaPort(): Int = config[Int]("kafka.batch.port")
  def kafkaTopic(): String = config[String]("kafka.batch.topic") 

  def zookeeperHosts(): String = config[String]("zookeeper.hosts")
  def zookeeperBase(): List[String] = config[List[String]]("zookeeper.basepath")
  def zookeeperPrefix(): String = config[String]("zookeeper.prefix")   
}

trait ProductionActorEcosystem extends
    KafkaIngestActorComponent with
    KafkaIngestBatchConsumerComponent with
    InMemoryMetadataActorComponent with
    SerialMetadataSerializationActorComponent with
    SingleColumnRoutingTableComponent with
    TableDrivenRoutingActorComponent with 
    CachedProjectionsActorComponent with 
    ZookeeperSystemCoordinationComponent with
    SystemCoordinationCheckpointsComponent {
  type YggConfig <: ProductionActorConfig
}

trait StandaloneActorEcosystem extends
    NoopIngestActorComponent with
    InMemoryMetadataActorComponent with
    SerialMetadataSerializationActorComponent with
    SingleColumnRoutingTableComponent with
    TableDrivenRoutingActorComponent with 
    CachedProjectionsActorComponent with 
    NullCheckpointsComponent {
  type YggConfig <: ProductionActorConfig
}

trait ActorEcosystem extends
  MetadataActorComponent with
  MetadataSerializationActorComponent with
  IngestActorComponent with
  ProjectionsActorComponent with
  RoutingActorComponent

trait ActorSystemComponent {
  def system(): ActorSystem
}

trait StateComponent {
  def yggState(): YggState
}

trait MetadataActorComponent {
  def metadataActor(): ActorRef
}

trait InMemoryMetadataActorComponent extends 
    MetadataActorComponent with 
    CheckpointsComponent with
    StateComponent with
    ActorSystemComponent {
  
  private def initialCheckpoint = checkpoints.latestCheckpoint.messageClock

  override lazy val metadataActor = {
    system.actorOf(Props(new ShardMetadataActor(yggState.metadata, initialCheckpoint)), "metadata") 
  }
}

trait MetadataSerializationActorComponent {
  def metadataSerializationActor(): ActorRef
}

trait SerialMetadataSerializationActorComponent extends 
    MetadataSerializationActorComponent with
    ActorSystemComponent with
    CheckpointsComponent with 
    StateComponent {
  override lazy val metadataSerializationActor = {
    system.actorOf(Props(new MetadataSerializationActor(checkpoints, yggState.metadataIO)), "metadata_serializer")
  }
}

trait IngestBatchConsumerComponent {
  def ingestBatchConsumer: BatchConsumer 
}

trait KafkaConsumerConfig {
  def kafkaHost(): String
  def kafkaPort(): Int
  def kafkaTopic(): String 
}

trait KafkaIngestBatchConsumerComponent extends IngestBatchConsumerComponent {

  type YggConfig <: KafkaConsumerConfig

  def yggConfig: YggConfig

  lazy val ingestBatchConsumer = {
    new KafkaBatchConsumer(yggConfig.kafkaHost, yggConfig.kafkaPort, yggConfig.kafkaTopic)
  }
}

trait IngestActorComponent {
  def ingestActor: ActorRef
}

trait KafkaIngestActorComponent extends 
    IngestActorComponent with 
    IngestBatchConsumerComponent with
    CheckpointsComponent with 
    ActorSystemComponent {

  override lazy val ingestActor = {
    system.actorOf(Props(new KafkaShardIngestActor(checkpoints, ingestBatchConsumer)), "shard_ingest")
  }
}

trait NoopIngestActorComponent extends IngestActorComponent with ActorSystemComponent {
  override lazy val ingestActor = {
    system.actorOf(Props(new NoopIngestActor), "noop_ingest")
  }

  class NoopIngestActor extends Actor {
    def receive = {
      case GetMessages(replyTo) =>
        replyTo ! NoMessages
    }
  }
}


trait RoutingTableComponent {
  def routingTable(): RoutingTable
}

trait SingleColumnRoutingTableComponent extends RoutingTableComponent {
  override lazy val routingTable = SingleColumnProjectionRoutingTable
}

trait RoutingActorComponent {
  def routingActor(): ActorRef
}

trait TableDrivenRoutingActorComponent extends 
    RoutingActorComponent with 
    RoutingTableComponent with
    IngestActorComponent with
    ProjectionsActorComponent with
    MetadataActorComponent with
    ActorSystemComponent {
  
  override lazy val routingActor = {
    system.actorOf(Props(new RoutingActor(routingTable, ingestActor, projectionsActor, metadataActor, system.scheduler)), "router")
  }
}

trait ProjectionsActorComponent {
  def projectionsActor(): ActorRef
}

trait CachedProjectionsActorComponent extends
    ProjectionsActorComponent with
    StateComponent with
    ActorSystemComponent {

  override lazy val projectionsActor = {
    system.actorOf(Props(new ProjectionActors(yggState.descriptorLocator, yggState.descriptorIO, system.scheduler)), "projections")
  }
}

trait SystemCoordinationComponent {
  def systemCoordination(): SystemCoordination
}

trait ZookeeperSystemCoordinationConfig {
  def zookeeperHosts(): String
  def zookeeperBase(): Seq[String]
  def zookeeperPrefix(): String
}

trait ZookeeperSystemCoordinationComponent extends 
    SystemCoordinationComponent {

  type YggConfig <: ZookeeperSystemCoordinationConfig
  def yggConfig: YggConfig
  override lazy val systemCoordination = {
    new ZookeeperSystemCoordination(yggConfig.zookeeperHosts, yggConfig.zookeeperBase, yggConfig.zookeeperPrefix) 
  }
}

trait CheckpointsComponent {
  def checkpoints(): YggCheckpoints
}

trait NullCheckpointsComponent extends CheckpointsComponent {
  override lazy val checkpoints = new YggCheckpoints {
    def saveRecoveryPoint(checkpoints: YggCheckpoint) { }
  }
}

trait ShardIdConfig {
  def shardId(): String
}

trait SystemCoordinationCheckpointsComponent extends 
    CheckpointsComponent with 
    SystemCoordinationComponent {
  type YggConfig <: ShardIdConfig
  def yggConfig: YggConfig

  override lazy val checkpoints = new SystemCoordinationYggCheckpoints(yggConfig.shardId, systemCoordination)
}

class NoopActor extends Actor {
  def receive = {
    case _ =>
  }
}
