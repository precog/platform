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
