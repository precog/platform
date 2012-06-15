package com.precog.yggdrasil
package actor

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

import java.net.InetAddress

trait ProductionActorConfig extends ActorEcosystemConfig {
  def kafkaHost: String = config[String]("kafka.batch.host")
  def kafkaPort: Int = config[Int]("kafka.batch.port")
  def kafkaTopic: String = config[String]("kafka.batch.topic") 
  def kafkaSocketTimeout: Duration = config[Long]("kafka.socket_timeout", 5000) millis
  def kafkaBufferSize: Int = config[Int]("kafka.buffer_size", 64 * 1024)

  def zookeeperHosts: String = config[String]("zookeeper.hosts")
  def zookeeperBase: List[String] = config[List[String]]("zookeeper.basepath")
  def zookeeperPrefix: String = config[String]("zookeeper.prefix")   

  def ingestEnabled: Boolean = config[Boolean]("ingest_enabled", true)

  def serviceUID: ServiceUID = ZookeeperSystemCoordination.extractServiceUID(config)
}

/**
 * The production actor ecosystem includes a real ingest actor which will read from the kafka queue.
 * At present, there's a bit of a problem around metadata serialization as the standalone actor
 * will not include manually ingested data in serialized metadata; this needs to be addressed.
 */
trait ProductionActorEcosystem[Dataset[_]] extends BaseActorEcosystem[Dataset] with YggConfigComponent with Logging {
  type YggConfig <: ProductionActorConfig

  protected val logPrefix = "[Production Yggdrasil Shard]"

  val actorSystem = ActorSystem("production_actor_system")

  val shardId: String = yggConfig.serviceUID.hostId + yggConfig.serviceUID.serviceId 

  lazy val checkpointCoordination = ZookeeperSystemCoordination(yggConfig.zookeeperHosts, yggConfig.serviceUID, yggConfig.ingestEnabled) 

  protected def actorsWithStatus = ingestActor.toList ++
                                   ingestSupervisor.toList ++
                                   (metadataActor :: projectionsActor :: Nil)
  lazy val ingestActor = {
    logger.info("Starting ingest actor")
    implicit val timeout = Timeout(45000l)
    // We can't have both actors trying to lock the ZK element or we race, so we just delegate to the metadataActor
    logger.debug("Requesting checkpoint from metadata")
    Await.result((metadataActor ? GetCurrentCheckpoint).mapTo[Option[YggCheckpoint]], 45 seconds) map { checkpoint =>
      logger.debug("Checkpoint load complete")
      val consumer = new SimpleConsumer(yggConfig.kafkaHost, yggConfig.kafkaPort, yggConfig.kafkaSocketTimeout.toMillis.toInt, yggConfig.kafkaBufferSize)
      actorSystem.actorOf(Props(new KafkaShardIngestActor(shardId, checkpoint, metadataActor, consumer, yggConfig.kafkaTopic, yggConfig.ingestEnabled)), "shard_ingest")
    }
  }

  //
  // Internal only actors
  //
  
  private val metadataSyncCancel = 
    actorSystem.scheduler.schedule(yggConfig.metadataSyncPeriod, yggConfig.metadataSyncPeriod, metadataActor, FlushMetadata)

  protected def actorsStopInternal: Future[Unit] = {
    import yggConfig.stopTimeout

    metadataSyncCancel.cancel
    for {
      _  <- ingestActor.map(actorStop(_, "ingest")).getOrElse(Future(()))
      _  <- actorStop(projectionsActor, "projection")
      _  <- actorStop(metadataActor, "metadata")
    } yield ()
  }
}

// vim: set ts=4 sw=4 et:
