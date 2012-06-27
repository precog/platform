package com.precog.yggdrasil
package actor

import com.precog.util._
import com.precog.common.YggCheckpoint
import com.precog.common.CheckpointCoordination
import com.precog.common.kafka._

import akka.actor._
import akka.dispatch._
import akka.util._
import akka.util.duration._
import akka.pattern.ask
import akka.pattern.gracefulStop


import com.weiglewilczek.slf4s.Logging

import java.net.InetAddress

import blueeyes.json.JsonAST._

import _root_.kafka.consumer._

abstract class ImportActorEcosystem[Dataset[_]] extends BaseActorEcosystem[Dataset] with YggConfigComponent with Logging {
  type YggConfig <: ProductionActorConfig
  
  protected val logPrefix = "[Import Yggdrasil Shard]"

  val actorSystem = ActorSystem("standalone_actor_system")

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
  
  val checkpointCoordination = CheckpointCoordination.Noop

  val shardId = "standalone"

  protected lazy val actorsWithStatus = ingestSupervisor.toList ++
                                        (metadataActor :: projectionsActor :: Nil)

  protected def actorsStopInternal: Future[Unit] = {
    for {
      _  <- actorStop(projectionsActor, "projection")
      _  <- actorStop(metadataActor, "metadata")
    } yield ()
  }
}
// vim: set ts=4 sw=4 et:
