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
