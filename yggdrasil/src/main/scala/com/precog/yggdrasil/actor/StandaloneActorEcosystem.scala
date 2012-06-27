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
//import com.precog.common.kafka._

import akka.actor._
import akka.dispatch._
import akka.util._
import akka.util.duration._
import akka.pattern.ask
import akka.pattern.gracefulStop


import com.weiglewilczek.slf4s.Logging

import java.net.InetAddress

import blueeyes.json.JsonAST._

class NoopIngestActor extends Actor {
  def receive = {
    case Status => sender ! JString("Noop ingest actor has no state.")
    case GetMessages(replyTo) => replyTo ! IngestData(Nil)
  }
}

/**
 * FIXME: The standalone actor ecosystem does not support updates to the metadata system.
 * This is why an empty checkpoint is passed in.
 */
abstract class StandaloneActorEcosystem[Dataset[_]] extends BaseActorEcosystem[Dataset] with YggConfigComponent with Logging {
  protected val logPrefix = "[Standalone Yggdrasil Shard]"

  val actorSystem = ActorSystem("standalone_actor_system")

  val ingestActor = None
  
  val checkpointCoordination = CheckpointCoordination.Noop

  val shardId = "standalone"

  protected lazy val actorsWithStatus = ingestSupervisor :: metadataActor :: projectionsActor :: Nil

  protected def actorsStopInternal: Future[Unit] = {
    for {
      _  <- actorStop(projectionsActor, "projection")
      _  <- actorStop(metadataActor, "metadata")
    } yield ()
  }
}
// vim: set ts=4 sw=4 et:
