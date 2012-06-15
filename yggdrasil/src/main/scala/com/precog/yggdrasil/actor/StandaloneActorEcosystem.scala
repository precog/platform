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
