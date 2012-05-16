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

trait StandaloneActorEcosystem extends BaseActorEcosystem with YggConfigComponent with Logging {
  protected lazy val pre = "[Standalone Yggdrasil Shard]"

  lazy val actorSystem = ActorSystem("standalone_actor_system")

  lazy val routingActor = actorSystem.actorOf(Props(new BatchStoreActor(routingDispatch, yggConfig.batchStoreDelay, None, actorSystem.scheduler, yggConfig.batchShutdownCheckInterval)), "router")
  
  def actorsStatus(): Future[JArray] = Future {
    JArray(List(JString("StandaloneActorEcosystem status not yet implemented.")))
  }

  protected def actorsStopInternal: Future[Unit] = {
    for {
      _  <- actorStop(projectionActors, "projection")
      _  <- actorStop(metadataActor, "metadata")
      _  <- actorStop(metadataSerializationActor, "flush")
    } yield ()
  }
  
  //
  // Internal only actors
  //
  
  protected lazy val checkpoints: YggCheckpoints = new YggCheckpoints {
    def saveRecoveryPoint(checkpoints: YggCheckpoint) { }
  }
}
// vim: set ts=4 sw=4 et:
