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
package shard 

import com.precog.common._
import com.precog.common.security._
import com.precog.common.kafka._
import com.precog.yggdrasil.kafka._

import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.ActorRef
import akka.actor.PoisonPill
import akka.dispatch.Future
import akka.dispatch.Promise
import akka.dispatch.ExecutionContext
import akka.pattern.ask
import akka.util.Timeout
import akka.util.duration._
import akka.util.Duration
import akka.actor.Terminated
import akka.actor.ReceiveTimeout
import akka.actor.ActorTimeoutException

import com.weiglewilczek.slf4s._

trait YggShard {
  def userMetadataView(uid: String): MetadataView
  def projection(descriptor: ProjectionDescriptor)(implicit timeout: Timeout): Future[Projection]
  def store(msg: EventMessage): Future[Unit]
}

trait YggShardComponent {
  type Storage <: YggShard
  def storage: Storage
}

trait ActorYggShard extends YggShard with Logging {
  val pre = "[Yggdrasil Shard]"

  def yggState: YggState
  def yggCheckpoints: YggCheckpoints
  def batchConsumer: BatchConsumer

  lazy implicit val system = ActorSystem("storage_shard")
  lazy implicit val executionContext = ExecutionContext.defaultExecutionContext
  lazy implicit val dispatcher = system.dispatcher

  lazy val ingestActor: ActorRef = system.actorOf(Props(new KafkaShardIngestActor(yggCheckpoints, batchConsumer)), "shard_ingest")

  lazy val routingTable: RoutingTable = SingleColumnProjectionRoutingTable
  lazy val routingActor: ActorRef = system.actorOf(Props(new RoutingActor(routingTable, ingestActor, projectionActors, ingestActor, system.scheduler)), "router")

  lazy val initialClock = yggCheckpoints.latestCheckpoint.messageClock

  lazy val projectionActors: ActorRef = system.actorOf(Props(new ProjectionActors(yggState.descriptorLocator, yggState.descriptorIO, system.scheduler)), "projections")

  lazy val metadataActor: ActorRef = system.actorOf(Props(new ShardMetadataActor(yggState.metadata, initialClock)), "metadata")
  lazy val metadata: StorageMetadata = new ShardMetadata(metadataActor)
  def userMetadataView(uid: String): MetadataView = new UserMetadataView(uid, UnlimitedAccessControl, metadata)
  
  lazy val metadataSerializationActor: ActorRef = system.actorOf(Props(new MetadataSerializationActor(yggCheckpoints, yggState.metadataIO)), "metadata_serializer")

  val metadataSyncPeriod = Duration(1, "minutes")
  
  lazy val metadataSyncCancel = system.scheduler.schedule(metadataSyncPeriod, metadataSyncPeriod, metadataActor, FlushMetadata(metadataSerializationActor))

  import logger._

  def start: Future[Unit] = Future {  
    // Note this is just to 'unlazy' the metadata
    // sync scheduler call
    this.metadataSyncCancel
    routingActor ! CheckMessages
  }

  def stop: Future[Unit] = {
    val defaultSystem = system
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
      _  <- actorStop(projectionActors, "projection")
      _  <- actorStop(metadataActor, "metadata")
      _  <- actorStop(metadataSerializationActor, "flush")
      _  <- Future {
              debug(pre + "Stopping actor system")
              system.shutdown 
              info(pre + "Stopped")
            } recover { 
              case e => error("Error stopping actor system", e)
            }
    } yield ()
  }

  def store(msg: EventMessage): Future[Unit] = {
    implicit val storeTimeout: Timeout = Duration(10, "seconds")
    val messages = Messages(Vector(msg))
    (routingActor ? messages) map { _ => () }
  }
  
  def projection(descriptor: ProjectionDescriptor)(implicit timeout: Timeout): Future[Projection] = {
    (projectionActors ? AcquireProjection(descriptor)) flatMap {
      case ProjectionAcquired(actorRef) =>
        projectionActors ! ReleaseProjection(descriptor)
        (actorRef ? ProjectionGet).mapTo[Projection]
      
      case ProjectionError(err) =>
        sys.error("Error acquiring projection actor: " + err)
    }
  }

  def gracefulStop(target: ActorRef, timeout: Duration)(implicit system: ActorSystem): Future[Boolean] = {
    if (target.isTerminated) {
      Promise.successful(true)
    } else {
      val result = Promise[Boolean]()
      system.actorOf(Props(new Actor {
        // Terminated will be received when target has been stopped
        context watch target
        
        target ! PoisonPill
        // ReceiveTimeout will be received if nothing else is received within the timeout
        context setReceiveTimeout timeout

        def receive = {
          case Terminated(a) if a == target ⇒
            result success true
            context stop self
          case ReceiveTimeout ⇒
            result failure new ActorTimeoutException(
              "Failed to stop [%s] within [%s]".format(target.path, context.receiveTimeout))
            context stop self
        }
      }))
      result
    }
  }
}

