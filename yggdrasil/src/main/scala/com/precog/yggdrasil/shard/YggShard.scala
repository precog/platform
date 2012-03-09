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

trait YggShardComponent {
  type Storage <: YggShard
  def storage: Storage
}

trait YggShard {
  def userMetadataView(uid: String): MetadataView
  def projection(descriptor: ProjectionDescriptor, timeout: Timeout): Future[Projection]
  def store(msg: EventMessage, timeout: Timeout): Future[Unit] = storeBatch(Vector(msg), timeout) 
  def storeBatch(msgs: Seq[EventMessage], timeout: Timeout): Future[Unit]
}

trait ActorYggShard extends YggShard with ActorEcosystem {
  
  def yggState: YggState

  lazy implicit val dispatcher = actorSystem.dispatcher

  private val metadata: StorageMetadata = new ShardMetadata(metadataActor)
  
  def userMetadataView(uid: String): MetadataView = new UserMetadataView(uid, UnlimitedAccessControl, metadata)
  
  def projection(descriptor: ProjectionDescriptor, timeout: Timeout): Future[Projection] = {
    implicit val ito = timeout 
    (projectionsActor ? AcquireProjection(descriptor)) flatMap {
      case ProjectionAcquired(actorRef) =>
        projectionsActor ! ReleaseProjection(descriptor)
        (actorRef ? ProjectionGet).mapTo[Projection]
      
      case ProjectionError(err) =>
        sys.error("Error acquiring projection actor: " + err)
    }
  }
  
  def storeBatch(msgs: Seq[EventMessage], timeout: Timeout): Future[Unit] = {
    implicit val ito = timeout
    (routingActor ? Messages(msgs)) map { _ => () }
  }
  
}

