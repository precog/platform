package com.precog.yggdrasil 
package actor

import metadata._

import com.precog.common._
import com.precog.common.security._

import akka.dispatch.ExecutionContext
import akka.dispatch.Future
import akka.pattern.ask
import akka.util.Timeout

trait ActorYggShard extends YggShard with ActorEcosystem {
  
  def yggState: YggState

  lazy implicit val dispatcher = actorSystem.dispatcher

  private lazy val metadata: StorageMetadata = new ShardMetadata(metadataActor)
  
  def userMetadataView(uid: String): MetadataView = {
    implicit val executionContext = ExecutionContext.defaultExecutionContext(actorSystem)
    new UserMetadataView(uid, new UnlimitedAccessControl, metadata)
  }
  
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

