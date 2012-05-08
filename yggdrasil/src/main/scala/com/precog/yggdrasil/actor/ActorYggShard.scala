package com.precog.yggdrasil 
package actor

import metadata._

import com.precog.common._
import com.precog.common.security._

import akka.dispatch.ExecutionContext
import akka.dispatch.Future
import akka.pattern.ask
import akka.util.Timeout
import scalaz.effect._

trait ActorYggShard[Dataset] extends YggShard[Dataset] with ActorEcosystem {
  def yggState: YggState
  def accessControl: AccessControl

  private lazy implicit val dispatcher = actorSystem.dispatcher
  private lazy val metadata: StorageMetadata = new ActorStorageMetadata(metadataActor)
  
  def userMetadataView(uid: String): MetadataView = {
    implicit val executionContext = ExecutionContext.defaultExecutionContext(actorSystem)
    new UserMetadataView(uid, accessControl, metadata)
  }
  
  def projection(descriptor: ProjectionDescriptor, timeout: Timeout): Future[(Projection[Dataset], Release)] = {
    implicit val ito = timeout 
    (projectionActors ? AcquireProjection(descriptor)) flatMap {
      case ProjectionAcquired(actorRef) =>
        val release = new Release(IO(projectionActors ! ReleaseProjection(descriptor)))

        (actorRef ? ProjectionGet).map(p => (p.asInstanceOf[Projection[Dataset]], release))
      
      case ProjectionError(err) =>
        sys.error("Error acquiring projection actor: " + err)
    }
  }
  
  def storeBatch(msgs: Seq[EventMessage], timeout: Timeout): Future[Unit] = {
    implicit val ito = timeout
    (routingActor ? DirectIngestData(msgs)) map { _ => () }
  }
}

