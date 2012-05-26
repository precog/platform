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

trait ActorYggShard[Dataset[_]] extends YggShard[Dataset] with ActorEcosystem with ProjectionsActorModule[Dataset] {
  def accessControl: AccessControl

  private lazy implicit val dispatcher = actorSystem.dispatcher
  private lazy val metadata: StorageMetadata = new ActorStorageMetadata(metadataActor)
  
  def userMetadataView(uid: String): MetadataView = {
    implicit val executionContext = ExecutionContext.defaultExecutionContext(actorSystem)
    new UserMetadataView(uid, accessControl, metadata)
  }
  
  def projection(descriptor: ProjectionDescriptor, timeout: Timeout): Future[(Projection[Dataset], Release)] = {
    implicit val ito = timeout 

    for (ProjectionAcquired(projection) <- (projectionsActor ? AcquireProjection(descriptor))) yield {
      (projection, new Release(IO(projectionsActor ! ReleaseProjection(descriptor))))
    }
  }
  
  def storeBatch(msgs: Seq[EventMessage], timeout: Timeout): Future[Unit] = {
    implicit val ito = timeout
    (ingestSupervisor ? DirectIngestData(msgs)).mapTo[Unit]
  }
}

