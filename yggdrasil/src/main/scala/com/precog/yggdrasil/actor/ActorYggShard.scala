package com.precog.yggdrasil 
package actor

import metadata._

import com.precog.common._
import com.precog.common.security._

import akka.actor.{ActorRef,ActorSystem,Props}
import akka.dispatch.{Await,Dispatcher,ExecutionContext,Future,Promise, Futures}
import akka.pattern.ask
import akka.pattern.gracefulStop
import akka.util.Timeout
import akka.util.duration._

import scalaz.effect._

import com.weiglewilczek.slf4s.Logging

trait ActorYggShard[Dataset] extends YggShard[Dataset] with Logging {
  def accessControl: AccessControl
  protected implicit def actorSystem: ActorSystem
  def shardSystemActor: ActorRef

  def start(): Future[Boolean]
  def stop(): Future[Boolean]

  private lazy implicit val dispatcher = actorSystem.dispatcher
  private lazy val metadata: StorageMetadata = new ActorStorageMetadata(shardSystemActor)
  
  def userMetadataView(uid: String): MetadataView = {
    implicit val executionContext = ExecutionContext.defaultExecutionContext(actorSystem)
    new UserMetadataView(uid, accessControl, metadata)
  }
  
  def projection(descriptor: ProjectionDescriptor, timeout: Timeout): Future[(Projection[Dataset], Release)] = {
    logger.debug("Obtain projection for " + descriptor)
    implicit val ito = timeout 

    (for (ProjectionAcquired(projection) <- (shardSystemActor ? AcquireProjection(descriptor))) yield {
      logger.debug("  projection obtained")
      (projection.asInstanceOf[Projection[Dataset]], new Release(IO(shardSystemActor ! ReleaseProjection(descriptor))))
    }) onFailure {
      case e => logger.error("Error acquiring projection: " + descriptor, e)
    }
  }
  
  def storeBatch(msgs: Seq[EventMessage], timeout: Timeout): Future[Unit] = {
    implicit val ito = timeout
    val result = Promise.apply[BatchComplete]
    val notifier = actorSystem.actorOf(Props(new BatchCompleteNotifier(result)))
    val batchHandler = actorSystem.actorOf(Props(new BatchHandler(notifier, null, YggCheckpoint.Empty, Timeout(120000))))
    shardSystemActor.tell(DirectIngestData(msgs), batchHandler)

    result map { complete =>
      logger.debug("Batch store complete: " + complete)
    }
  }
}


