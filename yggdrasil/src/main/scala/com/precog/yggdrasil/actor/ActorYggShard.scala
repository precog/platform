package com.precog.yggdrasil 
package actor

import metadata._

import com.precog.common._
import com.precog.common.security._

import akka.actor.Props
import akka.dispatch.ExecutionContext
import akka.dispatch.{Await,Future,Promise, Futures}
import akka.pattern.ask
import akka.util.Timeout
import akka.util.duration._

import scalaz.effect._

import com.weiglewilczek.slf4s.Logging

trait ActorYggShard[Dataset[_]] extends YggShard[Dataset] with ActorEcosystem with ProjectionsActorModule[Dataset] with Logging {
  def accessControl: AccessControl

  private lazy implicit val dispatcher = actorSystem.dispatcher
  private lazy val metadata: StorageMetadata = new ActorStorageMetadata(metadataActor)
  
  def userMetadataView(uid: String): MetadataView = {
    implicit val executionContext = ExecutionContext.defaultExecutionContext(actorSystem)
    new UserMetadataView(uid, accessControl, metadata)
  }
  
  def projection(descriptor: ProjectionDescriptor, timeout: Timeout): Future[(Projection[Dataset], Release)] = {
    logger.debug("Obtain projection for " + descriptor)
    implicit val ito = timeout 

    (for (ProjectionAcquired(projection) <- (projectionsActor ? AcquireProjection(descriptor))) yield {
      logger.debug("  projection obtained")
      (projection, new Release(IO(projectionsActor ! ReleaseProjection(descriptor))))
    }) onFailure {
      case e => logger.error("Error acquiring projection: " + descriptor, e)
    }
  }
  
  def storeBatch(msgs: Seq[EventMessage], timeout: Timeout): Future[Unit] = {
    implicit val ito = timeout
    val pollActor = actorSystem.actorOf(Props[PollBatchActor])
    val batchHandler = actorSystem.actorOf(Props(new BatchHandler(pollActor, null, YggCheckpoint.Empty, Timeout(120000))))
    ingestSupervisor map { ingestSupervisor =>
      ingestSupervisor.tell(DirectIngestData(msgs), batchHandler)

      // Poll until we get a result
      while (true) {
        Await.result(pollActor ? PollBatch, 1 second) match {
          case None => // NOOP, keep waiting
          case _    => return Future(()) // Done
        }
      }

      Future(()) // Done
    } getOrElse {
      Futures.failed[Unit](new IllegalStateException("No ingest subsystem present"), implicitly[ExecutionContext])
    }
  }
}

