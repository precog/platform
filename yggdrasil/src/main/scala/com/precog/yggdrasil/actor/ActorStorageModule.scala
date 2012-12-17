package com.precog.yggdrasil 
package actor

import metadata._

import com.precog.common.accounts.AccountFinder
import com.precog.common._
import com.precog.common.ingest._
import com.precog.common.security._

import com.precog.util.PrecogUnit

import akka.actor.{ActorRef,ActorSystem,Props}
import akka.dispatch.{Await,Dispatcher,ExecutionContext,Future,Promise, Futures}
import akka.pattern.ask
import akka.pattern.gracefulStop
import akka.util.Timeout
import akka.util.duration._

import scalaz._
import scalaz.effect._

import com.weiglewilczek.slf4s.Logging

trait ActorStorageModuleConfig {
  def metadataTimeout: Timeout
}

trait ActorStorageModule extends StorageModule[Future] with YggConfigComponent {
  type YggConfig <: ActorStorageModuleConfig

  protected implicit def actorSystem: ActorSystem

  trait ActorStorageLike extends StorageLike with Logging {
    def accessControl: AccessControl[Future]
    def accountManager: AccountFinder[Future]
    def shardSystemActor: ActorRef

    def start(): Future[Boolean]
    def stop(): Future[Boolean]

    implicit val asyncContext = ExecutionContext.defaultExecutionContext(actorSystem)
    implicit val M = blueeyes.bkka.AkkaTypeClasses.futureApplicative(asyncContext)

    private lazy val metadata: StorageMetadata[Future] = new ActorStorageMetadata(shardSystemActor, yggConfig.metadataTimeout)
    
    def userMetadataView(apiKey: APIKey): StorageMetadata[Future] = {
      new UserMetadataView(apiKey, accessControl, metadata)
    }
    
    def projection(descriptor: ProjectionDescriptor): Future[(Projection, Release)] = {
      logger.debug("Obtain projection for " + descriptor)
      implicit val storageTimeout: Timeout = Timeout(300 seconds)


      (for (ProjectionAcquired(projection) <- (shardSystemActor ? AcquireProjection(descriptor, false))) yield {
        logger.debug("  projection obtained")
        (projection.asInstanceOf[Projection], new Release(IO { shardSystemActor ! ReleaseProjection(descriptor); PrecogUnit }))
      }) onFailure {
        case e => logger.error("Error acquiring projection: " + descriptor, e)
      }
    }
    
    def storeBatch(msgs: Seq[IngestMessage]): Future[PrecogUnit] = {
      implicit val storageTimeout: Timeout = Timeout(300 seconds)

      val result = Promise.apply[BatchComplete]
      val notifier = actorSystem.actorOf(Props(new BatchCompleteNotifier(result)))
      val batchHandler = actorSystem.actorOf(Props(new BatchHandler(notifier, null, YggCheckpoint.Empty, Timeout(120000))))
      shardSystemActor.tell(DirectIngestData(msgs), batchHandler)

      for {
        complete <- result
        checkpoint = complete.checkpoint
        _ = logger.debug("Batch store complete: " + complete)
        _ = logger.debug("Sending metadata updates")
      } yield {
        shardSystemActor ! IngestBatchMetadata(complete.updatedProjections, checkpoint.messageClock, Some(checkpoint.offset))
        PrecogUnit
      }
    }
  }
}
