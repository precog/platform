package com.precog.yggdrasil 
package shard 

import com.precog.analytics.Path
import com.precog.common._
import com.precog.common.security._
import com.precog.util._
import com.precog.util.Bijection._
import com.precog.yggdrasil.util.IOUtils
import com.precog.yggdrasil.kafka._
import leveldb._

import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.ActorRef
import akka.actor.PoisonPill
import akka.dispatch.Await
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

import blueeyes.util._
import blueeyes.json.xschema._
import blueeyes.json.xschema.Extractor._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.persistence.cache.Cache
import blueeyes.persistence.cache.CacheSettings
import blueeyes.persistence.cache.ExpirationPolicy

import com.weiglewilczek.slf4s._
import _root_.kafka.consumer._

import java.io.File
import java.io.FileReader
import java.io.PrintWriter
import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent.TimeUnit

import org.scalacheck.Gen._

import com.precog.common._
import com.precog.common.kafka._
import com.precog.common.Event
import com.precog.common.util.RealisticIngestMessage

import scala.collection.mutable
import scala.annotation.tailrec

import scalaz._
import scalaz.std._
import scalaz.std.AllInstances._
import scalaz.syntax._
import scalaz.syntax.validation._
import scalaz.syntax.traverse._
import scalaz.syntax.semigroup._
import scalaz.effect._
import scalaz.iteratee.EnumeratorT
import scalaz.effect._
import scalaz.iteratee._
import scalaz.iteratee.Input._
import scalaz.syntax.plus._
import scalaz.syntax.monad._
import scalaz.syntax.applicativePlus._
import scalaz.syntax.biFunctor
import scalaz.Scalaz._

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
    // Not this is just to 'unlazy' the metadata
    // sync scheduler call
    metadataSyncCancel.isCancelled
    routingActor ! CheckMessages
  }

  def stop: Future[Unit] = {
    val defaultSystem = system
    val defaultTimeout = 300 seconds
    implicit val timeout: Timeout = defaultTimeout

    def actorStop(actor: ActorRef, timeout: Duration = defaultTimeout, system: ActorSystem = defaultSystem) = (_: Any) => 
      gracefulStop(actor, timeout)(system)

    def flushMetadata = (_: Any) => metadataActor ? FlushMetadata(metadataSerializationActor)

    Future { info(pre + "Stopping")(_) } map { _ =>
      debug(pre + "Stopping metadata sync") } map { _ =>
      metadataSyncCancel.cancel } map { _ =>
      debug(pre + "Stopping routing actor") } map { _ =>
      routingActor ? ControlledStop } flatMap
      actorStop(routingActor) recover { case e => error("Error stopping routing actor", e) } map { _ => 
      debug(pre + "Flushing metadata") } flatMap
      flushMetadata recover { case e => error("Error flushing metadata", e) } map { _ => 
      debug(pre + "Stopping ingest actor") } flatMap
      actorStop(ingestActor) recover { case e => ("Error stopping ingest actor") } map { _ => 
      debug(pre + "Stopping projection actors") } flatMap
      actorStop(projectionActors) recover { case e => ("Error stopping projection actors") } map { _ => 
      debug(pre + "Stopping metadata actor") } flatMap
      actorStop(metadataActor) recover { case e => ("Error stopping metadata actor") } map { _ => 
      debug(pre + "Stopping flush actor") } flatMap
      actorStop(metadataSerializationActor) recover { case e => error("Error stopping flush actor") } map { _ => 
      debug(pre + "Stopping actor system") } map
      { _ => system.shutdown } recover { case e => error("Error stopping actor system") } map { _ =>
      info(pre + "Stopped") }
  }

  def store(msg: EventMessage): Future[Unit] = {
    implicit val storeTimeout: Timeout = Duration(10, "seconds")
    (routingActor ? msg) map { _ => () }
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

