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

import com.precog.analytics.Path
import com.precog.common._
import com.precog.util._
import com.precog.util.Bijection._
import com.precog.yggdrasil.util.IOUtils
import kafka._
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
  def metadata: StorageMetadata
  def projection(descriptor: ProjectionDescriptor)(implicit timeout: Timeout): Future[Projection]
  def store(msg: EventMessage): Future[Unit]
}

trait ActorYggShard extends YggShard with Logging {
  val pre = "[Yggdrasil Shard]"

  def yggState: YggState

  lazy implicit val system = ActorSystem("storage_shard")
  lazy implicit val executionContext = ExecutionContext.defaultExecutionContext
  lazy implicit val dispatcher = system.dispatcher

  lazy val routingTable: RoutingTable = SingleColumnProjectionRoutingTable 
  lazy val routingActor: ActorRef = system.actorOf(Props(new RoutingActor(metadataActor, routingTable, yggState.descriptorLocator, yggState.descriptorIO)), "router")
  lazy val metadataActor: ActorRef = system.actorOf(Props(new ShardMetadataActor(yggState.metadata, yggState.checkpoints)), "metadata")
  lazy val metadata: StorageMetadata = new ShardMetadata(metadataActor, dispatcher) 

  import logger._

  def start: Future[Unit] = {
    Future(())
  }

  def stop: Future[Unit] = {
    val metadataSerializationActor: ActorRef = system.actorOf(Props(new MetadataSerializationActor(yggState.metadataIO, yggState.checkpointIO)), "metadata_serializer")

    val defaultSystem = system
    val defaultTimeout = 300 seconds
    implicit val timeout: Timeout = defaultTimeout

    def actorStop(actor: ActorRef, timeout: Duration = defaultTimeout, system: ActorSystem = defaultSystem) = (_: Any) =>
      gracefulStop(actor, timeout)(system)

    def flushMetadata = (_: Any) => metadataActor ? FlushMetadata(metadataSerializationActor)
    def flushCheckpoints = (_: Any) => metadataActor ? FlushCheckpoints(metadataSerializationActor)

    Future { info(pre + "Stopping")(_) } map { _ => 
      debug(pre + "Stopping routing actor") } flatMap
      actorStop(routingActor) recover { case e => error("Error stopping routing actor", e) } map { _ => 
      debug(pre + "Flushing metadata") } flatMap
      flushMetadata recover { case e => error("Error flushing metadata", e) } map { _ => 
      debug(pre + "Flushing checkpoints") } flatMap
      flushCheckpoints recover { case e => error("Error flushing checkpoints") } map { _ => 
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
    (routingActor ? ProjectionActorRequest(descriptor)).mapTo[ValidationNEL[Throwable, ActorRef]] flatMap {
      case Success(actorRef) => (actorRef ? ProjectionGet).mapTo[Projection]
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

