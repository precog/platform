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
package kafka

import com.precog.util._
import com.precog.common.util.FixMe._
import leveldb._
import shard._
import Bijection._

import com.precog.common._

import akka.actor.Actor
import akka.actor.Props
import akka.actor.ActorRef
import akka.dispatch.Future
import akka.pattern.ask
import akka.util.Timeout
import akka.util.duration._

import blueeyes.json.JPath
import blueeyes.json.JsonAST._
import blueeyes.persistence.cache.Cache
import blueeyes.persistence.cache.CacheSettings
import blueeyes.persistence.cache.ExpirationPolicy

import com.weiglewilczek.slf4s._

import scala.collection.mutable

import _root_.kafka.consumer._

import java.io.File
import java.io.FileInputStream
import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent.TimeUnit

import scalaz._
import scalaz.syntax.std.booleanV._
import scalaz.syntax.std.optionV._
import scalaz.syntax.validation._
import scalaz.effect._
import scalaz.iteratee.EnumeratorT
import scalaz.MonadPartialOrder._

case object Stop

case class ProjectionActorRequest(descriptor: ProjectionDescriptor)

object RoutingActor {
  private val pathDelimeter = "//"
  private val partDelimeter = "-"

  def toPath(columns: Seq[ColumnDescriptor]): String = {
    columns.map( s => sanitize(s.path.toString)).mkString(pathDelimeter) + pathDelimeter +
    columns.map( s => sanitize(s.selector.toString) + partDelimeter + sanitize(s.valueType.toString)).mkString(partDelimeter)
  }

  def projectionSuffix(descriptor: ProjectionDescriptor): String = ""

  private val whitespace = "//W".r
  def sanitize(s: String) = whitespace.replaceAllIn(s, "_") 

  private implicit val timeout: Timeout = 5 seconds
}

class RoutingActor(metadataActor: ActorRef, routingTable: RoutingTable, descriptorLocator: ProjectionDescriptorLocator, descriptorIO: ProjectionDescriptorIO) extends Actor with Logging {
  import RoutingActor._

  val projectionActors = Cache.concurrent[ProjectionDescriptor, ActorRef](
    CacheSettings(
      expirationPolicy = ExpirationPolicy(None, None, TimeUnit.SECONDS), 
      evict = { 
        (descriptor, actor) => descriptorIO(descriptor).map(_ => actor ! Stop).unsafePerformIO
      }
    )
  )

  // Event processing
  // 
  // - break event into route actions (note it is possible for some data not to be routed and other data to be routed to multiple locations)
  // - send count of route actions to metadata layer for sync accounting
  // - for each route action
  // -- Determine required projection
  // -- Lookup or create the required projection
  // --- This includes creation of initial metadata
  // -- Augment the route action with value based metadata
  // -- Send route action

  private def projectionActor(descriptor: ProjectionDescriptor): ValidationNEL[Throwable, ActorRef] = {
    val actor = projectionActors.get(descriptor).toSuccess(new RuntimeException("No cached actor available."): Throwable).toValidationNel.orElse {
      LevelDBProjection(initDescriptor(descriptor).unsafePerformIO, descriptor).map(p => context.actorOf(Props(new ProjectionActor(p, descriptor))))
    }

    actor.foreach(projectionActors.putIfAbsent(descriptor, _))
    actor
  }

  def initDescriptor(descriptor: ProjectionDescriptor): IO[File] = {
    descriptorLocator(descriptor).flatMap( f => descriptorIO(descriptor).map(_ => f) )
  }

  def receive = {
    case SyncMessage(producerId, syncId, eventIds) => //TODO

    case em @ EventMessage(eventId, _) =>
      val projectionUpdates = routingTable.route(em)

      registerCheckpointExpectation(eventId, projectionUpdates.size)

      for (ProjectionData(descriptor, identities, values, metadata) <- projectionUpdates) {
        projectionActor(descriptor) match {
          case Success(actor) =>
            val fut = actor ? ProjectionInsert(identities, values)
            fut.onComplete { _ => 
              metadataActor ! UpdateMetadata(eventId, descriptor, values, metadata)
            }

          case Failure(errors) => 
            for (t <- errors.list) logger.error("Could not obtain actor for projection: " , t)
        }
      }

    case ProjectionActorRequest(descriptor) =>
      sender ! projectionActor(descriptor)
  }

  def registerCheckpointExpectation(eventId: EventId, count: Int): Unit = metadataActor ! ExpectedEventActions(eventId, count)

  def extractMetadataFor(desc: ProjectionDescriptor, metadata: Set[(ColumnDescriptor, JValue, Set[Metadata])]): Seq[Set[Metadata]] = 
    desc.columns flatMap { c => metadata.find(_._1 == c).map( _._3 ) } toSeq
}
