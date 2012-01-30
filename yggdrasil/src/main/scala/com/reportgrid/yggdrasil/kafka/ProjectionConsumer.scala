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
package com.reportgrid.yggdrasil
package kafka

import com.reportgrid.util._
import com.reportgrid.common.util.FixMe._
import leveldb._
import shard._
import Bijection._

import com.reportgrid.common._

import akka.actor.Actor
import akka.actor.Props
import akka.actor.ActorRef
import akka.util.Timeout
import akka.util.duration._

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

class RoutingActor(metadataActor: ActorRef, routingTable: RoutingTable, descriptorLocator: ProjectionDescriptorLocator, descriptorIO: ProjectionDescriptorIO) extends Actor with Logging {

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

  private implicit val timeout: Timeout = 5 seconds

  def initDescriptor(descriptor: ProjectionDescriptor): IO[File] = {
    descriptorLocator(descriptor).flatMap( f => descriptorIO(descriptor).map(_ => f) )
  }

  def receive = {
    case SyncMessage(producerId, syncId, eventIds) => //TODO

    case em @ EventMessage(pid, eid, ev @ Event(_, data)) =>
      val unpacked = RoutingTable.unpack(ev)
      val qualifiedSelectors = unpacked collect { case Some(x) => x }
      val projectionUpdates = routingTable.route(qualifiedSelectors.map(x => (x._1, x._2)))

      registerCheckpointExpectation(pid, eid, projectionUpdates.size)

      for {
        (descriptor, values) <- projectionUpdates 
      } {
        val comparator = ProjectionComparator.forProjection(descriptor)
        val dataDir = descriptorLocator(descriptor).unsafePerformIO
        val actor: ValidationNEL[Throwable, ActorRef] = projectionActors.get(descriptor).toSuccess(new RuntimeException("No cached actor available."): Throwable).toValidationNel.orElse {
          LevelDBProjection(initDescriptor(descriptor).unsafePerformIO, descriptor, Some(comparator)).map(p => context.actorOf(Props(new ProjectionActor(p, descriptor))))
        }

        actor match {
          case Success(actor) =>
            projectionActors.putIfAbsent(descriptor, actor)
            val fut = actor ? ProjectionInsert(em.uid, values)
            fut.onComplete { _ => 
              metadataActor ! UpdateMetadata(pid, eid, descriptor, values, extractMetadataFor(descriptor, qualifiedSelectors))
            }

          case Failure(errors) => 
            for (t <- errors.list) logger.error("Could not obtain actor for projection: " , t)
        }
     }
  }

  def registerCheckpointExpectation(pid: Int, eid: Int, count: Int): Unit = metadataActor ! ExpectedEventActions(pid, eid, count)

  def extractMetadataFor(desc: ProjectionDescriptor, metadata: Set[(ColumnDescriptor, JValue, Set[Metadata])]): Seq[Set[Metadata]] = 
    desc.columns flatMap { c => metadata.find(_._1 == c).map( _._3 ) } toSeq
}

case class ProjectionInsert(id: Long, values: Seq[JValue])

case class ProjectionGet(idInterval : Interval[Identities], sender : ActorRef)

trait ProjectionResults {
  val desc : ProjectionDescriptor
  def enumerator : EnumeratorT[Unit, Seq[CValue], IO]
}

class ProjectionActor(projection: LevelDBProjection, descriptor: ProjectionDescriptor) extends Actor {

  def asCValue(jval: JValue): CValue = jval match { 
    case JString(s) => CString(s)
    case JInt(i) => CNum(BigDecimal(i))
    case JDouble(d) => CDouble(d)
    case JBool(b) => CBoolean(b)
    case JNull => CNull
    case x       => sys.error("JValue type not yet supported: " + x.getClass.getName )
  }

  def receive = {
    case Stop => //close the db
      projection.close.unsafePerformIO

    case ProjectionInsert(id, values) => 
      projection.insert(Vector(id), values.map(asCValue)).unsafePerformIO
      sender ! ()

    case ProjectionGet(interval, sender) =>
      sender ! new ProjectionResults {
        val desc = descriptor
        def enumerator = projection.getValuesByIdRange[Unit](interval).apply[IO]
      }
  }
}

// vim: set ts=4 sw=4 et:
