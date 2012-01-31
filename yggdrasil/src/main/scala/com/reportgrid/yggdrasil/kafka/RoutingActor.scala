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
import akka.pattern.ask
import akka.util.Timeout
import akka.util.duration._

import blueeyes.json.JPath
import blueeyes.json.JsonAST._
import blueeyes.persistence.cache.Cache
import blueeyes.persistence.cache.CacheSettings
import blueeyes.persistence.cache.ExpirationPolicy

import com.weiglewilczek.slf4s._

import _root_.kafka.consumer._

import java.io.File
import java.io.FileInputStream
import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent.TimeUnit

import scala.collection._

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

class RoutingActor(baseDir: File, descriptors: mutable.Map[ProjectionDescriptor, File], routingTable: RoutingTable, metadataActor: ActorRef) extends Actor with Logging {
  import RoutingActor._

  val projectionActors = Cache.concurrent[ProjectionDescriptor, ActorRef](
    CacheSettings(
      expirationPolicy = ExpirationPolicy(None, None, TimeUnit.SECONDS), 
      evict = { 
        (descriptor, actor) => {
          val sync = LevelDBProjection.descriptorSync(descriptors(descriptor))
          sync.sync(descriptor).map(_ => actor ! Stop).unsafePerformIO
        }
      }
    )
  )

  def syncDescriptors : IO[List[Validation[Throwable,Unit]]] = {
    projectionActors.keys.foldLeft(IO(List[Validation[Throwable,Unit]]())) { 
      case (io,descriptor) => {
        descriptors.get(descriptor).map { dir =>
          io.flatMap { a => LevelDBProjection.descriptorSync(dir).sync(descriptor).map(_ :: a) }
        } getOrElse {
          io.map(a => (new RuntimeException("Could not locate directory for " + descriptor) : Throwable).fail[Unit] :: a)
        }
      }
    }
  }

  def dirFor(descriptor : ProjectionDescriptor) : File = {
    descriptors.get(descriptor) match {
      case Some(dir) => dir
      case None => {
        val newDir = File.createTempFile("col", "", baseDir)
        newDir.delete
        newDir.mkdirs
        logger.debug(newDir.toString)
        LevelDBProjection.descriptorSync(newDir).sync(descriptor).unsafePerformIO.fold({ t => logger.error("Failed to sync descriptor: " + t) },
                                                                                       { _ => ()})
        descriptors += (descriptor -> newDir)
        newDir
      }
    }
  }

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
      LevelDBProjection(dirFor(descriptor), descriptor).map(p => context.actorOf(Props(new ProjectionActor(p, descriptor))))
    }

    actor.foreach(projectionActors.putIfAbsent(descriptor, _))
    actor
  }

  def receive = {
    case SyncMessage(producerId, syncId, eventIds) => //TODO

    case em @ EventMessage(pid, eid, ev @ Event(_, data)) =>
      val projectionUpdates = routingTable.route(RoutingTable.unpack(ev).flatten)

      registerCheckpointExpectation(pid, eid, projectionUpdates.size)

      for ((descriptor, values) <- projectionUpdates) {
        projectionActor(descriptor) match {
          case Success(actor) =>
            val fut = actor ? ProjectionInsert(em.uid, values)
            fut.onComplete { _ => 
              fixme("Ignoring metadata update for now, but need to come back.")
              //metadataActor ! UpdateMetadata(pid, eid, descriptor, values, extractMetadataFor(descriptor, boundMetadata))
            }

          case Failure(errors) => 
            for (t <- errors.list) logger.error("Could not obtain actor for projection: " , t)
        }
      }

    case ProjectionActorRequest(descriptor) =>
      sender ! projectionActor(descriptor)
  }

  def registerCheckpointExpectation(pid: Int, eid: Int, count: Int): Unit = metadataActor ! ExpectedEventActions(pid, eid, count)

//  def extractMetadataFor(desc: ProjectionDescriptor, metadata: Set[(ColumnDescriptor, JValue)]): Seq[Set[Metadata]] = 
//    desc.columns flatMap { c => metadata.exists(_ == c).option(c.metadata) } toSeq
}

// vim: set ts=4 sw=4 et:
