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

case object Stop

class RoutingActor(baseDir: File, routingTable: RoutingTable, metadataActor: ActorRef) extends Actor with Logging {
  def syncDescriptors : IO[List[Validation[Throwable,Unit]]] = {
    projectionActors.keys.foldLeft(IO(List[Validation[Throwable,Unit]]())) { 
      case (io,descriptor) => {
        projectionDirMap.get(descriptor).map { dir =>
          io.flatMap { a => LevelDBProjection.descriptorSync(dir).sync(descriptor).map(_ :: a) }
        } getOrElse {
          io.map(a => (new RuntimeException("Could not locate directory for " + descriptor) : Throwable).fail[Unit] :: a)
        }
      }
    }
  }

  def restoreDescriptors : Map[ProjectionDescriptor,File] = {
    def walk(dir : File, map : Map[ProjectionDescriptor,File]) : Map[ProjectionDescriptor,File] = {
      val updatedMap = LevelDBProjection.descriptorSync(dir).read match {
        case Some(dio) => dio.unsafePerformIO.fold({ t => logger.warn("Failed to restore %s: %s".format(dir, t)); map },
                                                   { pd => map + (pd -> dir) })
        case None      => map
      }

      dir.listFiles.foldLeft(updatedMap) { case (m, f) => if (f.isDirectory) walk(f, m) else m }
    }

    walk(baseDir, Map())
  }

  def dirFor(descriptor : ProjectionDescriptor) : File = {
    projectionDirMap.get(descriptor) match {
      case Some(dir) => dir
      case None => {
        val newDir = new File(baseDir, toPath(descriptor.columns.map(_._1.qsel).toList) + projectionSuffix(descriptor))
        LevelDBProjection.descriptorSync(newDir).sync(descriptor).unsafePerformIO.fold({ t => logger.error("Failed to sync descriptor: " + t) },
                                                                                       { _ => ()})
        projectionDirMap += (descriptor -> newDir)
        newDir
      }
    }
  }

  private val pathDelimeter = "//"
  private val partDelimeter = "-"

  def toPath(columns: Seq[QualifiedSelector]): String = {
    columns.map( s => sanitize(s.path.toString)).mkString(pathDelimeter) + pathDelimeter +
    columns.map( s => sanitize(s.selector.toString) + partDelimeter + sanitize(s.valueType.toString)).mkString(partDelimeter)
  }

  def projectionSuffix(descriptor: ProjectionDescriptor): String = ""

  private val whitespace = "//W".r

  def sanitize(s: String) = whitespace.replaceAllIn(s, "_") 
      
  var projectionDirMap : Map[ProjectionDescriptor,File] = restoreDescriptors

  val projectionActors = Cache.concurrent[ProjectionDescriptor, ActorRef](
    CacheSettings(
      expirationPolicy = ExpirationPolicy(None, None, TimeUnit.SECONDS), 
      evict = { 
        (descriptor, actor) => {
          val sync = LevelDBProjection.descriptorSync(projectionDirMap(descriptor))
          sync.sync(descriptor).map(_ => actor ! Stop).unsafePerformIO
        }
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

  def receive = {
    case SyncMessage(producerId, syncId, eventIds) => //TODO

    case em @ EventMessage(pid, eid, ev @ Event(_, data)) =>
      val unpacked = RoutingTable.unpack(ev)
      val boundMetadata = unpacked filter { 
        case Some(_) => true
        case _       => false
      } map { 
        case Some(x) => x
        case _       => sys.error("Theoretically unreachable code")
      }
      val qualifiedSelectors = boundMetadata.map( t => (t._1.qsel, t._2) )
      val projectionUpdates = routingTable.route(qualifiedSelectors)

      registerCheckpointExpectation(pid, eid, projectionUpdates.size)

      for {
        (descriptor, values) <- projectionUpdates 
      } {
        val comparator = ProjectionComparator.forProjection(descriptor)
        val actor: ValidationNEL[Throwable, ActorRef] = projectionActors.get(descriptor).toSuccess(new RuntimeException("No cached actor available."): Throwable).toValidationNel.orElse {
          sys.error("todo")
          //LevelDBProjection(dirFor(descriptor), Some(comparator)).map(p => context.actorOf(Props(new ProjectionActor(p, descriptor))))
        }

        actor match {
          case Success(actor) =>
            projectionActors.putIfAbsent(descriptor, actor)
            val fut = actor ? ProjectionInsert(em.uid, values)
            fut.onComplete { _ => 
              metadataActor ! UpdateMetadata(pid, eid, descriptor, values, extractMetadataFor(descriptor, boundMetadata))
            }

          case Failure(errors) => 
            for (t <- errors.list) logger.error("Could not obtain actor for projection: " , t)
        }
      }
  }

  def registerCheckpointExpectation(pid: Int, eid: Int, count: Int): Unit = metadataActor ! ExpectedEventActions(pid, eid, count)

  def extractMetadataFor(desc: ProjectionDescriptor, boundMetadata: Set[(ColumnDescriptor, JValue)]): Seq[Set[Metadata]] = 
    desc.columns flatMap { case (c, _) => boundMetadata.exists(_._1 == c).option(c.metadata) } toSeq
}

case class ProjectionInsert(id: Long, values: Seq[JValue])

case class ProjectionGet(idInterval : Interval[Identities], sender : ActorRef)

trait ProjectionResults {
  val desc : ProjectionDescriptor
  def enumerator[A] : EnumeratorT[Unit, Array[Byte], ({type l[a] = IdT[IO, a]})#l, A]
}

class ProjectionActor(projection: LevelDBProjection, descriptor: ProjectionDescriptor) extends Actor {
  //implicit val bijection: Bijection[Seq[JValue], Array[Byte]] = projectionBijection2(descriptor)

  def receive = {
    case Stop => //close the db
      projection.close.unsafePerformIO

    case ProjectionInsert(id, values) => 
      sys.error("todo")
      //projection.insert(id, values.as[Array[Byte]]).unsafePerformIO

    case ProjectionGet(interval, sender) =>
      sender ! new ProjectionResults {
        val desc = descriptor
        def enumerator[A] = projection.getValuesByIdRange[Unit](interval).apply[IdT, A]
      }
  }
}

// vim: set ts=4 sw=4 et:
