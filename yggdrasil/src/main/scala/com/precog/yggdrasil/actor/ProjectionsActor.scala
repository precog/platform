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
package actor

import leveldb._
import metadata._
import com.precog.common._

import akka.actor.Actor
import akka.actor.Props
import akka.actor.Scheduler
import akka.actor.ActorRef
import akka.actor.PoisonPill
import akka.pattern.ask
import akka.util.Timeout

import blueeyes.json.JsonAST._
import blueeyes.persistence.cache.Cache
import blueeyes.persistence.cache.CacheSettings
import blueeyes.persistence.cache.ExpirationPolicy

import com.weiglewilczek.slf4s._

import java.io.File
import java.util.concurrent.TimeUnit

import scala.annotation.tailrec
import scala.collection.mutable

import scalaz._
import scalaz.Validation._
import scalaz.effect._
import scalaz.syntax.std.option._

//////////////
// MESSAGES //
//////////////

case class AcquireProjection(descriptor: ProjectionDescriptor)
case class ReleaseProjection(descriptor: ProjectionDescriptor) 

case class ProjectionInsert(descriptor: ProjectionDescriptor, rows: Seq[ProjectionInsert.Row])
object ProjectionInsert {
  case class Row(id: EventId, values: Seq[CValue], metadata: Seq[Set[Metadata]])
}

case class BatchInsert(rows: Seq[ProjectionInsert.Row], replyTo: ActorRef)
case class InsertMetadata(descriptor: ProjectionDescriptor, metadata: ColumnMetadata)

trait ProjectionsActorModule[Dataset[_]] {
  // projection retrieval result messages
  trait ProjectionResult
  case class ProjectionAcquired(projection: Projection[Dataset]) extends ProjectionResult
  case class ProjectionError(descriptor: ProjectionDescriptor, error: Throwable) extends ProjectionResult

  ////////////
  // ACTORS //
  ////////////

  def newProjectionsActor(metadataActor: ActorRef, metadataTimeout: Timeout): ProjectionsActor

  /**
   * The responsibilities of
   */
  abstract class ProjectionsActor(metadataActor: ActorRef, metadataTimeout: Timeout) extends Actor with Logging { self =>
    implicit val metadataTO = metadataTimeout

    def receive = {
      case Status =>
        sender ! status

      // Increment the outstanding reference count for the specified descriptor
      // and return the reference if available.
      case AcquireProjection(descriptor) =>
        logger.debug("Acquiring projection for " + descriptor)
        val mySender = sender
        for (dir <- (metadataActor ? FindDescriptorRoot(descriptor, false)).onFailure { case e => logger.error("Error finding descriptor root for " + descriptor, e) }) {
          projection(dir.asInstanceOf[Option[File]], descriptor) match {
            case Success(p) =>
              reserved(p.descriptor)
            mySender ! ProjectionAcquired(p)
            
            case Failure(error) =>
              mySender ! ProjectionError(descriptor, error)
          }
        } 
      
      // Decrement the outstanding reference count for the specified descriptor
      case ReleaseProjection(descriptor) =>
        logger.debug("Releasing projection for " + descriptor)
        released(descriptor)
      
      case ProjectionInsert(descriptor, inserts) =>
        val coordinator = sender
        logger.debug(coordinator + " is inserting into projection for " + descriptor)
        for (dir <- (metadataActor ? FindDescriptorRoot(descriptor, true)).onFailure { case e => logger.error("Error finding descriptor root for " + descriptor, e) }) {
          projection(dir.asInstanceOf[Option[File]], descriptor) match {
            case Success(p) =>
              logger.debug("Reserving " + descriptor + " in " + dir)
              reserved(p.descriptor)
              // Spawn a new short-lived insert actor for the projection and send a batch insert
              // request to it so that any IO involved doesn't block queries from obtaining projection
              // references. This could alternately be a single actor, but this design conforms more closely
              // to 'error kernel' or 'let it crash' style.
              context.actorOf(Props(new ProjectionInsertActor(p))) ! BatchInsert(inserts, coordinator)

            case Failure(error) => 
              logger.error("Could not load projection: " + error)
              coordinator ! ProjectionError(descriptor, error)
          }
        }
    }

    protected def status: JValue

    protected def projection(base: Option[File], descriptor: ProjectionDescriptor): Validation[Throwable, Projection[Dataset]]

    protected def reserved(descriptor: ProjectionDescriptor): Unit 

    protected def released(descriptor: ProjectionDescriptor): Unit

    @inline @tailrec private def releaseAll(descriptors: Iterator[ProjectionDescriptor]): Unit = {
      if (descriptors.hasNext) {
        released(descriptors.next())
        releaseAll(descriptors)
      }
    }
  }

  /**
   * A short-lived worker actor intended to asynchronously and safely perform
   * an insert on a projection. Replies to the sender of ingest messages when
   * it is done with an insert.
   */
  class ProjectionInsertActor(projection: Projection[Dataset]) extends Actor with Logging {
    override def preStart(): Unit = {
      logger.debug("Preparing for insert on " + projection)
    }
    
    import ProjectionInsert.Row

    def receive = {
      case BatchInsert(rows, replyTo) =>
        logger.debug("Inserting " + rows.size)
        insertAll(rows)
        sender  ! ReleaseProjection(projection.descriptor)
        logger.debug("Notifying coordinator")
        replyTo ! InsertMetadata(projection.descriptor, ProjectionMetadata.columnMetadata(projection.descriptor, rows))
        self    ! PoisonPill
    }

    private def insertAll(batch: Seq[Row]): Unit = {
      @tailrec def step(iter: Iterator[Row]) {
        if (iter.hasNext) {
          val Row(eventId, values, _) = iter.next
          projection.insert(Vector1(eventId.uid), values).unsafePerformIO
          step(iter)
        }
      }

      step(batch.iterator)
    }
  }
}



// vim: set ts=4 sw=4 et:
