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

// To simplify routing, tag all messages for the ProjectionLike actor
sealed trait ShardProjectionAction

case class AcquireProjection(descriptor: ProjectionDescriptor) extends ShardProjectionAction
case class ReleaseProjection(descriptor: ProjectionDescriptor) extends ShardProjectionAction

case class ProjectionInsert(descriptor: ProjectionDescriptor, rows: Seq[ProjectionInsert.Row]) extends ShardProjectionAction
object ProjectionInsert {
  case class Row(id: EventId, values: Seq[CValue], metadata: Seq[Set[Metadata]])
}

case class BatchInsert(rows: Seq[ProjectionInsert.Row], replyTo: ActorRef)
case class InsertMetadata(descriptor: ProjectionDescriptor, metadata: ColumnMetadata)

// projection retrieval result messages
sealed trait ProjectionResult
case class ProjectionAcquired(projection: ProjectionLike) extends ProjectionResult
case class ProjectionError(descriptor: ProjectionDescriptor, error: Throwable) extends ProjectionResult

trait ProjectionsActorModule extends ProjectionModule {
  ////////////
  // ACTORS //
  ////////////

  /**
   * The responsibilities of
   */
  class ProjectionsActor extends Actor with Logging { self =>
    private val projectionCacheSettings = CacheSettings(
      expirationPolicy = ExpirationPolicy(Some(2), Some(2), TimeUnit.MINUTES), 
      evict = (descriptor: ProjectionDescriptor, projection: Projection) => Projection.close(projection).unsafePerformIO
    )

    // Cache control map that stores reference counts for projection descriptors managed by the cache
    private val outstandingReferences = mutable.Map.empty[ProjectionDescriptor, Int]

    // Cache for active projections
    private val projections = Cache.concurrentWithCheckedEviction[ProjectionDescriptor, Projection](projectionCacheSettings) {
      (descriptor, _) => outstandingReferences.get(descriptor) forall { _ == 0 }
    }

    def receive = {
      case Status =>
        sender ! status

      // Increment the outstanding reference count for the specified descriptor
      // and return the reference if available.
      case AcquireProjection(descriptor) =>
        logger.debug("Acquiring projection for " + descriptor)
        val mySender = sender

        cacheLookup(descriptor) map { p =>
          reserved(p.descriptor)
          mySender ! ProjectionAcquired(p)
        } except {
          error => IO(mySender ! ProjectionError(descriptor, error))
        } unsafePerformIO
      
      // Decrement the outstanding reference count for the specified descriptor
      case ReleaseProjection(descriptor) =>
        logger.debug("Releasing projection for " + descriptor)
        released(descriptor)
      
      case ProjectionInsert(descriptor, inserts) =>
        val coordinator = sender
        logger.debug(coordinator + " is inserting into projection for " + descriptor)

        cacheLookup(descriptor) map { p =>
          logger.debug("Reserving " + descriptor)
          reserved(p.descriptor)
          // Spawn a new short-lived insert actor for the projection and send a batch insert
          // request to it so that any IO involved doesn't block queries from obtaining projection
          // references. This could alternately be a single actor, but this design conforms more closely
          // to 'error kernel' or 'let it crash' style.
          context.actorOf(Props(new ProjectionInsertActor(p))) ! BatchInsert(inserts, coordinator)
        } except {
          error => IO(coordinator ! ProjectionError(descriptor, error))
        } unsafePerformIO
    }

    override def postStop(): Unit = {
      if (projections.size > 0) {
        logger.info("Stopping remaining (cached) projections: " + projections.size)
        projections.foreach { case (_,projection) => Projection.close(projection).except { t: Throwable => logger.error("Error closing " + projection, t); IO(()) }.unsafePerformIO }
      }

      logger.info("Stopped ProjectionsActor")
    }

    protected def status =  JObject(JField("Projections", JObject(JField("cacheSize", JNum(projections.size)) :: 
                                                                  JField("outstandingReferences", JNum(outstandingReferences.size)) :: Nil)) :: Nil)

    private def cacheLookup(descriptor: ProjectionDescriptor): IO[Projection] = {
      projections.get(descriptor) map { IO(_) } getOrElse {
        for (p <- Projection.open(descriptor)) yield {
          // funkiness due to putIfAbsent semantics of returning Some(v) only if k already exists in the map
          projections.putIfAbsent(descriptor, p) getOrElse p 
        }
      }
    }

    private def reserved(descriptor: ProjectionDescriptor): Unit = {
      val current = outstandingReferences.getOrElse(descriptor, 0)
      outstandingReferences += (descriptor -> (current + 1))
    }

    private def released(descriptor: ProjectionDescriptor): Unit = {
      outstandingReferences.get(descriptor) match {
        case Some(current) if current > 1 => 
          outstandingReferences += (descriptor -> (current - 1))

        case Some(current) if current == 1 => 
          outstandingReferences -= descriptor

        case None =>
          logger.warn("Extraneous request to release reference to projection descriptor " + descriptor + 
                      "; no outstanding references for this descriptor recorded.")
      }
    }

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
  class ProjectionInsertActor(projection: Projection) extends Actor with Logging {
    override def preStart(): Unit = {
      logger.debug("Preparing for insert on " + projection)
    }
    
    import ProjectionInsert.Row

    def receive = {
      case BatchInsert(rows, replyTo) =>
        logger.debug("Inserting " + rows.size + " rows into " + projection)
        val insertOK = insertAll(rows)
        sender  ! ReleaseProjection(projection.descriptor)

        if (insertOK) {
          // Notify the coordinator of the completeion of the insert of this projection batch,
          // along with a patch for the associated metadata. This patch will be combined with the
          // other patches for the *ingest* batch (the block of messages retrieved from kafka)
          // and the result will be sent on to the metadata actor when the batch is complete.
          replyTo ! InsertMetadata(projection.descriptor, ProjectionMetadata.columnMetadata(projection.descriptor, rows))
        }

        self    ! PoisonPill
    }

    private def insertAll(batch: Seq[Row]): Boolean = {
      try {
        @tailrec def step(iter: Iterator[Row]) {
          if (iter.hasNext) {
            val Row(eventId, values, _) = iter.next
            projection.insert(Vector1(eventId.uid), values).unsafePerformIO
            step(iter)
          }
        }

        step(batch.iterator)
        true
      } catch {
        case t: Throwable => logger.error("Error during insert, aborting batch", t); false
      }
    }
  }
}



// vim: set ts=4 sw=4 et:
