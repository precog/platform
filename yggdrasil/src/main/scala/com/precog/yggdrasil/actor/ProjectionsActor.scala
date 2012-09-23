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

case class AcquireProjection(descriptor: ProjectionDescriptor, exclusive: Boolean = false, evict: Boolean = false) extends ShardProjectionAction
case class ReleaseProjection(descriptor: ProjectionDescriptor) extends ShardProjectionAction

trait ProjectionUpdate extends ShardProjectionAction {
  def descriptor: ProjectionDescriptor
}

case class ProjectionInsert(descriptor: ProjectionDescriptor, rows: Seq[ProjectionInsert.Row]) extends ProjectionUpdate
object ProjectionInsert {
  case class Row(id: EventId, values: Seq[CValue], metadata: Seq[Set[Metadata]])
}

case class ProjectionArchive(descriptor: ProjectionDescriptor, id: ArchiveId) extends ProjectionUpdate

case class InsertMetadata(descriptor: ProjectionDescriptor, metadata: ColumnMetadata)
case class ArchiveMetadata(descriptor: ProjectionDescriptor)
case object InsertNoMetadata

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

    case class AcquisitionRequest(requestor: ActorRef, exclusive: Boolean, evict: Boolean) 
    case class AcquisitionState(exclusive: Boolean, count: Int, queue: List[AcquisitionRequest])
    
    private val acquisitionState = mutable.Map.empty[ProjectionDescriptor, AcquisitionState] 
    
    // Cache for active projections
    private val projections = Cache.concurrentWithCheckedEviction[ProjectionDescriptor, Projection](projectionCacheSettings) {
      (descriptor, _) => !acquisitionState.isDefinedAt(descriptor)
    }

    def receive = {
      case Status =>
        sender ! status

      case AcquireProjection(descriptor, exclusive, evict) => {
        acquisitionState.get(descriptor) match {
          case None =>
            acquisitionState(descriptor) = AcquisitionState(exclusive, 1, Nil)
            acquired(sender, descriptor, exclusive, evict)
            
          case Some(s @ AcquisitionState(false, count0, Nil)) if !exclusive =>
            acquisitionState(descriptor) = s.copy(count = count0+1)
            acquired(sender, descriptor, exclusive, evict)
            
          case Some(s @ AcquisitionState(_, _, queue0)) =>
            acquisitionState(descriptor) = s.copy(queue = AcquisitionRequest(sender, exclusive, evict) :: queue0)
        }
      }

      case ReleaseProjection(descriptor) => {
        acquisitionState.get(descriptor) match {
          case None =>
            logger.warn("Extraneous request to release projection descriptor " + descriptor + 
                        "; no outstanding acquisitions for this descriptor recorded.")
                        
          case Some(s @ AcquisitionState(_, 1, Nil)) =>
            acquisitionState -= descriptor
          
          case Some(s @ AcquisitionState(_, 1, queue)) =>
            queue.reverse.span(!_.exclusive) match {
              case (Nil, excl :: rest) =>
                acquisitionState(descriptor) = AcquisitionState(true, 1, rest)
                acquired(excl.requestor, descriptor, true, excl.evict)

              case (nonExcl, excl) =>
                acquisitionState(descriptor) = AcquisitionState(false, nonExcl.size, excl)
                nonExcl map { req => acquired(req.requestor, descriptor, false, req.evict) }
            }
            
          case Some(s @ AcquisitionState(_, count0, _)) =>
            acquisitionState(descriptor) = s.copy(count = count0-1)
        }
      }

      case ProjectionInsert(descriptor, rows) => {
        val coordinator = sender
        logger.debug(coordinator + " is inserting into projection " + descriptor)
        
        val insertActor = context.actorOf(Props(new ProjectionInsertActor(rows, coordinator)))
        self.tell(AcquireProjection(descriptor), insertActor)
      }
      
      case ProjectionArchive(descriptor, archive) => {
        val coordinator = sender
        logger.debug(coordinator + " is archiving projection " + descriptor)
        
        val archiveActor = context.actorOf(Props(new ProjectionArchiveActor(coordinator)))
        self.tell(AcquireProjection(descriptor, true, true), archiveActor)
      }
    }

    override def postStop(): Unit = {
      if (projections.size > 0) {
        logger.info("Stopping remaining (cached) projections: " + projections.size)
        projections.foreach { case (_,projection) => Projection.close(projection).except { t: Throwable => logger.error("Error closing " + projection, t); IO(()) }.unsafePerformIO }
      }

      logger.info("Stopped ProjectionsActor")
    }

    protected def status =  JObject(JField("Projections", JObject(JField("cacheSize", JNum(projections.size)) :: 
                                                                  JField("outstandingReferences", JNum(acquisitionState.size)) :: Nil)) :: Nil)
                                                                  
    private def acquired(sender: ActorRef, descriptor: ProjectionDescriptor, exclusive: Boolean, evict: Boolean)(implicit self: ActorRef) : Unit = {
      logger.debug("Acquiring projection for " + descriptor + (if(exclusive) "(exclusive)" else ""))
      cacheLookup(descriptor) map { p =>
        if (evict) {
          logger.debug("Evicting projection for " + descriptor + (if(exclusive) "(exclusive)" else ""))
          assert(exclusive)
          projections -= descriptor
        }
        sender ! ProjectionAcquired(p)
      } except {
        error => IO(sender ! ProjectionError(descriptor, error))
      } unsafePerformIO
    }

    private def cacheLookup(descriptor: ProjectionDescriptor): IO[Projection] = {
      projections.get(descriptor) map { IO(_) } getOrElse {
        for (p <- Projection.open(descriptor)) yield {
          // funkiness due to putIfAbsent semantics of returning Some(v) only if k already exists in the map
          projections.putIfAbsent(descriptor, p) getOrElse p 
        }
      }
    }
  }

  
  /**
   * A short-lived worker actor intended to asynchronously and safely perform
   * an insert on a projection. Replies to the sender of ingest messages when
   * it is done with an insert.
   */
  class ProjectionInsertActor(rows: Seq[ProjectionInsert.Row], replyTo: ActorRef) extends Actor with Logging {
    def receive = {
      case ProjectionAcquired(projection) => { 
        logger.debug("Inserting " + rows.size + " rows into " + projection)
        
        try {
          for(ProjectionInsert.Row(eventId, values, _) <- rows)
            projection.insert(Array(eventId.uid), values).unsafePerformIO
          
          replyTo ! InsertMetadata(projection.descriptor, ProjectionMetadata.columnMetadata(projection.descriptor, rows))
        } catch {
          case t: Throwable => logger.error("Error during insert, aborting batch", t)
        }

        sender ! ReleaseProjection(projection.descriptor)

        self ! PoisonPill
      }
      
      case err: ProjectionError => {
        replyTo ! err
        self ! PoisonPill
      }
    }
  }

  /**
   * A short-lived worker actor intended to asynchronously and safely perform
   * an archival on a projection. Replies to the sender of ingest messages when
   * the archival is complete.
   */
  class ProjectionArchiveActor(replyTo: ActorRef) extends Actor with Logging {
    def receive = {
      case ProjectionAcquired(projection) => { 
        logger.debug("Archiving " + projection)
        
        try {
          Projection.archive(projection.asInstanceOf[Projection]).unsafePerformIO // The cake is a lie!
          
          replyTo ! ArchiveMetadata(projection.descriptor)
        } catch {
          case t: Throwable => logger.error("Error during archive on %s, aborting batch".format(projection), t)
        }

        sender ! ReleaseProjection(projection.descriptor)

        self ! PoisonPill
      }
      
      case err: ProjectionError => {
        replyTo ! err
        self ! PoisonPill
      }
    }
  }
}

// vim: set ts=4 sw=4 et:
