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

import org.slf4j._

import java.io.File
import java.util.concurrent.TimeUnit

import scala.annotation.tailrec
import scala.collection.mutable

import scalaz._
import scalaz.Validation._
import scalaz.effect._
import scalaz.std.list._
import scalaz.syntax.show._
import scalaz.syntax.std.option._
import scalaz.syntax.traverse._

//////////////
// MESSAGES //
//////////////

// To simplify routing, tag all messages for the ProjectionLike actor
sealed trait ShardProjectionAction

case class AcquireProjection(descriptor: ProjectionDescriptor, lockForArchive: Boolean) extends ShardProjectionAction
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

case class LockedForArchive(descriptor: ProjectionDescriptor) 

trait ProjectionsActorModule extends ProjectionModule {
  ////////////
  // ACTORS //
  ////////////

  /**
   * The responsibilities of
   */
  class ProjectionsActor extends Actor { self =>
    private lazy val logger = LoggerFactory.getLogger("com.precog.yggdrasil.actor.ProjectionsActor")

    case class AcquisitionRequest(requestor: ActorRef, lockForArchive: Boolean) 
    case class AcquisitionState(archiveLock: Boolean, count: Int, queue: List[AcquisitionRequest])
    
    private val acquisitionState = mutable.Map.empty[ProjectionDescriptor, AcquisitionState] 
    
    def receive = {
      case Status =>
        sender ! status

      case AcquireProjection(descriptor, lockForArchive) => 
        val io = acquisitionState.get(descriptor) match {
          case None =>
            logger.debug("Fresh acquisition of " + descriptor.shows + (if(lockForArchive) " (archive)" else ""))
            acquisitionState(descriptor) = AcquisitionState(lockForArchive, 1, Nil)
            if (lockForArchive) archive(sender, descriptor) else acquired(sender, descriptor)
            
          case Some(s @ AcquisitionState(false, count0, Nil)) if !lockForArchive =>
            logger.debug("Non-exclusive acquisition (%d prior) of %s".format(count0, descriptor.shows))
            acquisitionState(descriptor) = s.copy(count = count0+1)
            acquired(sender, descriptor)
            
          case Some(s @ AcquisitionState(_, _, queue0)) =>
            logger.warn("Queueing due to exclusive lock on " + descriptor.shows)
            IO(acquisitionState(descriptor) = s.copy(queue = AcquisitionRequest(sender, lockForArchive) :: queue0))
        } 

        io.unsafePerformIO

      case ReleaseProjection(descriptor) => 
        val io: IO[Any] = acquisitionState.get(descriptor) match {
          case None =>
            IO {
              logger.warn("Extraneous request to release projection descriptor " + descriptor.shows + 
                          "; no outstanding acquisitions for this descriptor recorded.")
            }
                        
          case Some(s @ AcquisitionState(_, 1, Nil)) =>
            IO {
              logger.debug("Release of last acquisition of " + descriptor.shows)
              acquisitionState -= descriptor
            }
          
          case Some(s @ AcquisitionState(_, 1, queue)) =>
            queue.reverse.span(!_.lockForArchive) match {
              case (Nil, excl :: rest) =>
                logger.debug("Dequeued exclusive archive lock request for " + descriptor.shows + " after release")
                acquisitionState(descriptor) = AcquisitionState(true, 1, rest)
                archive(excl.requestor, descriptor)

              case (nonExcl, excl) =>
                logger.debug("Dequeued non-exclusive acquisition of " + descriptor.shows + " after release")
                acquisitionState(descriptor) = AcquisitionState(false, nonExcl.size, excl)
                (nonExcl map { req => acquired(req.requestor, descriptor) }).sequence[IO, Unit]
            }
            
          case Some(s @ AcquisitionState(_, count0, _)) =>
            IO {
              logger.debug("Non-exclusive release of %s, count now %d".format(descriptor.shows, count0 - 1))
              acquisitionState(descriptor) = s.copy(count = count0-1)
            }
        }

        io.unsafePerformIO

      case ProjectionInsert(descriptor, rows) => 
        val coordinator = sender
        logger.trace(coordinator + " is inserting into projection " + descriptor.shows)
        
        val insertActor = context.actorOf(Props(new ProjectionInsertActor(rows, coordinator)))
        self.tell(AcquireProjection(descriptor, false), insertActor)
      
      case ProjectionArchive(descriptor, archive) => 
        val coordinator = sender
        logger.info(coordinator + " is archiving projection " + descriptor.shows)
        
        val archiveActor = context.actorOf(Props(new ProjectionArchiveActor(coordinator)))
        self.tell(AcquireProjection(descriptor, true), archiveActor)
    }

    override def postStop(): Unit = {
      if (openProjections.size > 0) {
        logger.info("Stopping remaining (cached) projections: " + openProjections.size)
        openProjections.foreach { case (_,(projection,_)) => Projection.close(projection).except { t: Throwable => logger.error("Error closing " + projection, t); IO(()) }.unsafePerformIO }
      }

      logger.info("Stopped ProjectionsActor")
    }

    protected def status =  JObject(JField("Projections", JObject(JField("cacheSize", JNum(openProjections.size)) :: 
                                                                  JField("outstandingReferences", JNum(acquisitionState.size)) :: Nil)) :: Nil)

    val maxOpenProjections = 1000
    private val openProjections = mutable.Map.empty[ProjectionDescriptor, (Projection, Long)]
                             
    private def evictProjection(descriptor: ProjectionDescriptor, projection: Projection): IO[Unit] = {
      logger.debug("Evicting " + descriptor.shows)
      openProjections -= descriptor
      Projection.close(projection)
    }

    private def archive(sender: ActorRef, descriptor: ProjectionDescriptor)(implicit self: ActorRef): IO[Unit] = {
      IO(openProjections.get(descriptor)) flatMap {
        case Some((projection, _)) =>
          for {
            _ <- evictProjection(descriptor, projection)
          } yield {
            sender ! LockedForArchive(descriptor)
          }

        case None => 
          IO {
            sender ! LockedForArchive(descriptor)
          }
      }
    }

    private def evictExcessProjections: IO[Unit] = {
      val overage = openProjections.size - maxOpenProjections
      if (overage >= 0) {
        logger.debug("Evicting " + overage + " projections")
        // attempt to "evict" some unused projections to get us back under the limit
        val (retained, unused) = openProjections.partition { case (d, _) => acquisitionState.contains(d) }
        val toEvict = unused.toList.sortBy(_._2._2).take(overage + 1)

        if ((openProjections.size - toEvict.size) > maxOpenProjections) {
          logger.warn("Unable to evict sufficient projections to get under open projection limit (currently %d)".format(openProjections.size))
        }

        toEvict.map({ case (d, (p, _)) => evictProjection(d, p) }).sequence.map(_ => ())
      } else {
        IO(())
      }
    }

    private def acquired(sender: ActorRef, descriptor: ProjectionDescriptor)(implicit self: ActorRef) : IO[Unit] = {
      IO(openProjections.get(descriptor)) flatMap { 
        case Some((projection, _)) => 
          IO { 
            openProjections += (descriptor -> (projection, System.currentTimeMillis)) 
            projection
          }
          
        case None => 
          for {
            _ <- evictExcessProjections
            p <- Projection.open(descriptor)
            _ <- IO { openProjections += (descriptor -> (p, System.currentTimeMillis)) }
          } yield p
      } map { p =>
        sender ! ProjectionAcquired(p)
      } except {
        error => IO(sender ! ProjectionError(descriptor, error))
      }
    }
  }

  
  /**
   * A short-lived worker actor intended to asynchronously and safely perform
   * an insert on a projection. Replies to the sender of ingest messages when
   * it is done with an insert.
   */
  class ProjectionInsertActor(rows: Seq[ProjectionInsert.Row], replyTo: ActorRef) extends Actor {
    private val logger = LoggerFactory.getLogger("com.precog.yggdrasil.actor.ProjectionInsertActor")

    @tailrec
    private def runInsert(projection: ProjectionLike, rows: Seq[ProjectionInsert.Row]) {
      if (rows.nonEmpty) {
        val row = rows.head
        projection.insert(Array(row.id.uid), row.values)
        runInsert(projection, rows.tail)
      }
    }

    def receive = {
      case ProjectionAcquired(projection) => { 
        logger.debug("Inserting " + rows.size + " rows into " + projection)
        
        val startTime = System.currentTimeMillis

        val insertRun: IO[Unit] = for {
          _ <- IO { MDC.put("projection", projection.descriptor.shows) }
          _ <- IO { runInsert(projection, rows) }
          _ <- projection.commit()
        } yield {
          logger.debug("Insertion of %d rows in %d ms".format(rows.size, System.currentTimeMillis - startTime))          
          replyTo ! InsertMetadata(projection.descriptor, ProjectionMetadata.columnMetadata(projection.descriptor, rows))
        }

        insertRun.except {
          t: Throwable => IO { logger.error("Error during insert, aborting batch", t) }
        }.ensuring {
          IO { MDC.clear() }
        }.unsafePerformIO

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
  class ProjectionArchiveActor(replyTo: ActorRef) extends Actor {
    private val logger = LoggerFactory.getLogger("com.precog.yggdrasil.actor.ProjectionArchiveActor")

    def receive = {
      case LockedForArchive(descriptor) => { 
        logger.debug("Archiving " + descriptor)
        
        try {
          Projection.archive(descriptor).unsafePerformIO 
          replyTo ! ArchiveMetadata(descriptor)
        } catch {
          case t: Throwable => logger.error("Error during archive on %s".format(descriptor), t)
        }

        sender ! ReleaseProjection(descriptor)

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
