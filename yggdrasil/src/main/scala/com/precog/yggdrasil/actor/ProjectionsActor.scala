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
import scalaz.syntax.show._
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
  class ProjectionsActor extends Actor { self =>
    private lazy val logger = LoggerFactory.getLogger("com.precog.yggdrasil.actor.ProjectionsActor")

    private val projectionCacheSettings = CacheSettings(
      expirationPolicy = ExpirationPolicy(Some(2), Some(2), TimeUnit.MINUTES), 
      evict = { (descriptor: ProjectionDescriptor, projection: Projection) => 
        Projection.close(projection).except {
          case t: Throwable => logger.error("Error during projection eviction", t); IO(())
        }.unsafePerformIO
      }
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
            logger.debug("Fresh acquisition of " + descriptor.shows + (if(exclusive) " (exclusive)" else ""))
            acquisitionState(descriptor) = AcquisitionState(exclusive, 1, Nil)
            acquired(sender, descriptor, exclusive, evict)
            
          case Some(s @ AcquisitionState(false, count0, Nil)) if !exclusive =>
            logger.debug("Non-exclusive acquisition (%d prior) of %s".format(count0, descriptor.shows))
            acquisitionState(descriptor) = s.copy(count = count0+1)
            acquired(sender, descriptor, exclusive, evict)
            
          case Some(s @ AcquisitionState(_, _, queue0)) =>
            logger.warn("Queueing exclusive acquisition of " + descriptor.shows)
            acquisitionState(descriptor) = s.copy(queue = AcquisitionRequest(sender, exclusive, evict) :: queue0)
        }
      }

      case ReleaseProjection(descriptor) => {
        acquisitionState.get(descriptor) match {
          case None =>
            logger.warn("Extraneous request to release projection descriptor " + descriptor + 
                        "; no outstanding acquisitions for this descriptor recorded.")
                        
          case Some(s @ AcquisitionState(_, 1, Nil)) =>
            logger.debug("Release of last acquisition of " + descriptor.shows)
            acquisitionState -= descriptor
          
          case Some(s @ AcquisitionState(_, 1, queue)) =>
            queue.reverse.span(!_.exclusive) match {
              case (Nil, excl :: rest) =>
                logger.debug("Dequeued exclusive acquisition of " + descriptor.shows + " after release")
                acquisitionState(descriptor) = AcquisitionState(true, 1, rest)
                acquired(excl.requestor, descriptor, true, excl.evict)

              case (nonExcl, excl) =>
                logger.debug("Dequeued non-exclusive acquisition of " + descriptor.shows + " after release")
                acquisitionState(descriptor) = AcquisitionState(false, nonExcl.size, excl)
                nonExcl map { req => acquired(req.requestor, descriptor, false, req.evict) }
            }
            
          case Some(s @ AcquisitionState(_, count0, _)) =>
            logger.debug("Non-exclusive release of %s, count now %d".format(descriptor.shows, count0 - 1))
            acquisitionState(descriptor) = s.copy(count = count0-1)
        }
      }

      case ProjectionInsert(descriptor, rows) => {
        val coordinator = sender
        logger.trace(coordinator + " is inserting into projection " + descriptor)
        
        val insertActor = context.actorOf(Props(new ProjectionInsertActor(rows, coordinator)))
        self.tell(AcquireProjection(descriptor), insertActor)
      }
      
      case ProjectionArchive(descriptor, archive) => {
        val coordinator = sender
        logger.trace(coordinator + " is archiving projection " + descriptor)
        
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
  class ProjectionInsertActor(rows: Seq[ProjectionInsert.Row], replyTo: ActorRef) extends Actor {
    private val logger = LoggerFactory.getLogger("com.precog.yggdrasil.actor.ProjectionInsertActor")

    def receive = {
      case ProjectionAcquired(projection) => { 
        logger.debug("Inserting " + rows.size + " rows into " + projection)
        
        try {
          MDC.put("projection", projection.descriptor.shows)
          val startTime = System.currentTimeMillis
          for(ProjectionInsert.Row(eventId, values, _) <- rows)
            projection.insert(Array(eventId.uid), values).unsafePerformIO
          logger.debug("Insertion of %d rows in %d ms".format(rows.size, System.currentTimeMillis - startTime))
          MDC.clear()
          
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
  class ProjectionArchiveActor(replyTo: ActorRef) extends Actor {
    private val logger = LoggerFactory.getLogger("com.precog.yggdrasil.actor.ProjectionArchiveActor")

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
