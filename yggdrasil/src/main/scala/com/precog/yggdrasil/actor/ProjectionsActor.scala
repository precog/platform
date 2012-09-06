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

case class AcquireProjection(descriptor: ProjectionDescriptor, exclusive: Boolean = false) extends ShardProjectionAction
case class ReleaseProjection(descriptor: ProjectionDescriptor) extends ShardProjectionAction

object ProjectionUpdate {
  sealed trait UpdateAction
  case class Row(id: EventId, values: Seq[CValue], metadata: Seq[Set[Metadata]]) extends UpdateAction
  case class Archive(id: ArchiveId) extends UpdateAction
}
import ProjectionUpdate._

case class ProjectionUpdate(descriptor: ProjectionDescriptor, rows: Seq[UpdateAction]) extends ShardProjectionAction

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

    case class AcquisitionRequest(requestor: ActorRef, exclusive: Boolean) 
    case class AcquisitionState(exclusive: Boolean, count: Int, queue: List[AcquisitionRequest])
    
    private val acquisitionState = mutable.Map.empty[ProjectionDescriptor, AcquisitionState] 
    
    // Cache for active projections
    private val projections = Cache.concurrentWithCheckedEviction[ProjectionDescriptor, Projection](projectionCacheSettings) {
      (descriptor, _) => !acquisitionState.isDefinedAt(descriptor)
    }

    def receive = {
      case Status =>
        sender ! status

      case AcquireProjection(descriptor, exclusive) => {
        logger.debug("Attempting to acquiring projection for " + descriptor + (if(exclusive) "(exclusive)" else ""))
        
        acquisitionState.get(descriptor) match {
          case None =>
            acquisitionState(descriptor) = AcquisitionState(exclusive, 1, Nil)
            acquired(sender, descriptor, exclusive)
            
          case Some(s @ AcquisitionState(false, count0, Nil)) if !exclusive =>
            acquisitionState(descriptor) = s.copy(count = count0+1)
            acquired(sender, descriptor, exclusive)
            
          case Some(s @ AcquisitionState(_, _, queue0)) =>
            acquisitionState(descriptor) = s.copy(queue = AcquisitionRequest(sender, exclusive) :: queue0)
        }
      }

      case ReleaseProjection(descriptor) => {
        logger.debug("Releasing projection for " + descriptor)
        
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
                acquired(excl.requestor, descriptor, true)

              case (nonExcl, excl) =>
                acquisitionState(descriptor) = AcquisitionState(false, nonExcl.size, excl)
                nonExcl map { req => acquired(req.requestor, descriptor, false) }
            }
            
          case Some(s @ AcquisitionState(_, count0, _)) =>
            acquisitionState(descriptor) = s.copy(count = count0-1)
        }
      }

      case ProjectionUpdate(descriptor, actions) => {
        val coordinator = sender
        logger.debug(coordinator + " is updating projection for " + descriptor)
        
        @tailrec
        def dispatch(actions: Seq[UpdateAction]) : Unit = {
          val (revRows, revArchives) = actions.foldLeft((List.empty[Row], List.empty[UpdateAction])) {
            case ((rows, archives), row : Row) => (row :: rows, archives)
            case ((rows, archives), archive : ProjectionUpdate.Archive) => (rows, archive :: archives)
          }
          (revRows.reverse, revArchives.reverse) match {
            case (Nil, Nil) =>
            case (Nil, archival :: rest) =>
              val archiveActor = context.actorOf(Props(new ProjectionArchiveActor(coordinator)))
              self.tell(AcquireProjection(descriptor, true), archiveActor)
              dispatch(rest)
            case (insertions, rest) =>
              val insertActor = context.actorOf(Props(new ProjectionInsertActor(insertions, coordinator)))
              self.tell(AcquireProjection(descriptor), insertActor)
              dispatch(rest)
          }
        }
        
        dispatch(actions)
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
                                                                  
    private def acquired(sender: ActorRef, descriptor: ProjectionDescriptor, exclusive: Boolean) : Unit = {
      logger.debug("Acquiring projection for " + descriptor + (if(exclusive) "(exclusive)" else ""))
      cacheLookup(descriptor) map { p =>
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
  class ProjectionInsertActor(rows: Seq[Row], replyTo: ActorRef) extends Actor with Logging {
    def receive = {
      case ProjectionAcquired(projection) => { 
        logger.debug("Inserting " + rows.size + " rows into " + projection)
        
        try {
          for(Row(eventId, values, _) <- rows)
            projection.insert(Vector1(eventId.uid), values).unsafePerformIO
          
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
}

// vim: set ts=4 sw=4 et:
