package com.precog.yggdrasil
package actor

import leveldb._
import metadata._
import com.precog.common._

import akka.actor.Actor
import akka.actor.Props
import akka.actor.Scheduler
import akka.actor.ActorRef

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
import scalaz.syntax.std.optionV._

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

  def newProjectionsActor(descriptorLocator: ProjectionDescriptorLocator, descriptorIO: ProjectionDescriptorIO): ProjectionsActor

  /**
   * The responsibilities of
   */
  abstract class ProjectionsActor(descriptorLocator: ProjectionDescriptorLocator, descriptorIO: ProjectionDescriptorIO) 
  extends Actor with Logging { self =>

    def receive = {
      case Status =>
        sender ! status

      // Increment the outstanding reference count for the specified descriptor
      // and return the reference if available.
      case AcquireProjection(descriptor) =>
        projection(descriptor) match {
          case Success(p) =>
            reserved(p.descriptor)
            sender ! ProjectionAcquired(p)

          case Failure(error) =>
            sender ! ProjectionError(descriptor, error)
        }
      
      // Decrement the outstanding reference count for the specified descriptor
      case ReleaseProjection(descriptor) =>
        released(descriptor)
      
      case ProjectionInsert(descriptor, inserts) =>
        projection(descriptor) match {
          case Success(p) =>
            reserved(p.descriptor)
            // Spawn a new short-lived insert actor for the projection and send a batch insert
            // request to it so that any IO involved doesn't block queries from obtaining projection
            // references. This could alternately be a single actor, but this design conforms more closely
            // to 'error kernel' or 'let it crash' style.
            context.actorOf(Props(new ProjectionInsertActor(p))) ! BatchInsert(inserts, sender)

          case Failure(error) => 
            sender ! ProjectionError(descriptor, error)
        }
    }

    protected def status: JValue

    protected def projection(descriptor: ProjectionDescriptor): Validation[Throwable, Projection[Dataset]]

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
  class ProjectionInsertActor(projection: Projection[Dataset]) extends Actor {
    import ProjectionInsert.Row

    def receive = {
      case BatchInsert(rows, replyTo) =>
        insertAll(rows)
        sender  ! ReleaseProjection(projection.descriptor)
        replyTo ! InsertMetadata(projection.descriptor, ProjectionMetadata.columnMetadata(projection.descriptor, rows))
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
