package com.precog.yggdrasil
package kafka

import com.precog.util._
import leveldb._
import shard._
import Bijection._

import com.precog.common._
import com.precog.common.kafka._
import com.precog.common.util._

import akka.actor.Actor
import akka.actor.Props
import akka.actor.Scheduler
import akka.actor.ActorRef
import akka.dispatch.Future
import akka.pattern.ask
import akka.util.Timeout
import akka.util.duration._
import akka.util.Duration

import blueeyes.json.JPath
import blueeyes.json.JsonAST._
import blueeyes.persistence.cache.Cache
import blueeyes.persistence.cache.CacheSettings
import blueeyes.persistence.cache.ExpirationPolicy

import com.weiglewilczek.slf4s._

import scala.collection.mutable

import _root_.kafka.consumer._
import _root_.kafka.message._

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


case object CheckMessages

case class AcquireProjection(descriptor: ProjectionDescriptor)
case class ReleaseProjection(descriptor: ProjectionDescriptor) 

trait ProjectionResult

case class ProjectionAcquired(proj: ActorRef) extends ProjectionResult
case class ProjectionError(ex: NonEmptyList[Throwable]) extends ProjectionResult

class ProjectionActors(descriptorLocator: ProjectionDescriptorLocator, descriptorIO: ProjectionDescriptorIO, scheduler: Scheduler) extends Actor with Logging {


  def receive = {

    case AcquireProjection(descriptor: ProjectionDescriptor) =>
      val proj = projectionActor(descriptor)
      mark(proj)
      sender ! proj

    case ReleaseProjection(descriptor: ProjectionDescriptor) =>
      unmark(projectionActor(descriptor))

  }

  def mark(result: ProjectionResult): Unit = result match {
    case ProjectionAcquired(proj) => proj ! IncrementRefCount
    case _                        =>
  }

  def unmark(result: ProjectionResult): Unit = result match {
    case ProjectionAcquired(proj) => proj ! DecrementRefCount
    case _                        =>
  }
  
  val projectionActors = Cache.concurrent[ProjectionDescriptor, ActorRef](
    CacheSettings(
      expirationPolicy = ExpirationPolicy(None, None, TimeUnit.SECONDS), 
      evict = { 
        (descriptor, actor) => descriptorIO(descriptor).map(_ => actor ! Stop).unsafePerformIO
      }
    )
  )

  private def projectionActor(descriptor: ProjectionDescriptor): ProjectionResult = {
    import ProjectionActors._
    val actor = projectionActors.get(descriptor).toSuccess(new RuntimeException("No cached actor available."): Throwable).toValidationNel.orElse {
      LevelDBProjection(initDescriptor(descriptor).unsafePerformIO, descriptor).map(p => context.actorOf(Props(new ProjectionActor(p, descriptor, scheduler))))
    }

    actor.foreach(projectionActors.putIfAbsent(descriptor, _))
    actor
  }

  def initDescriptor(descriptor: ProjectionDescriptor): IO[File] = {
    descriptorLocator(descriptor).flatMap( f => descriptorIO(descriptor).map(_ => f) )
  }

}

object ProjectionActors {
  implicit def validationToResult(validation: ValidationNEL[Throwable, ActorRef]): ProjectionResult = validation match {
    case Success(proj) => ProjectionAcquired(proj)
    case Failure(exs) => ProjectionError(exs)
  }
}

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

  private[kafka] implicit val timeout: Timeout = 30 seconds
}

case object ControlledStop
case class AwaitShutdown(replyTo: ActorRef)

class RoutingActor(routingTable: RoutingTable, ingestActor: ActorRef, projectionActors: ActorRef, metadataActor: ActorRef, scheduler: Scheduler) extends Actor with Logging {
  
  import RoutingActor._

  private var inShutdown = false

  def receive = {
    
    case ControlledStop =>
      inShutdown = true 
      self ! AwaitShutdown(sender)

    case as @ AwaitShutdown(replyTo) =>
      if(inserted.isEmpty) {
        logger.debug("Routing actor shutdown - Complete")
        replyTo ! ()
      } else {
        logger.debug("Routing actor shutdown - Pending inserts (%d)".format(inserted.size)) 
        scheduler.scheduleOnce(Duration(1, "second"), self, as)
      }

    case CheckMessages =>
      if(!inShutdown) {
        logger.debug("Routing Actor - Check Messages")
        ingestActor ! GetMessages(self)
      }
    
    case NoMessages =>
      logger.debug("Routing Actor - No Messages")
      scheduleNextCheck
    
    case Messages(messages) =>
      logger.debug("Routing Actor - Processing Message Batch (%d)".format(messages.size))
      processMessages(messages)
      sender ! ()
    
    case ic @ InsertComplete(_, _, _, _) =>
      //logger.debug("Insert Complete")
      markInsertComplete(ic)

  }

  def scheduleNextCheck {
    if(!inShutdown) {
      scheduler.scheduleOnce(Duration(1, "second"), self, CheckMessages)
    }
  }

  def processMessages(messages: Seq[IngestMessage]) {
    messages foreach {
      case SyncMessage(_, _, _) => // TODO

      case em @ EventMessage(eventId, _) =>
        val projectionUpdates = routingTable.route(em)
        
        markInsertsPending(eventId, projectionUpdates.size)  

        for (ProjectionData(descriptor, identities, values, metadata) <- projectionUpdates) {
          val acquire = projectionActors ? AcquireProjection(descriptor)
          acquire.onComplete { 
            case Left(t) =>
              logger.error("Exception acquiring projection actor: ", t)

            case Right(ProjectionAcquired(proj)) => 
              val fut = proj ? ProjectionInsert(identities, values)
              fut.onComplete { _ => 
                self ! InsertComplete(eventId, descriptor, values, metadata)
                projectionActors ! ReleaseProjection(descriptor)
              }
              
            case Right(ProjectionError(errs)) =>
              for(err <- errs.list) logger.error("Error acquiring projection actor: ", err)
          }
        }
    }
  }

  private var expectation = Map[EventId, Int]()
  private var inserted = Map[EventId, List[InsertComplete]]()

  def markInsertsPending(eventId: EventId, expected: Int) { 
    expectation += (eventId -> expected)
  }

  def markInsertComplete(insert: InsertComplete) { 
    val eventId = insert.eventId
    val inserts = inserted.get(eventId) map { _ :+ insert } getOrElse(List(insert))
    if(inserts.size >= expectation(eventId)) {
      expectation -= eventId
      inserted -= eventId
      //logger.debug("Event insert complete: updating metadata")
      metadataActor ! UpdateMetadata(inserts)
    } else {
      inserted += (eventId -> inserts) 
    }
    if(expectation.isEmpty) {
      logger.debug("Batch complete")
      self ! CheckMessages
    }
  }
}

case class InsertComplete(eventId: EventId, descriptor: ProjectionDescriptor, values: Seq[CValue], metadata: Seq[Set[Metadata]])

class KafkaShardIngestActor(checkpoints: YggCheckpoints, consumer: BatchConsumer) extends ShardIngestActor {

  private val bufferSize = 1024 * 1024

  private var lastCheckpoint = checkpoints.latestCheckpoint 

  def readMessages(): Seq[IngestMessage] = {
    val messages = readMessageBatch(lastCheckpoint.offset)

    val (out, clock, offset) = messages.foldLeft( (Vector[IngestMessage](), lastCheckpoint.messageClock, lastCheckpoint.offset ) ) {
      case ((acc, clock, offset), msgAndOffset) => 
        IngestMessageSerialization.read(msgAndOffset.message.payload) match {
          case em @ EventMessage(EventId(pid, sid), _) =>
            (acc :+ em, clock.update(pid, sid), msgAndOffset.offset)
          case sm @ SyncMessage(_, _, _) =>
            (acc :+ sm, clock, msgAndOffset.offset)
        }
    }

    recordCheckpoint(YggCheckpoint(offset, clock))

    out
  }

  def recordCheckpoint(newCheckpoint: YggCheckpoint) {
    checkpoints.messagesConsumed(newCheckpoint)
    lastCheckpoint = newCheckpoint
  }

  def readMessageBatch(offset: Long): Seq[MessageAndOffset] = {
    consumer.ingestBatch(offset, bufferSize)
  }
  
  override def postStop() {
    consumer.close
  }
}

trait ShardIngestActor extends Actor with Logging {

  def receive = {
    case GetMessages(replyTo) => 
      logger.debug("Ingest Actor - Read Batch")
      try {
        val messages = getMessages
        replyTo ! messages 
      } catch {
        case e => 
          logger.error("Error get message batch from kafka.", e) 
          replyTo ! NoMessages
      }
  }

  def getMessages(): MessageResponse = readMessages 

  def readMessages(): Seq[IngestMessage]

}

case class GetMessages(sendTo: ActorRef)

trait MessageResponse

case object NoMessages extends MessageResponse
case class Messages(messages: Seq[IngestMessage]) extends MessageResponse

object MessageResponse {
  implicit def seqToMessageResponse(messages: Seq[IngestMessage]): MessageResponse =
    if(messages.isEmpty) NoMessages else Messages(messages)

}
