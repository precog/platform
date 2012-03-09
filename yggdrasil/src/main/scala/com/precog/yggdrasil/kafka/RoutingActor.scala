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

import scala.collection.mutable.ListBuffer

case object CheckMessages

case class AcquireProjection(descriptor: ProjectionDescriptor)
case class AcquireProjectionBatch(descriptors: Iterable[ProjectionDescriptor])
case class ReleaseProjection(descriptor: ProjectionDescriptor) 
case class ReleaseProjectionBatch(descriptors: Array[ProjectionDescriptor]) 

trait ProjectionResult

case class ProjectionAcquired(proj: ActorRef) extends ProjectionResult
case class ProjectionBatchAcquired(projs: Map[ProjectionDescriptor, ActorRef]) extends ProjectionResult
case class ProjectionError(ex: NonEmptyList[Throwable]) extends ProjectionResult

class ProjectionActors(descriptorLocator: ProjectionDescriptorLocator, descriptorIO: ProjectionDescriptorIO, scheduler: Scheduler) extends Actor with Logging {

  def receive = {

    case AcquireProjection(descriptor: ProjectionDescriptor) =>
      val proj = projectionActor(descriptor)
      mark(proj)
      sender ! proj
    
    case AcquireProjectionBatch(descriptors) =>
      var result = Map.empty[ProjectionDescriptor, ActorRef] 
      var errors = ListBuffer.empty[ProjectionError]
      
      val descItr = descriptors.iterator
     
      while(descItr.hasNext && errors.size == 0) {
        val desc = descItr.next
        val proj = projectionActor(desc)
        mark(proj)
        proj match {
          case ProjectionAcquired(proj) => result += (desc -> proj)
          case pe @ ProjectionError(_)  => errors += pe
        }
      }

      if(errors.size == 0) {
        sender ! ProjectionBatchAcquired(result) 
      } else {
        sender ! errors(0)
      }

    case ReleaseProjection(descriptor: ProjectionDescriptor) =>
      unmark(projectionActor(descriptor))
    
    case ReleaseProjectionBatch(descriptors: Array[ProjectionDescriptor]) =>
      var cnt = 0
      while(cnt < descriptors.length) {
        unmark(projectionActor(descriptors(cnt)))
        cnt += 1
      }
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

class RoutingActor(routingTable: RoutingTable, ingestActor: Option[ActorRef], projectionActors: ActorRef, metadataActor: ActorRef, scheduler: Scheduler, shutdownCheck: Duration = Duration(1, "second")) extends Actor with Logging {
  
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
        scheduler.scheduleOnce(shutdownCheck, self, as)
      }

    case CheckMessages =>
      if(!inShutdown) {
        //logger.debug("Routing Actor - Check Messages")
        ingestActor foreach { actor => actor ! GetMessages(self) }
      }
    
    case NoMessages =>
      //logger.debug("Routing Actor - No Messages")
      scheduleNextCheck
    
    case Messages(messages) =>
      //logger.debug("Routing Actor - Processing Message Batch (%d)".format(messages.size))
      batchProcessMessages(messages)
      sender ! ()
    
    case ic @ InsertComplete(_, _, _, _) =>
      //logger.debug("Insert Complete")
      markInsertComplete(ic)
    
    case ibc @ InsertBatchComplete(inserts) =>
      var i = 0
      while(i < inserts.size) {
        markInsertComplete(inserts(i))
        i += 1
      }

  }

  def scheduleNextCheck {
    if(!inShutdown) {
      scheduler.scheduleOnce(Duration(1, "second"), self, CheckMessages)
    }
  }

  def batchProcessMessages(messages: Seq[IngestMessage]) {
    import scala.collection.mutable

    var actions = mutable.Map.empty[ProjectionDescriptor, (Seq[ProjectionInsert], Seq[InsertComplete])]

    var m = 0
    while(m < messages.size) {
      messages(m) match {
        case SyncMessage(_, _, _) => // TODO

        case em @ EventMessage(eventId, _) =>

          val projectionUpdates = routingTable.route(em)
          markInsertsPending(eventId, projectionUpdates.size)  

          var i = 0
          while(i < projectionUpdates.size) {
            val update = projectionUpdates(i)
            val insert = ProjectionInsert(update.identities, update.values)
            val complete = InsertComplete(eventId, update.descriptor, update.values, update.metadata)
            val (inserts, completes) = actions.get(update.descriptor) getOrElse { (Vector.empty, Vector.empty) }
            val newActions = (inserts :+ insert, completes :+ complete) 
            actions += (update.descriptor -> newActions)
            i += 1
          }
      }
      m += 1
    }

    val acquire = projectionActors ? AcquireProjectionBatch(actions.keys)
    acquire.onComplete { 
      case Left(t) =>
        logger.error("Exception acquiring projection actor: ", t)

      case Right(ProjectionBatchAcquired(actorMap)) => 
        
        val descItr = actions.keys.iterator
        while(descItr.hasNext) {
          val desc = descItr.next
          val (inserts, completes) = actions(desc)
          val actor = actorMap(desc)
          val fut = actor ? ProjectionBatchInsert(inserts)
          fut.onComplete { _ => 
            self ! InsertBatchComplete(completes)
            projectionActors ! ReleaseProjection(desc)
          }
        }
        
      case Right(ProjectionError(errs)) =>
        for(err <- errs.list) logger.error("Error acquiring projection actor: ", err)
    }
  }

  def processMessages(messages: Seq[IngestMessage]) {
    var m = 0
    while(m < messages.size) {
      messages(m) match {
        case SyncMessage(_, _, _) => // TODO

        case em @ EventMessage(eventId, _) =>

          val projectionUpdates = routingTable.route(em)

          markInsertsPending(eventId, projectionUpdates.size)  

          var cnt = 0
          while(cnt < projectionUpdates.length) {
            val pd = projectionUpdates(cnt)
            val acquire = projectionActors ? AcquireProjection(pd.descriptor)
            acquire.onComplete { 
              case Left(t) =>
                logger.error("Exception acquiring projection actor: ", t)

              case Right(ProjectionAcquired(proj)) => 
                val fut = proj ? ProjectionInsert(pd.identities, pd.values)
                fut.onComplete { _ => 
                  self ! InsertComplete(eventId, pd.descriptor, pd.values, pd.metadata)
                  projectionActors ! ReleaseProjection(pd.descriptor)
                }
                
              case Right(ProjectionError(errs)) =>
                for(err <- errs.list) logger.error("Error acquiring projection actor: ", err)
            }
            cnt += 1
          }
      }
      m += 1
    }
  }

  import scala.collection.mutable

  private var expectation = mutable.Map[EventId, Int]()
  private val inserted = mutable.Map[EventId, mutable.ListBuffer[InsertComplete]]()

  def markInsertsPending(eventId: EventId, expected: Int) { 
    expectation += (eventId -> expected)
  }

  def markInsertComplete(insert: InsertComplete) { 
    val eventId = insert.eventId
    val inserts = inserted.get(eventId) map { _ += insert } getOrElse(mutable.ListBuffer(insert))
    if(inserts.size >= expectation(eventId)) {
      expectation -= eventId
      inserted -= eventId
      //logger.debug("Event insert complete: updating metadata")
      metadataActor ! UpdateMetadata(inserts)
    } else {
      inserted += (eventId -> inserts) 
    }
    if(expectation.isEmpty) {
      //logger.debug("Batch complete")
      self ! CheckMessages
    }
  }
}

case class InsertComplete(eventId: EventId, descriptor: ProjectionDescriptor, values: Seq[CValue], metadata: Seq[Set[Metadata]])
case class InsertBatchComplete(inserts: Seq[InsertComplete])

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
