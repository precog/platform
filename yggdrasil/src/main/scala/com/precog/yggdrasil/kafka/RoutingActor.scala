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
case class ProjectionActorRequest(descriptor: ProjectionDescriptor)

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

class RoutingActor(metadataActor: ActorRef, routingTable: RoutingTable, descriptorLocator: ProjectionDescriptorLocator, descriptorIO: ProjectionDescriptorIO) extends Actor with Logging {
  import RoutingActor._

  val projectionActors = Cache.concurrent[ProjectionDescriptor, ActorRef](
    CacheSettings(
      expirationPolicy = ExpirationPolicy(None, None, TimeUnit.SECONDS), 
      evict = { 
        (descriptor, actor) => descriptorIO(descriptor).map(_ => actor ! Stop).unsafePerformIO
      }
    )
  )

  private def projectionActor(descriptor: ProjectionDescriptor): ValidationNEL[Throwable, ActorRef] = {
    val actor = projectionActors.get(descriptor).toSuccess(new RuntimeException("No cached actor available."): Throwable).toValidationNel.orElse {
      LevelDBProjection(initDescriptor(descriptor).unsafePerformIO, descriptor).map(p => context.actorOf(Props(new ProjectionActor(p, descriptor))))
    }

    actor.foreach(projectionActors.putIfAbsent(descriptor, _))
    actor
  }

  def initDescriptor(descriptor: ProjectionDescriptor): IO[File] = {
    descriptorLocator(descriptor).flatMap( f => descriptorIO(descriptor).map(_ => f) )
  }

  def receive = {
    case SyncMessage(producerId, syncId, eventIds) => // TODO 

    case em @ EventMessage(eventId, _) =>
      val projectionUpdates = routingTable.route(em)

      registerCheckpointExpectation(eventId, projectionUpdates.size)

      for (ProjectionData(descriptor, identities, values, metadata) <- projectionUpdates) {
        projectionActor(descriptor) match {
          case Success(actor) =>
            val fut = actor ? ProjectionInsert(identities, values)
            fut.onComplete { _ => 
              metadataActor ! UpdateMetadata(List(InsertComplete(eventId, descriptor, values, metadata)))
            }

          case Failure(errors) => 
            for (t <- errors.list) logger.error("Could not obtain actor for projection: " , t)
        }
      }
      sender ! ()

    case ProjectionActorRequest(descriptor) =>
      sender ! projectionActor(descriptor)

    case a                                  =>
      logger.error("Unknown Routing Actor action: " + a.getClass.getName)
  }

  def registerCheckpointExpectation(eventId: EventId, count: Int): Unit = metadataActor ! ExpectedEventActions(eventId, count)

  def extractMetadataFor(desc: ProjectionDescriptor, metadata: Set[(ColumnDescriptor, JValue, Set[Metadata])]): Seq[Set[Metadata]] = 
    desc.columns flatMap { c => metadata.find(_._1 == c).map( _._3 ) } toSeq
}

class NewRoutingActor(routingTable: RoutingTable, descriptorLocator: ProjectionDescriptorLocator, descriptorIO: ProjectionDescriptorIO, metadataActor: ActorRef, ingestActor: ActorRef, scheduler: Scheduler) extends Actor with Logging {
  
  import RoutingActor._

  def receive = {
    
    case CheckMessages =>
      logger.debug("Routing Actor - Check Messages")
      ingestActor ! GetMessages(self)
    
    case NoMessages =>
      logger.debug("Routing Actor - No Messages")
      scheduleNextCheck
    
    case Messages(messages) =>
      logger.debug("Routing Actor - Processing Message Batch (%d)".format(messages.size))
      processMessages(messages)
    
    case ic @ InsertComplete(_, _, _, _) =>
      //logger.debug("Insert Complete")
      markInsertComplete(ic)
    
    case ProjectionActorRequest(descriptor) =>
      sender ! projectionActor(descriptor)
  
  }

  def scheduleNextCheck { 
    scheduler.scheduleOnce(Duration(1, "second"), self, CheckMessages)
  }

  def processMessages(messages: Seq[IngestMessage]) {
    messages foreach {
      case SyncMessage(_, _, _) => // TODO

      case em @ EventMessage(eventId, _) =>
        val projectionUpdates = routingTable.route(em)
        
        markInsertsPending(eventId, projectionUpdates.size)  

        for (ProjectionData(descriptor, identities, values, metadata) <- projectionUpdates) {
          projectionActor(descriptor) match {
            case Success(actor) =>
              val fut = actor ? ProjectionInsert(identities, values)
              fut.onComplete { _ => 
                self ! InsertComplete(eventId, descriptor, values, metadata)
              }

            case Failure(errors) => 
              for (t <- errors.list) logger.error("Could not obtain actor for projection: " , t)
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
    //logger.debug("Expectations: (Before)" + expectation.size)
    //logger.debug("Inserted: (Before)" + inserted.size)
    val eventId = insert.eventId
    val inserts = inserted.get(eventId) map { _ :+ insert } getOrElse(List(insert))
    if(inserts.size >= expectation(eventId)) {
      expectation -= eventId
      inserted -= eventId
      metadataActor ! UpdateMetadata(inserts)
    } else {
      inserted += (eventId -> inserts) 
    }
    if(expectation.isEmpty) self ! CheckMessages
    //logger.debug("Expectations: (After)" + expectation.size)
    //logger.debug("Inserted: (After)" + inserted.size)
    //logger.debug("Exp summary: " + expectation.foldLeft ( Map[Int, Int]() ) {
    //  case (acc, (_, exp)) =>
    //    val newCnt = acc.get(exp) map { _ + 1 } getOrElse { 1 }
    //    acc + (exp -> newCnt)
    //})
    //logger.debug("Inserted summary: " + inserted.foldLeft ( Map[Int, Int]() ) {
    //  case (acc, (_, ins)) =>
    //    val newCnt = acc.get(ins.size) map { _ + 1 } getOrElse { 1 }
    //    acc + (ins.size -> newCnt)
    //})
  }

  val projectionActors = Cache.concurrent[ProjectionDescriptor, ActorRef](
    CacheSettings(
      expirationPolicy = ExpirationPolicy(None, None, TimeUnit.SECONDS), 
      evict = { 
        (descriptor, actor) => descriptorIO(descriptor).map(_ => actor ! Stop).unsafePerformIO
      }
    )
  )

  private def projectionActor(descriptor: ProjectionDescriptor): ValidationNEL[Throwable, ActorRef] = {
    val actor = projectionActors.get(descriptor).toSuccess(new RuntimeException("No cached actor available."): Throwable).toValidationNel.orElse {
      LevelDBProjection(initDescriptor(descriptor).unsafePerformIO, descriptor).map(p => context.actorOf(Props(new ProjectionActor(p, descriptor))))
    }

    actor.foreach(projectionActors.putIfAbsent(descriptor, _))
    actor
  }

  def initDescriptor(descriptor: ProjectionDescriptor): IO[File] = {
    descriptorLocator(descriptor).flatMap( f => descriptorIO(descriptor).map(_ => f) )
  }
}

//
// NEED SKETCH FOR
//

// Updated projection actor responsibilities
//
// - wait for messages from routing actor
// - process the given action and reply to routing actor

// Updated routing actor responsibilities
//
// - start from idle
// -- request messages from kafka consumer
// -- if none 
// --   enact delay strategy
// -- else 
// --   register leveldb expections
// --   send events to leveldb
// -- wait for leveldb responses
// -- foreach leveldb response
// --   update expectations
// --   if expectations complete notify metadata
// --   if all expectations complete continue
// -- end

//
// HAVE SKETCH FOR
//

// Updated kafka consumer (now actor) responsibilities
//
// - get initial offset/message clock from YggCheckpoints
// - wait for message request from routing actor
// -   read message batch and send to routing actor
// -   update YggCheckpoints with latest offset/message clock
// - back to wait
// - on shutdown close kafka connection

// Updated metadata actor responsibilities 
// - wait for metadata update calls from routing actor
// - update metadata
// - periodically write metadata to disk
//   and register checkpoint

case class InsertComplete(eventId: EventId, descriptor: ProjectionDescriptor, values: Seq[CValue], metadata: Seq[Set[Metadata]])


class KafkaShardIngestActor(checkpoints: YggCheckpoints, consumer: KafkaBatchConsumer) extends ShardIngestActor {

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
