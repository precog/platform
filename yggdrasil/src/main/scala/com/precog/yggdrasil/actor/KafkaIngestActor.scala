package com.precog
package yggdrasil
package actor

import com.precog.common._
import com.precog.common.kafka._
import com.precog.common.util._

import akka.actor.Actor
import akka.actor.ActorRef

import com.weiglewilczek.slf4s._

import _root_.kafka.message._

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
