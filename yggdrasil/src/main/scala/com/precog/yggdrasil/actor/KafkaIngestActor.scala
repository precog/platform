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
