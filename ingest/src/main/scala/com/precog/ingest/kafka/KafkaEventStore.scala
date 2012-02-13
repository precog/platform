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
package ingest
package kafka

import common._
import ingest.util._

import scala.annotation.tailrec

import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger

import akka.dispatch.Future
import akka.dispatch.MessageDispatcher

import com.weiglewilczek.slf4s._ 

import org.I0Itec.zkclient

import _root_.kafka.api._
import _root_.kafka.consumer._
import _root_.kafka.producer._

import scalaz._
import Scalaz._

class LocalKafkaEventStore(localTopic: String, localConfig: Properties)(implicit dispatcher: MessageDispatcher) extends EventStore with Logging {
  
  private val producer = new Producer[String, Event](new ProducerConfig(localConfig))

  def start(): Future[Unit] = Future { () } 

  def save(event: Event) = Future {
    val data = new ProducerData[String, Event](localTopic, event)
    try {
      producer.send(data)
    } catch {
      case x => logger.info("Error saving event to local kafka queue.", x) 
    }
  }

  def stop(): Future[Unit] = Future { producer.close } 

}

class KafkaEventStore(router: EventRouter, producerId: Int, firstEventId: Int = 0)(implicit dispatcher: MessageDispatcher) extends EventStore {
  private val nextEventId = new AtomicInteger(firstEventId)
  
  def save(event: Event) = {
    val eventId = nextEventId.incrementAndGet
    router.route(EventMessage(producerId, eventId, event)) map { _ => () }
  }

  def start(): Future[Unit] = Future { () }

  def stop(): Future[Unit] = router.close
}

trait EventIdSequence {
  def next(offset: Long): (Int, Int)
  def saveState(offset: Long): Unit
  def getLastOffset(): Long
  def close(): Future[Unit]
}

class TestEventIdSequence(producerId: Int, sequenceIdStart: Int = 0)(implicit dispatcher: MessageDispatcher) extends EventIdSequence {
  private val nextSequenceId = new AtomicInteger(sequenceIdStart) 
  def next(offset: Long): (Int, Int) = (producerId, nextSequenceId.getAndIncrement)
  def saveState(offset: Long) = ()
  def getLastOffset() = 0 
  def close(): Future[Unit] = Future { () }
}

class SystemEventIdSequence(agent: String, coordination: SystemCoordination, blockSize: Int = 100000)(implicit dispatcher: MessageDispatcher) extends EventIdSequence {

  case class InternalState(eventRelayState: EventRelayState) {
    private val nextSequenceId = new AtomicInteger(eventRelayState.nextSequenceId)

    val block = eventRelayState.idSequenceBlock
    val lastOffset = eventRelayState.offset

    def current = nextSequenceId.get
    def isEmpty = current > block.lastSequenceId
    def next() = if(isEmpty) sys.error("Next on empty sequence is invalid.") else
                             (block.producerId, nextSequenceId.getAndIncrement)
  }

  // How to approach this from a lazy viewpoint (deferred at this time but need to return)
  private var state: InternalState = loadInitialState

  private def loadInitialState() = {
    val eventRelayState = coordination.registerRelayAgent(agent, blockSize).getOrElse(sys.error("Unable to retrieve relay agent state."))
    InternalState(eventRelayState)
  }

  def next(offset: Long) = {
    if(state.isEmpty) {
      state = refill(offset) 
    }
    state.next
  }

  def currentRelayState(offset: Long) = {
    EventRelayState(offset, state.current, state.block)
  }

  def refill(offset: Long): InternalState = {
    coordination.renewEventRelayState(agent, currentRelayState(offset), blockSize) match {
      case Success(ers @ EventRelayState(_,_,_)) => InternalState(ers) 
      case Failure(e)                            => sys.error("Error trying to renew relay agent: " + e)
    }
  }
  
  def saveState(offset: Long) {
    state = coordination.saveEventRelayState(agent, currentRelayState(offset)) match {
      case Success(ers @ EventRelayState(_,_,_)) => InternalState(ers)  
      case Failure(e)                            => sys.error("Error trying to save relay agent state: " + e)
    }
  }

  def close() = Future { 
    coordination.close() 
  }

  def getLastOffset(): Long = {
    state.lastOffset
  }

}

class KafkaEventRelayAgent(eventIdSeq: EventIdSequence, localTopic: String, localConfig: Properties, centralTopic: String, centralConfig: Properties)(implicit dispatcher: MessageDispatcher) extends Logging {

  lazy private val eventCodec = new KafkaEventCodec

  lazy private val producer = new Producer[String, EventMessage](new ProducerConfig(centralConfig))

  lazy private val consumer = {
    new SimpleConsumer("localhost", 9082, 5000, 64 * 1024)
  }

  private val bufferSize: Int = 1024 * 1024

  def start() = Future[Unit] {
    
    val relayThread = new Thread() {
      override def run() {
        val startingOffset = eventIdSeq.getLastOffset
        logger.debug("Kafka relay agent starting from offset: " + startingOffset)
        ingestBatch(startingOffset, 0, 0, 0)
      }

      @tailrec
      def ingestBatch(offset: Long, batch: Long, delay: Long, waitCount: Long) {
        if(batch % 100 == 0) logger.debug("Processing batch %d [%s]".format(batch, if(waitCount > 0) "IDLE" else "ACTIVE"))
        val fetchRequest = new FetchRequest(localTopic, 0, offset, bufferSize)

        val messages = consumer.fetch(fetchRequest)

        // A future optimizatin would be to move this to another thread (or maybe actors)
        val outgoing = messages map { msg => 
          val (producerId, sequenceId) = eventIdSeq.next(msg.offset)
          EventMessage(producerId, sequenceId, eventCodec.toEvent(msg.message)) 
        } toList
        val data = new ProducerData[String, EventMessage](centralTopic, outgoing)
        producer.send(data)

        val newDelay = delayStrategy(messages.sizeInBytes.toInt, delay, waitCount)

        val (newOffset, newWaitCount) = if(messages.size > 0) {
          val o: Long = messages.last.offset
          logger.debug("Kafka relay agent batch size: %d offset: %d)".format(messages.size, o))
          (o, 0L)
        } else {
          (offset, waitCount + 1)
        }
        
        Thread.sleep(newDelay)
        
        if(newOffset != offset) {
          eventIdSeq.saveState(newOffset)
        }

        ingestBatch(newOffset, batch + 1, newDelay, newWaitCount)
      }
    }
    relayThread.start
  }

  def stop() = Future { consumer.close } map 
                      { _ => producer.close } flatMap 
                      { _ => eventIdSeq.close }

  val maxDelay = 100.0
  val waitCountFactor = 25

  def delayStrategy(messageBytes: Int, currentDelay: Long, waitCount: Long): Long = {
    if(messageBytes == 0) {
      val boundedWaitCount = if(waitCount > waitCountFactor) waitCountFactor else waitCount
      (maxDelay * boundedWaitCount / waitCountFactor).toLong
    } else {
      (maxDelay * (1.0 - messageBytes.toDouble / bufferSize)).toLong
    }
  }
}
