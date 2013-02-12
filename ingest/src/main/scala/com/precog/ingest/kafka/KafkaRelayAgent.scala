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
package com.precog.ingest
package kafka

import com.precog.common._
import com.precog.common.accounts._
import com.precog.common.ingest._
import com.precog.common.kafka._
import com.precog.util.PrecogUnit

import blueeyes.bkka._
import blueeyes.json.serialization.Extractor.Error

import akka.dispatch.Future
import akka.dispatch.Promise
import akka.dispatch.ExecutionContext

import com.weiglewilczek.slf4s._

import _root_.kafka.api._
import _root_.kafka.consumer._
import _root_.kafka.producer._
import _root_.kafka.message._

import java.util.Properties
import org.streum.configrity.{Configuration, JProperties}

import scalaz._
import scalaz.std.list._
import scalaz.syntax.traverse._
import scala.annotation.tailrec

/** An independent agent that will consume records using the specified consumer,
  * augment them with record identities, then send them with the specified producer. */
final class KafkaRelayAgent(
    accountFinder: AccountFinder[Future], eventIdSeq: EventIdSequence,
    consumer: SimpleConsumer, topic: String, 
    producer: Producer[String, Message], 
    bufferSize: Int = 1024 * 1024, retryDelay: Long = 5000L,
    maxDelay: Double = 100.0, waitCountFactor: Int = 25)(implicit executor: ExecutionContext) extends Runnable with Logging {

  @volatile private var runnable = false;
  private val stopPromise = Promise[PrecogUnit]()

  def stop: Future[PrecogUnit] = Future({ runnable = false }) flatMap { _ => stopPromise }

  override def run() {
    while (runnable) {
      val offset = eventIdSeq.getLastOffset
      logger.debug("Kafka consumer starting from offset: " + offset)

      try {
        ingestBatch(offset, 0, 0, 0)
      } catch {
        case ex => logger.error("Error in kafka consumer.", ex)
      }

      Thread.sleep(retryDelay)
    }

    stopPromise.success(PrecogUnit)
  }

  @tailrec
  private def ingestBatch(offset: Long, batch: Long, delay: Long, waitCount: Long) {
    if(batch % 100 == 0) logger.debug("Processing kafka consumer batch %d [%s]".format(batch, if(waitCount > 0) "IDLE" else "ACTIVE"))
    val fetchRequest = new FetchRequest(topic, 0, offset, bufferSize)

    val messages = consumer.fetch(fetchRequest)

    // A future optimizatin would be to move this to another thread (or maybe actors)
    val outgoing = messages.toList

    if(outgoing.size > 0) {
      processor(outgoing)
    }

    val newDelay = delayStrategy(messages.sizeInBytes.toInt, delay, waitCount)

    val (newOffset, newWaitCount) = if(messages.size > 0) {
      val o: Long = messages.last.offset
      logger.debug("Kafka consumer batch size: %d offset: %d)".format(messages.size, o))
      (o, 0L)
    } else {
      (offset, waitCount + 1)
    }

    Thread.sleep(newDelay)

    ingestBatch(newOffset, batch + 1, newDelay, newWaitCount)
  }

  private def delayStrategy(messageBytes: Int, currentDelay: Long, waitCount: Long): Long = {
    if(messageBytes == 0) {
      val boundedWaitCount = if (waitCount > waitCountFactor) waitCountFactor else waitCount
      (maxDelay * boundedWaitCount / waitCountFactor).toLong
    } else {
      (maxDelay * (1.0 - messageBytes.toDouble / bufferSize)).toLong
    }
  }

  private def processor(messages: List[MessageAndOffset]) = {
    val outgoing: List[Validation[Error, Future[Message]]] = messages map { msg => 
      EventEncoding.read(msg.message.buffer) map { identify(_, msg.offset) } map { _ map { em => new Message(EventMessageEncoding.toMessageBytes(em)) } }
    }

    outgoing.sequence[({ type λ[α] = Validation[Error, α] })#λ, Future[Message]] map { messageFutures =>
      messageFutures.sequence[Future, Message] map { messages => 
        producer.send(new ProducerData[String, Message](centralTopic, messages))
      } onFailure {
        case ex => logger.error("An error occurred forwarding messages from the local queue to central.", ex)
      } onSuccess {
        case _ => eventIdSeq.saveState(messages.last.offset)
      }
    } valueOr { error =>
      logger.error("Deserialization errors occurred reading events from Kafka: " + error.message)
    }
  }

  private def identify(event: Event, offset: Long): Future[EventMessage] = {
    event match {
      case Ingest(apiKey, path, ownerAccountId, data, jobId) =>
        accountFinder.resolveForWrite(ownerAccountId, apiKey) map {
          case Some(accountId) =>
            val ingestRecords = data map { IngestRecord(eventIdSeq.next(offset), _) }
            IngestMessage(apiKey, path, accountId, ingestRecords, jobId)

          case None =>
            // cannot relay event without a resolved owner account ID; fail loudly.
            // this will abort the future, ensuring that state doesn't get corrupted
            sys.error("Unable to establish owner account ID for ingest " + event)
        } 

      case archive @ Archive(_, _, _) =>
        Promise.successful(ArchiveMessage(eventIdSeq.next(offset), archive))
    }
  }
}

object KafkaRelayAgent {
  def apply(accountFinder: AccountFinder[Future], eventIdSeq: EventIdSequence, localConfig: Configuration, centralConfig: Configuration)(implicit executor: ExecutionContext): (KafkaRelayAgent, Stoppable) = {

    val localTopic = localConfig[String]("topic", "local_event_cache")
    val centralTopic = centralConfig[String]("topic", "central_event_store")

    val centralProperties = JProperties.configurationToProperties(centralConfig)
    val producer = new Producer[String, Message](new ProducerConfig(centralProperties))

    val consumerHost = localConfig[String]("broker.host", "localhost")
    val consumerPort = localConfig[String]("broker.port", "9082").toInt
    val consumer = new SimpleConsumer(consumerHost, consumerPort, 5000, 64 * 1024)

    val relayAgent = new KafkaRelayAgent(accountFinder, eventIdSeq, consumer, localTopic, producer) 
    val stoppable = Stoppable.fromFuture(relayAgent.stop map { _ => consumer.close; producer.close })

    new Thread(relayAgent).start()
    (relayAgent, stoppable)
  }
}
