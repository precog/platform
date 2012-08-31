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
import common.kafka._

import com.precog.common.kafka.KafkaEventCodec

import akka.dispatch.Future
import akka.dispatch.MessageDispatcher

import com.weiglewilczek.slf4s._

import scala.annotation.tailrec

import java.util.Properties

import _root_.kafka.api._
import _root_.kafka.consumer._
import _root_.kafka.producer._
import _root_.kafka.message._

import org.streum.configrity.{Configuration, JProperties}

class KafkaRelayAgent(eventIdSeq: EventIdSequence, localConfig: Configuration, centralConfig: Configuration)(implicit dispatcher: MessageDispatcher) extends Logging {

  lazy private val localTopic = localConfig[String]("topic")
  lazy private val centralTopic = centralConfig[String]("topic")

  lazy private val ingestCodec = new KafkaIngestMessageCodec
  lazy private val centralProperties = JProperties.configurationToProperties(centralConfig)
  lazy private val producer = new Producer[String, IngestMessage](new ProducerConfig(centralProperties))

  lazy private val consumer = {
    val hostname = localConfig[String]("broker.host", "localhost")
    val port = localConfig[String]("broker.port", "9082").toInt
    new KafkaMessageConsumer(hostname, port, localTopic)(relayMessages _)
  }

  def start() = consumer.start(eventIdSeq.getLastOffset)

  def stop() = consumer.stop().map
               { _ => producer.close } flatMap
               { _ => eventIdSeq.close }

  def relayMessages(messages: List[MessageAndOffset]) {
    val outgoing = messages.map { msg =>
      ingestCodec.toEvent(msg.message) match {
        case em: EventMessage =>
          val (producerId, sequenceId) = eventIdSeq.next(msg.offset)
          EventMessage(producerId, sequenceId, em.event)
        case am: ArchiveMessage =>
          val (producerId, sequenceId) = eventIdSeq.next(msg.offset)
          ArchiveMessage(producerId, sequenceId, am.archive)
      }
    }
    val data = new ProducerData[String, IngestMessage](centralTopic, outgoing)
    producer.send(data)
    eventIdSeq.saveState(messages.last.offset)
  }
}

private class KafkaMessageConsumer(host: String, port: Int, topic: String)(processor: List[MessageAndOffset] => Unit)(implicit dispatcher: MessageDispatcher) extends Logging {

  lazy private val consumer = {
    new SimpleConsumer(host, port, 5000, 64 * 1024)
  }

  val bufferSize = 1024 * 1024

  def start(nextOffset: => Long) = Future[Unit] {
    val consumerThread = new Thread() {
      val retryDelay = 5000

      override def run() {
        while(true) {
          val offset = nextOffset
          logger.debug("Kafka consumer starting from offset: " + offset)
          try {
            ingestBatch(offset, 0, 0, 0)
          } catch {
            case ex =>
              logger.error("Error in kafka consumer.", ex)
          }
          Thread.sleep(retryDelay)
        }
      }

      @tailrec
      def ingestBatch(offset: Long, batch: Long, delay: Long, waitCount: Long) {
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
    }
    consumerThread.start()
  }

  def stop() = Future { consumer.close }

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
