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
package com.querio.ingest.api

import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger
import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.nio.charset.CharsetDecoder
import java.nio.charset.CharsetEncoder

import scalaz._
import Scalaz._

import org.scalacheck.Gen._

import kafka.consumer._
import kafka.message._
import kafka.serializer._

import blueeyes.json.JsonAST._
import blueeyes.json.JPath
import blueeyes.json.JsonParser
import blueeyes.json.Printer

import blueeyes.json.xschema.{ ValidatedExtraction, Extractor, Decomposer }
import blueeyes.json.xschema.Extractor._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.Extractor._

import com.reportgrid.common.util.ArbitraryIngestMessage

import com.reportgrid.common._

trait IngestMessageReceivers {
  def find(address: MailboxAddress): List[IngestMessageReceiver]
}

trait IngestMessageReceiver extends Iterator[IngestMessage] {
  def hasNext: Boolean
  def next(): IngestMessage 
  def sync()
}

object TestIngestMessageRecievers extends IngestMessageReceivers with ArbitraryIngestMessage {
  def find(address: MailboxAddress) = List(new IngestMessageReceiver() {
    def hasNext = true
    def next() = genRandomEventMessage.sample.get
    def sync() = Unit
  })
}

class KafkaIngestMessageRecievers(receivers: Map[MailboxAddress, List[IngestMessageReceiver]]) {
  def find(address: MailboxAddress) = receivers(address)
}

class KafkaIngestMessageReceiver(topic: String, config: Properties) extends IngestMessageReceiver {
  config.put("autocommit.enable", "false")
  
  val connector = Consumer.create(new ConsumerConfig(config))
  val streams = connector.createMessageStreams[IngestMessage](Map(topic -> 1), new IngestMessageCodec)  
  val stream = streams(topic)(0)
  val itr = stream.iterator
  
  def hasNext() = itr.hasNext
  def next() = itr.next
  def sync() = connector.commitOffsets
}

case class MailboxAddress(id: Long)

class SyncMessages(producerId: Int, initialId: Int = 1) {
  private val nextId = new AtomicInteger(initialId)
  
  val start: SyncMessage = SyncMessage(producerId, 0, List.empty)
  def next(eventIds: List[Int]): SyncMessage = SyncMessage(producerId, nextId.getAndIncrement(), eventIds)
  def stop(eventIds: List[Int] = List.empty) = SyncMessage(producerId, Int.MaxValue, eventIds)
}

// This could be made more efficient by writing a custom message class that bootstraps from
// a ByteBuffer, but this was the quick and dirty way to get moving

class IngestMessageCodec extends Encoder[IngestMessage] with Decoder[IngestMessage] {
  def toMessage(event: IngestMessage) = {
    new Message(IngestMessageSerialization.toBytes(event))
  }

  def toEvent(msg: Message) = {
    IngestMessageSerialization.read(msg.payload)
  }
}

