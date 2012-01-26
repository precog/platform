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

