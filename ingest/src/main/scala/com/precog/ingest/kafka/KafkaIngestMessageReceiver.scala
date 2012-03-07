package com.precog.ingest
package kafka

import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger
import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.nio.charset.CharsetDecoder
import java.nio.charset.CharsetEncoder

import scalaz._
import Scalaz._

import org.scalacheck.Gen._

import _root_.kafka.consumer._
import _root_.kafka.message._
import _root_.kafka.serializer._

import blueeyes.json.JsonAST._
import blueeyes.json.JPath
import blueeyes.json.JsonParser
import blueeyes.json.Printer

import blueeyes.json.xschema.{ ValidatedExtraction, Extractor, Decomposer }
import blueeyes.json.xschema.Extractor._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.Extractor._

import com.precog.common._

class KafkaIngestMessageRecievers(receivers: Map[MailboxAddress, List[IngestMessageReceiver]]) {
  def find(address: MailboxAddress) = receivers(address)
}

class KafkaIngestMessageReceiver(topic: String, config: Properties) extends IngestMessageReceiver {
  config.put("autocommit.enable", "false")
  
  val connector = Consumer.create(new ConsumerConfig(config))
  val streams = connector.createMessageStreams[IngestMessage](Map(topic -> 1), new KafkaIngestMessageCodec)  
  val stream = streams(topic)(0)
  val itr = stream.iterator
  
  def hasNext() = itr.hasNext
  def next() = itr.next
  def sync() = connector.commitOffsets
}

// This could be made more efficient by writing a custom message class that bootstraps from
// a ByteBuffer, but this was the quick and dirty way to get moving

class KafkaIngestMessageCodec extends Encoder[IngestMessage] with Decoder[IngestMessage] {
  def toMessage(event: IngestMessage) = {
    new Message(IngestMessageSerialization.toBytes(event))
  }

  def toEvent(msg: Message) = {
    IngestMessageSerialization.read(msg.payload)
  }
}

class KafkaEventCodec extends Encoder[Event] with Decoder[Event] {
  val charset = Charset.forName("UTF-8")
 
  def toMessage(event: Event) = {
    val msgBuffer = charset.encode(Printer.compact(Printer.render(event.serialize)))
    val byteArray = new Array[Byte](msgBuffer.limit)
    msgBuffer.get(byteArray)
    new Message(byteArray)
  }

  def toEvent(msg: Message): Event = {
    val decoder = charset.newDecoder
    val charBuffer = decoder.decode(msg.payload)
    val jvalue = JsonParser.parse(charBuffer.toString()) 
    jvalue.validated[Event] match {
      case Success(e) => e
      case Failure(e) => sys.error("Error parsing event: " + e)
    }
  }
}

