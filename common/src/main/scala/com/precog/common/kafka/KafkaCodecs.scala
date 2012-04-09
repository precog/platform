package com.precog.common
package kafka

import com.precog.common._

import java.nio.charset.Charset
import java.nio.charset.CharsetDecoder
import java.nio.charset.CharsetEncoder

import scalaz._
import Scalaz._

import _root_.kafka.message._
import _root_.kafka.serializer._

import blueeyes.json.JsonParser
import blueeyes.json.Printer
import blueeyes.json.xschema.DefaultSerialization._

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

