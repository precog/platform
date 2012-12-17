package com.precog.common
package kafka

import com.precog.common.ingest._

import java.nio.charset.Charset
import java.nio.charset.CharsetDecoder
import java.nio.charset.CharsetEncoder

import scalaz._
import scalaz.syntax.apply._

import _root_.kafka.message._
import _root_.kafka.serializer._

import blueeyes.json.JParser
import blueeyes.json.serialization.DefaultSerialization._

// This could be made more efficient by writing a custom message class that bootstraps from
// a ByteBuffer, but this was the quick and dirty way to get moving

class KafkaEventMessageCodec extends Encoder[EventMessage] with Decoder[EventMessage] {
  def toMessage(message: EventMessage) = {
    new Message(EventMessage.toBytes(message))
  }

  def toEvent(msg: Message) = {
    EventMessage.read(msg.payload)
  }
}

/*
class KafkaIngestCodec extends Encoder[Ingest] with Decoder[Ingest] {
  val charset = Charset.forName("UTF-8")
 
  def toMessage(event: Ingest) = {
    val msgBuffer = charset.encode(event.serialize.renderCompact)
    val byteArray = new Array[Byte](msgBuffer.limit)
    msgBuffer.get(byteArray)
    new Message(byteArray)
  }

  def toEvent(msg: Message): Ingest = {
    val decoder = charset.newDecoder
    val charBuffer = decoder.decode(msg.payload)
    val jvalue = JParser.parse(charBuffer.toString()) 
    jvalue.validated[Ingest] match {
      case Success(e) => e
      case Failure(e) => sys.error("Error parsing event: " + e)
    }
  }
}

class KafkaArchiveCodec extends Encoder[Archive] with Decoder[Archive] {
  val charset = Charset.forName("UTF-8")
 
  def toMessage(archive: Archive) = {
    val msgBuffer = charset.encode(archive.serialize.renderCompact)
    val byteArray = new Array[Byte](msgBuffer.limit)
    msgBuffer.get(byteArray)
    new Message(byteArray)
  }

  def toEvent(msg: Message): Archive = {
    val decoder = charset.newDecoder
    val charBuffer = decoder.decode(msg.payload)
    val jvalue = JParser.parse(charBuffer.toString()) 
    jvalue.validated[Archive] match {
      case Success(a) => a
      case Failure(a) => sys.error("Error parsing archive: " + a)
    }
  }
}
*/
