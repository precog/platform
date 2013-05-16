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
package com.precog.common
package kafka

import com.precog.common.ingest._

import blueeyes.json.JParser
import blueeyes.json.serialization.Extractor._
import blueeyes.json.serialization.DefaultSerialization._

import com.weiglewilczek.slf4s.Logging

import _root_.kafka.message._
import _root_.kafka.serializer._

import java.nio.charset.Charset
import java.nio.ByteBuffer

import scalaz._
import scalaz.syntax.id._
import scalaz.Validation._
import scalaz.syntax.bifunctor._

trait EncodingFlags {
  val charset = Charset.forName("UTF-8")

  val stopByte: Byte = 0x00
  val jsonIngestMessageFlag: Byte = 0x01
  val jsonArchiveMessageFlag: Byte = 0x03
  val jsonIngestFlag: Byte = 0x04
  val jsonArchiveFlag: Byte = 0x05
  val storeFileFlag: Byte = 0x06
  val magicByte: Byte = -123

  def writeHeader(buffer: ByteBuffer, encodingFlag: Byte): ByteBuffer = {
    buffer.put(magicByte).put(encodingFlag).put(stopByte)
  }

  def readHeader(buffer: ByteBuffer): Validation[Error, Byte] = {
    val magic = buffer.get()
    if (magic == magicByte) {
      val msgType = buffer.get()
      val stop    = buffer.get()

      if (stop == stopByte) {
        success(msgType)
      } else {
        failure(Error.invalid("Invalid message: bad stop byte. Found [" + stop + "]"))
      }
    } else {
      failure(Error.invalid("Invalid message: bad magic byte. Found [" + magic + "]"))
    }
  }
}

class KafkaEventCodec extends Encoder[Event] {
  def toMessage(event: Event) = {
    val msgBuf = EventEncoding.toMessageBytes(event)
    val byteArray = new Array[Byte](msgBuf.limit)
    msgBuf.get(byteArray)
    // If you attempt to simply create the Message passing it the byte buffer,
    // as one of its constructors permits, the Kafka internal "magic byte"
    // and checksum do not get integrated, so things blow up deep in the internals
    // of Kafka. Demand PETA action now to save the kittens!
    new Message(byteArray)
  }
}

object EventEncoding extends EncodingFlags with Logging {
  def toMessageBytes(event: Event) = {
    val serialized = event.serialize.renderCompact
    logger.trace("Serialized event " + event + " to " + serialized)
    val msgBuffer = charset.encode(serialized)
    val bytes = ByteBuffer.allocate(msgBuffer.limit + 3)
    writeHeader(bytes, event.fold(_ => jsonIngestFlag, _ => jsonArchiveFlag, _ => storeFileFlag))
    bytes.put(msgBuffer)
    bytes.flip()
    bytes
  }

  def write(buffer: ByteBuffer, event: Event) = {
    buffer.put(toMessageBytes(event))
  }

  def read(buffer: ByteBuffer): Validation[Error, Event] = {
    for {
      msgType <- readHeader(buffer)
      jv <- ((Error.thrown _) <-: JParser.parseFromByteBuffer(buffer))
      event <-  msgType match {
                  case `jsonIngestFlag`  => jv.validated[Ingest]
                  case `jsonArchiveFlag` => jv.validated[Archive]
                  case `jsonIngestMessageFlag`  => jv.validated[Ingest]("ingest")
                  case `jsonArchiveMessageFlag` => jv.validated[Archive]("archive")
                  case `storeFileFlag` => jv.validated[StoreFile]
                }
    } yield event
  }
}

class KafkaEventMessageCodec extends Encoder[EventMessage] {
  def toMessage(msg: EventMessage) = {
    val msgBuf = EventMessageEncoding.toMessageBytes(msg)
    val byteArray = new Array[Byte](msgBuf.limit)
    msgBuf.get(byteArray)
    // If you attempt to simply create the Message passing it the byte buffer,
    // as one of its constructors permits, the Kafka internal "magic byte"
    // and checksum do not get integrated, so things blow up deep in the internals
    // of Kafka. Demand PETA action now to save the kittens!
    new Message(byteArray)
  }
}

object EventMessageEncoding extends EncodingFlags with Logging {
  def toMessageBytes(msg: EventMessage) = {
    val serialized = msg.serialize.renderCompact
    logger.trace("Serialized event " + msg + " to " + serialized)
    val msgBuffer = charset.encode(serialized)
    val bytes = ByteBuffer.allocate(msgBuffer.limit + 3)
    writeHeader(bytes, msg.fold(_ => jsonIngestMessageFlag, _ => jsonArchiveMessageFlag, _ => storeFileFlag))
    bytes.put(msgBuffer)
    bytes.flip()
    bytes
  }

  def write(buffer: ByteBuffer, msg: EventMessage) {
    buffer.put(toMessageBytes(msg))
  }

  import EventMessage.EventMessageExtraction

  def read(buffer: ByteBuffer): Validation[Error, EventMessageExtraction] = {
    for {
      msgType <- readHeader(buffer)
      //_ = println(java.nio.charset.Charset.forName("UTF-8").decode(buffer).toString)
      jv <- ((Error.thrown _) <-: JParser.parseFromByteBuffer(buffer))
      message <-  msgType match {
        case `jsonIngestMessageFlag`  => jv.validated[EventMessageExtraction](IngestMessage.Extractor)
        case `jsonArchiveMessageFlag` => jv.validated[ArchiveMessage].map(\/.right(_))
        case `storeFileFlag`          => jv.validated[StoreFileMessage].map(\/.right(_))
      }
    } yield message
  }
}
