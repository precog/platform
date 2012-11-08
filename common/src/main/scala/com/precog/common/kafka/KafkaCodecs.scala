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

import com.precog.common._

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

class KafkaIngestMessageCodec extends Encoder[IngestMessage] with Decoder[IngestMessage] {
  def toMessage(message: IngestMessage) = {
    new Message(IngestMessageSerialization.toBytes(message))
  }

  def toEvent(msg: Message) = {
    IngestMessageSerialization.read(msg.payload)
  }
}

class KafkaEventCodec extends Encoder[Event] with Decoder[Event] {
  val charset = Charset.forName("UTF-8")
 
  def toMessage(event: Event) = {
    val msgBuffer = charset.encode(event.serialize.renderCompact)
    val byteArray = new Array[Byte](msgBuffer.limit)
    msgBuffer.get(byteArray)
    new Message(byteArray)
  }

  def toEvent(msg: Message): Event = {
    val decoder = charset.newDecoder
    val charBuffer = decoder.decode(msg.payload)
    val jvalue = JParser.parse(charBuffer.toString()) 
    jvalue.validated[Event] match {
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
