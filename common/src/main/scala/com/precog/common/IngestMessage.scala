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

import java.nio.ByteBuffer
import java.nio.charset.Charset

import blueeyes.json._
import blueeyes.json.serialization.{ ValidatedExtraction, Extractor, Decomposer }
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.json.serialization.Extractor._

import scalaz._
import scalaz.syntax.apply._

sealed trait IngestMessage

class IngestMessageSerialization {
  implicit val IngestMessageDecomposer: Decomposer[IngestMessage] = new Decomposer[IngestMessage] {
    override def decompose(ingestMessage: IngestMessage): JValue = ingestMessage match {
      //case sm @ SyncMessage(_, _, _)  => SyncMessage.SyncMessageDecomposer.apply(sm)
      case em @ EventMessage(_, _) => EventMessage.EventMessageDecomposer.apply(em)
      case dm @ ArchiveMessage(_, _) => ArchiveMessage.ArchiveMessageDecomposer.apply(dm)
    }
  }
}

object IngestMessage extends IngestMessageSerialization


case class EventId(producerId: ProducerId, sequenceId: SequenceId) {
  val uid = (producerId.toLong << 32) | (sequenceId.toLong & 0xFFFFFFFFL)
}

case class EventMessage(eventId: EventId, event: Event) extends IngestMessage

trait EventMessageSerialization {
  implicit val EventMessageDecomposer: Decomposer[EventMessage] = new Decomposer[EventMessage] {
    override def decompose(eventMessage: EventMessage): JValue = JObject(
      List(
        JField("producerId", eventMessage.eventId.producerId.serialize),
        JField("eventId", eventMessage.eventId.sequenceId.serialize),
        JField("schemaVersion", JString("1")),
        JField("event", eventMessage.event.serialize)))
  }

  implicit val EventMessageExtractor: Extractor[EventMessage] = new Extractor[EventMessage] with ValidatedExtraction[EventMessage] {
    override def validated(obj: JValue): Validation[Error, EventMessage] =
      ((obj \ "producerId" ).validated[Int] |@|
        (obj \ "eventId").validated[Int] |@| {
          (obj \ "event").validated[Event] orElse Event.legacyEventExtractor.validated(obj \ "event")
        }).apply(EventMessage(_, _, _))
  }
}

object EventMessage extends EventMessageSerialization {
  def apply(producerId: ProducerId, sequenceId: SequenceId, event: Event): EventMessage = {
    EventMessage(EventId(producerId, sequenceId), event)
  }
}


case class ArchiveId(producerId: ProducerId, sequenceId: SequenceId) {
  val uid = (producerId.toLong << 32) | (sequenceId.toLong & 0xFFFFFFFFL)
}

case class ArchiveMessage(archiveId: ArchiveId, archive: Archive) extends IngestMessage

trait ArchiveMessageSerialization {
  implicit val ArchiveMessageDecomposer: Decomposer[ArchiveMessage] = new Decomposer[ArchiveMessage] {
    override def decompose(archiveMessage: ArchiveMessage): JValue = JObject(
      List(
        JField("producerId", archiveMessage.archiveId.producerId.serialize),
        JField("deletionId", archiveMessage.archiveId.sequenceId.serialize),
        JField("schemaVersion", JString("1")),
        JField("deletion", archiveMessage.archive.serialize)))
  }

  implicit val ArchiveMessageExtractor: Extractor[ArchiveMessage] = new Extractor[ArchiveMessage] with ValidatedExtraction[ArchiveMessage] {
    override def validated(obj: JValue): Validation[Error, ArchiveMessage] =
      ((obj \ "producerId" ).validated[Int] |@|
        (obj \ "deletionId").validated[Int] |@| {
          (obj \ "deletion").validated[Archive] orElse Archive.legacyArchiveExtractor.validated(obj \ "event")
        }).apply(ArchiveMessage(_, _, _))
  }
}

object ArchiveMessage extends ArchiveMessageSerialization {

  def apply(producerId: ProducerId, sequenceId: SequenceId, archive: Archive): ArchiveMessage = {
    ArchiveMessage(ArchiveId(producerId, sequenceId), archive)
  }
}


object IngestMessageSerialization {
  private val stopByte: Byte = 0x00
  private val jsonEventFlag: Byte = 0x01
  private val jsonSyncFlag: Byte = 0x02
  private val jsonArchiveFlag: Byte = 0x03
  private val magicByte: Byte = -123

  val charset = Charset.forName("UTF-8")

  def toBytes(msg: IngestMessage): Array[Byte] = {
    val buf = ByteBuffer.allocate(1024*1024)
    write(buf, msg)
    buf.flip()
    val result = new Array[Byte](buf.limit)
    buf.get(result)
    result
  }

  def fromBytes(bytes: Array[Byte]): IngestMessage = {
    read(ByteBuffer.wrap(bytes))
  }

  def write(buffer: ByteBuffer, msg: IngestMessage) {
    (msg match {
      //case SyncMessage(_, _, _)    => writeSync _
      case EventMessage(_, _)      => writeEvent _
      case ArchiveMessage(_, _)    => writeArchive _
    })(msg)(buffer)
  }
  
  def writeEvent(msg: IngestMessage): ByteBuffer => ByteBuffer = (writeHeader(_: ByteBuffer, jsonEventFlag)) andThen 
                                                                 (writeMessage(_: ByteBuffer, msg))
  
  def writeArchive(msg: IngestMessage): ByteBuffer => ByteBuffer = (writeHeader(_: ByteBuffer, jsonArchiveFlag)) andThen 
                                                                   (writeMessage(_: ByteBuffer, msg))
  
  def writeHeader(buffer: ByteBuffer, encodingFlag: Byte): ByteBuffer = {
    buffer.put(magicByte).put(encodingFlag).put(stopByte)
  }

  def writeMessage(buffer: ByteBuffer, msg: IngestMessage): ByteBuffer = {
    val msgBuffer = charset.encode(msg.serialize.renderCompact)
    buffer.put(msgBuffer)
  }

  def read(buf: ByteBuffer): IngestMessage = {
    readMessage(buf) match {
      case Failure(e)   => throw new RuntimeException("Ingest message parse error: " + e)
      case Success(msg) => msg
    }
  }

  def readMessage(buffer: ByteBuffer): Validation[String, IngestMessage] = {
    val magic = buffer.get()
    if (magic != magicByte) {
      Failure("Invalid message bad magic byte. Found [" + magic + "]")
    } else {
      val msgType = buffer.get()
      val stop    = buffer.get()
      
      (stop, msgType) match {
        case (`stopByte`, `jsonEventFlag`) => parseEvent(buffer)
        case (`stopByte`, `jsonArchiveFlag`) => parseArchive(buffer)
      }
    }
  }
  
  def parseEvent = (parseJValue _) andThen (jvalueToEvent _)

  def parseArchive = (parseJValue _) andThen (jvalueToArchive _)
  
  def jvalueToEvent(jvalue: JValue): Validation[String, IngestMessage] = {
    jvalue.validated[EventMessage] match {
      case Failure(e)  => Failure(e.message + " parsing: " + jvalue.renderCompact)
      case Success(em) => Success(em)
    }
  }

  def jvalueToArchive(jvalue: JValue): Validation[String, IngestMessage] = {
    jvalue.validated[ArchiveMessage] match {
      case Failure(e)  => Failure(e.message)
      case Success(am) => Success(am)
    }
  }
  
  def parseJValue(buffer: ByteBuffer): JValue = {
    val decoder = charset.newDecoder()
    val charBuffer = decoder.decode(buffer)
    JParser.parse(charBuffer.toString())
  }
}

// vim: set ts=4 sw=4 et:
