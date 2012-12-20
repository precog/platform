package com.precog.common
package kafka

import com.precog.common.ingest._

import blueeyes.json.JParser
import blueeyes.json.serialization.Extractor._
import blueeyes.json.serialization.DefaultSerialization._

import _root_.kafka.message._
import _root_.kafka.serializer._

import java.nio.charset.Charset
import java.nio.ByteBuffer

import scalaz._
import scalaz.Validation._
import scalaz.syntax.bifunctor._

trait EncodingFlags {
  val charset = Charset.forName("UTF-8")

  val stopByte: Byte = 0x00
  val jsonIngestMessageFlag: Byte = 0x01
  val jsonArchiveMessageFlag: Byte = 0x03
  val jsonIngestFlag: Byte = 0x04
  val jsonArchiveFlag: Byte = 0x05
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

object EventEncoding extends EncodingFlags {
  def toMessageBytes(event: Event) = {
    val msgBuffer = charset.encode(event.serialize.renderCompact)
    val bytes = ByteBuffer.allocate(msgBuffer.limit + 3)
    writeHeader(bytes, event.fold(_ => jsonIngestFlag, _ => jsonArchiveFlag))
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
                  case `jsonIngestMessageFlag`  => (jv \ "event").validated[Ingest]
                  case `jsonArchiveMessageFlag` => (jv \ "archive").validated[Archive]
                }
    } yield event
  }
}

object EventMessageEncoding extends EncodingFlags {
  def toMessageBytes(msg: EventMessage) = {
    val msgBuffer = charset.encode(msg.serialize.renderCompact)
    val bytes = ByteBuffer.allocate(msgBuffer.limit + 3)
    writeHeader(bytes, msg.fold(_ => jsonIngestMessageFlag, _ => jsonArchiveMessageFlag))
    bytes.put(msgBuffer)
    bytes.flip()
    bytes
  }

  def write(buffer: ByteBuffer, msg: EventMessage) {
    buffer.put(toMessageBytes(msg))
  }
  
  def read(buffer: ByteBuffer): Validation[Error, EventMessage] = {
    for {
      msgType <- readHeader(buffer) 
      jv <- ((Error.thrown _) <-: JParser.parseFromByteBuffer(buffer)) 
      message <-  msgType match {
                    case `jsonIngestMessageFlag`  => jv.validated[IngestMessage]
                    case `jsonArchiveMessageFlag` => jv.validated[ArchiveMessage]
                  }
    } yield message
  }
}
