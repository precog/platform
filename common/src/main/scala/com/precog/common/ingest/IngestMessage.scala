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
package ingest

import accounts.AccountId
import security._
import com.precog.common.serialization._
import jobs.JobId

import blueeyes.core.http.MimeType
import blueeyes.json._
import blueeyes.json.serialization._
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.json.serialization.IsoSerialization._
import blueeyes.json.serialization.Extractor._
import blueeyes.json.serialization.Versioned._
import blueeyes.json.serialization.JodaSerializationImplicits.{InstantExtractor, InstantDecomposer}

import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.util.UUID

import org.joda.time.Instant

import scalaz._
import scalaz.Validation._
import scalaz.syntax.apply._
import scalaz.syntax.bifunctor._
import scalaz.syntax.plus._
import scalaz.syntax.validation._

import shapeless._

sealed trait EventMessage {
  def apiKey: APIKey
  def path: Path
  def jobId: Option[JobId]
  def timestamp: Instant
  def fold[A](im: IngestMessage => A, am: ArchiveMessage => A, sf: StoreFileMessage => A): A
}

object EventMessage {
  type EventMessageExtraction = (APIKey, Path, Authorities => EventMessage) \/ EventMessage

  // an instant that's close enough to the start of timestamping for our purposes
  val defaultTimestamp = new Instant(1362465101979L)

  implicit val decomposer: Decomposer[EventMessage] = new Decomposer[EventMessage] {
    override def decompose(eventMessage: EventMessage): JValue = {
      eventMessage.fold(IngestMessage.Decomposer.apply _, ArchiveMessage.Decomposer.apply _, StoreFileMessage.Decomposer.apply _)
    }
  }
}

case class EventId(producerId: ProducerId, sequenceId: SequenceId) {
  val uid = (producerId.toLong << 32) | (sequenceId.toLong & 0xFFFFFFFFL)
}

object EventId {
  implicit val iso = Iso.hlist(EventId.apply _, EventId.unapply _)

  val schemaV1 = "producerId" :: "sequenceId" :: HNil

  implicit val (decomposerV1, extractorV1) = serializationV[EventId](schemaV1, Some("1.0".v))

  def fromLong(id: Long): EventId = EventId(producerId(id), sequenceId(id))

  def producerId(id: Long) = (id >> 32).toInt
  def sequenceId(id: Long) = id.toInt
}

case class IngestRecord(eventId: EventId, value: JValue)

object IngestRecord {
  implicit val ingestRecordIso = Iso.hlist(IngestRecord.apply _, IngestRecord.unapply _)

  val schemaV1 = "eventId" :: "jvalue" :: HNil

  implicit val (decomposerV1, extractorV1) = serializationV[IngestRecord](schemaV1, Some("1.0".v))
}

/**
 * ownerAccountId must be determined before the message is sent to the central queue; we have to
 * accept records for processing in the local queue.
 */
case class IngestMessage(apiKey: APIKey, path: Path, writeAs: Authorities, data: Seq[IngestRecord], jobId: Option[JobId], timestamp: Instant, streamRef: StreamRef) extends EventMessage {
  def fold[A](im: IngestMessage => A, am: ArchiveMessage => A, sf: StoreFileMessage => A): A = im(this)
  def split: Seq[IngestMessage] = {
    if (data.size > 1) {
      val (dataA, dataB) = data.splitAt(data.size / 2)
      val Seq(refA, refB) = streamRef.split(2)
      List(this.copy(data = dataA, streamRef = refA), this.copy(data = dataB, streamRef = refB))
    } else {
      List(this)
    }
  }

  override def toString = "IngestMessage(%s, %s, %s, (%d records), %s, %s, %s)".format(apiKey, path, writeAs, data.size, jobId, timestamp, streamRef)
}

object IngestMessage {
  import EventMessage._

  implicit val ingestMessageIso = Iso.hlist(IngestMessage.apply _, IngestMessage.unapply _)

  val schemaV1 = "apiKey"  :: "path" :: "writeAs" :: "data" :: "jobId" :: "timestamp" :: "streamRef" :: HNil
  implicit def seqExtractor[A: Extractor]: Extractor[Seq[A]] = implicitly[Extractor[List[A]]].map(_.toSeq)

  val decomposerV1: Decomposer[IngestMessage] = decomposerV[IngestMessage](schemaV1, Some("1.1".v))
  val extractorV1: Extractor[EventMessageExtraction] = new Extractor[EventMessageExtraction] {
    private val extractor = extractorV[IngestMessage](schemaV1, Some("1.1".v))
    override def validated(jv: JValue) = extractor.validated(jv).map(\/.right(_))
  }

  val extractorV0: Extractor[EventMessageExtraction] = new Extractor[EventMessageExtraction] {
    override def validated(obj: JValue): Validation[Error, EventMessageExtraction] =
      obj.validated[Ingest]("event").flatMap { ingest =>
        (obj.validated[Int]("producerId") |@|
         obj.validated[Int]("eventId")) { (producerId, sequenceId) =>
          val eventRecords = ingest.data map { jv => IngestRecord(EventId(producerId, sequenceId), jv) }
          ingest.writeAs map { authorities =>
            assert(ingest.data.size == 1)
            \/.right(IngestMessage(ingest.apiKey, ingest.path, authorities, eventRecords, ingest.jobId, defaultTimestamp, StreamRef.Append))
          } getOrElse {
            \/.left(
              (ingest.apiKey, ingest.path, (authorities: Authorities) =>
                IngestMessage(ingest.apiKey, ingest.path, authorities, eventRecords, ingest.jobId, defaultTimestamp, StreamRef.Append))
            )
          }
        }
    }
  }

  implicit val Decomposer: Decomposer[IngestMessage] = decomposerV1
  implicit val Extractor: Extractor[EventMessageExtraction] = extractorV1 <+> extractorV0
}

case class ArchiveMessage(apiKey: APIKey, path: Path, jobId: Option[JobId], eventId: EventId, timestamp: Instant) extends EventMessage {
  def fold[A](im: IngestMessage => A, am: ArchiveMessage => A, sf: StoreFileMessage => A): A = am(this)
}

object ArchiveMessage {
  import EventMessage._
  implicit val archiveMessageIso = Iso.hlist(ArchiveMessage.apply _, ArchiveMessage.unapply _)

  val schemaV1 = "apiKey" :: "path" :: "jobId" :: "eventId" :: "timestamp" :: HNil

  val decomposerV1: Decomposer[ArchiveMessage] = decomposerV[ArchiveMessage](schemaV1, Some("1.0".v))
  val extractorV1: Extractor[ArchiveMessage] = extractorV[ArchiveMessage](schemaV1, Some("1.0".v))
  val extractorV0: Extractor[ArchiveMessage] = new Extractor[ArchiveMessage] {
    override def validated(obj: JValue): Validation[Error, ArchiveMessage] = {
      (obj.validated[Int]("producerId") |@|
       obj.validated[Int]("deletionId") |@|
       obj.validated[Archive]("deletion")) { (producerId, sequenceId, archive) =>
        ArchiveMessage(archive.apiKey, archive.path, archive.jobId, EventId(producerId, sequenceId), defaultTimestamp)
      }
    }
  }

  implicit val Decomposer: Decomposer[ArchiveMessage] = decomposerV1
  implicit val Extractor: Extractor[ArchiveMessage] = extractorV1 <+> extractorV0
}

case class StoreFileMessage(apiKey: APIKey, path: Path, writeAs: Authorities, jobId: Option[JobId], eventId: EventId, content: FileContent, timestamp: Instant, streamRef: StreamRef) extends EventMessage {
  def fold[A](im: IngestMessage => A, am: ArchiveMessage => A, sf: StoreFileMessage => A): A = sf(this)
}

object StoreFileMessage {
  implicit val storeFileMessageIso = Iso.hlist(StoreFileMessage.apply _, StoreFileMessage.unapply _)

  val schemaV1 = "apiKey" :: "path" :: "writeAs" :: "jobId" :: "eventId" :: "content" :: "timestamp" :: "streamRef" :: HNil

  implicit val Decomposer: Decomposer[StoreFileMessage] = decomposerV[StoreFileMessage](schemaV1, Some("1.0".v))

  implicit val Extractor: Extractor[StoreFileMessage] = extractorV[StoreFileMessage](schemaV1, Some("1.0".v))
}

