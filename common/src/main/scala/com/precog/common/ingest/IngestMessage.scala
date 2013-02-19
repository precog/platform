package com.precog.common
package ingest

import accounts.AccountId
import security.APIKey
import jobs.JobId
import json._

import blueeyes.json.{ JValue, JParser }
import blueeyes.json.serialization.{ Extractor, Decomposer }
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.json.serialization.IsoSerialization._
import blueeyes.json.serialization.Extractor._

import java.nio.ByteBuffer
import java.nio.charset.Charset

import scalaz._
import scalaz.Validation._
import scalaz.syntax.apply._
import scalaz.syntax.bifunctor._
import scalaz.syntax.plus._
import scalaz.syntax.validation._

import shapeless._


sealed trait EventMessage {
  def fold[A](im: IngestMessage => A, am: ArchiveMessage => A): A
}

object EventMessage {
  implicit val decomposer: Decomposer[EventMessage] = new Decomposer[EventMessage] {
    override def decompose(eventMessage: EventMessage): JValue = {
      eventMessage.fold(IngestMessage.Decomposer.apply _, ArchiveMessage.Decomposer.apply _)
    }
  }
}

case class EventId(producerId: ProducerId, sequenceId: SequenceId) {
  val uid = (producerId.toLong << 32) | (sequenceId.toLong & 0xFFFFFFFFL)
}

object EventId {
  implicit val iso = Iso.hlist(EventId.apply _, EventId.unapply _)

  val schemaV1 = "producerId" :: "sequenceId" :: HNil

  implicit val (decomposerV1, extractorV1) = serializationV[EventId](schemaV1, Some("1.0"))

  def fromLong(id: Long): EventId = EventId(producerId(id), sequenceId(id))

  def producerId(id: Long) = (id >> 32).toInt
  def sequenceId(id: Long) = id.toInt
}

case class IngestRecord(eventId: EventId, value: JValue)

object IngestRecord {
  implicit val ingestRecordIso = Iso.hlist(IngestRecord.apply _, IngestRecord.unapply _)

  val schemaV1 = "eventId" :: "jvalue" :: HNil

  implicit val (decomposerV1, extractorV1) = serializationV[IngestRecord](schemaV1, Some("1.0"))
}

/**
 * ownerAccountId must be determined before the message is sent to the central queue; we have to 
 * accept records for processing in the local queue.
 */
case class IngestMessage(apiKey: APIKey, path: Path, ownerAccountId: AccountId, data: Seq[IngestRecord], jobId: Option[JobId]) extends EventMessage {
  def fold[A](im: IngestMessage => A, am: ArchiveMessage => A): A = im(this)
}

object IngestMessage {
  //def fromIngest[M[+_]](ingest: Ingest, accountFinder: AccountFinder[M]): M[Validation[String, IngestMessage]] = {
  //  sys.error("todo")
  //}

  implicit val ingestMessageIso = Iso.hlist(IngestMessage.apply _, IngestMessage.unapply _)

  val schemaV1 = "apiKey"  :: "path" :: "ownerAccountId" :: "data" :: "jobId" :: HNil
  implicit def seqExtractor[A: Extractor]: Extractor[Seq[A]] = implicitly[Extractor[List[A]]].map(_.toSeq)

  val decomposerV1: Decomposer[IngestMessage] = decomposerV[IngestMessage](schemaV1, Some("1.0"))
  val extractorV1: Extractor[IngestMessage] = extractorV[IngestMessage](schemaV1, Some("1.0"))

  val extractorV0: Extractor[IngestMessage] = new Extractor[IngestMessage] {
    override def validated(obj: JValue): Validation[Error, IngestMessage] = {
      ( (obj \ "producerId" ).validated[Int] |@|
        (obj \ "eventId").validated[Int] |@|
        Ingest.extractorV0.validated(obj \ "event") ) { (producerId, sequenceId, ingest) =>
        assert(ingest.data.size == 1)
        val eventRecords = ingest.data map { jv => IngestRecord(EventId(producerId, sequenceId), jv) }
        IngestMessage(ingest.apiKey, ingest.path, ingest.ownerAccountId.getOrElse(sys.error("fixme")), eventRecords, ingest.jobId)
      }
    }
  }

  implicit val Decomposer: Decomposer[IngestMessage] = decomposerV1
  implicit val Extractor: Extractor[IngestMessage] = extractorV1 <+> extractorV0
}

case class ArchiveMessage(eventId: EventId, archive: Archive) extends EventMessage {
  def fold[A](im: IngestMessage => A, am: ArchiveMessage => A): A = am(this)
}

object ArchiveMessage {
  implicit val archiveMessageIso = Iso.hlist(ArchiveMessage.apply _, ArchiveMessage.unapply _)

  val schemaV1 = "eventId" :: "archive" :: HNil

  val decomposerV1: Decomposer[ArchiveMessage] = decomposerV[ArchiveMessage](schemaV1, Some("1.0"))
  val extractorV1: Extractor[ArchiveMessage] = extractorV[ArchiveMessage](schemaV1, Some("1.0"))
  val extractorV0: Extractor[ArchiveMessage] = new Extractor[ArchiveMessage] {
    override def validated(obj: JValue): Validation[Error, ArchiveMessage] = {
      ( (obj \ "producerId" ).validated[Int] |@|
        (obj \ "deletionId").validated[Int] |@|
        Archive.extractorV0.validated(obj \ "archive") ) { (producerId, sequenceId, archive) =>
        ArchiveMessage(EventId(producerId, sequenceId), archive)
      }
    }
  }

  implicit val Decomposer: Decomposer[ArchiveMessage] = decomposerV1
  implicit val Extractor: Extractor[ArchiveMessage] = extractorV1 <+> extractorV0
}
