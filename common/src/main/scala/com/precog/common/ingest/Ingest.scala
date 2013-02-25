package com.precog.common
package ingest

import accounts.AccountId
import security.APIKey
import jobs.JobId
import json._

import blueeyes.json.{ JPath, JValue, JUndefined }
import blueeyes.json.serialization._
import blueeyes.json.serialization.Extractor.Error
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.json.serialization.IsoSerialization._

import scalaz.Validation
import scalaz.Validation._
import scalaz.std.option._
import scalaz.syntax.plus._
import scalaz.syntax.applicative._
import scalaz.syntax.validation._

import shapeless._

sealed trait Event {
  def fold[A](ingest: Ingest => A, archive: Archive => A): A
}

object Event {
  implicit val decomposer: Decomposer[Event] = new Decomposer[Event] {
    override def decompose(event: Event): JValue = {
      event.fold(_.serialize, _.serialize)
    }
  }
}

case class Ingest(apiKey: APIKey, path: Path, ownerAccountId: Option[AccountId], data: Seq[JValue], jobId: Option[JobId]) extends Event {
  def fold[A](ingest: Ingest => A, archive: Archive => A): A = ingest(this)
}

object Ingest {
  implicit val eventIso = Iso.hlist(Ingest.apply _, Ingest.unapply _)

  val schemaV1 = "apiKey" :: "path" :: "ownerAccountId" :: "data" :: "jobId" :: HNil
  implicit def seqExtractor[A: Extractor]: Extractor[Seq[A]] = implicitly[Extractor[List[A]]].map(_.toSeq)

  val decomposerV1: Decomposer[Ingest] = decomposerV[Ingest](schemaV1, Some("1.0"))
  val extractorV1: Extractor[Ingest] = extractorV[Ingest](schemaV1, Some("1.0"))

  // A transitionary format similar to V1 structure, but lacks a version number and only carries a single data element
  val extractorV1a = new Extractor[Ingest] {
    def validated(obj: JValue): Validation[Error, Ingest] = {
      ( obj.validated[APIKey]("apiKey") |@|
        obj.validated[Path]("path") |@|
        obj.validated[Option[AccountId]]("ownerAccountId") ) { (apiKey, path, ownerAccountId) =>
          val jv = (obj \ "data")
          Ingest(apiKey, path, ownerAccountId, if (jv == JUndefined) Vector() else Vector(jv), None)
        }
    }
  }

  val extractorV0 = new Extractor[Ingest] {
    def validated(obj: JValue): Validation[Error, Ingest] = {
      ( obj.validated[String]("tokenId") |@|
        obj.validated[Path]("path") ) { (apiKey, path) =>
          val jv = (obj \ "data")
          Ingest(apiKey, path, None, if (jv == JUndefined) Vector() else Vector(jv), None)
        }
    }
  }

  implicit val Decomposer: Decomposer[Ingest] = decomposerV1
  implicit val Extractor: Extractor[Ingest] = extractorV1 <+> extractorV1a <+> extractorV0
}

case class Archive(apiKey: APIKey, path: Path, jobId: Option[JobId]) extends Event {
  def fold[A](ingest: Ingest => A, archive: Archive => A): A = archive(this)
}

object Archive {
  implicit val archiveIso = Iso.hlist(Archive.apply _, Archive.unapply _)

  val schemaV1 = "apiKey" :: "path" :: "jobId" :: HNil
  val schemaV0 = "tokenId" :: "path" :: Omit :: HNil

  val decomposerV1: Decomposer[Archive] = decomposerV[Archive](schemaV1, Some("1.0"))
  val extractorV1: Extractor[Archive] = extractorV[Archive](schemaV1, Some("1.0")) <+> extractorV1a

  // Support un-versioned V1 schemas and out-of-order fields due to an earlier bug
  val extractorV1a: Extractor[Archive] = extractorV[Archive](schemaV1, None) map {
    // FIXME: This is a complete hack to work around an accidental mis-ordering of fields for serialization
    case Archive(apiKey, path, jobId) if (apiKey.startsWith("/")) =>
      Archive(path.components.head.toString, Path(apiKey), jobId)

    case ok => ok
  }

  val extractorV0: Extractor[Archive] = extractorV[Archive](schemaV0, None)

  implicit val Decomposer: Decomposer[Archive] = decomposerV1
  implicit val Extractor: Extractor[Archive] = extractorV1 <+> extractorV0
}

// vim: set ts=4 sw=4 et:
