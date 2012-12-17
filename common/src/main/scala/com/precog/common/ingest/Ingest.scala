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

sealed trait Action

case class Ingest(apiKey: APIKey, path: Path, ownerAccountId: Option[AccountId], data: Vector[JValue], jobId: Option[JobId]) extends Action 

object Ingest {
  implicit val eventIso = Iso.hlist(Ingest.apply _, Ingest.unapply _)
  
  val schemaV1 = "apiKey" :: "path" :: "ownerAccountId" :: "data" :: "jobId" :: HNil
  
  val (decomposerV1, extractorV1) = serializationV[Ingest](schemaV1, Some("1.0"))

  val extractorV0 = new Extractor[Ingest] with ValidatedExtraction[Ingest] {
    override def validated(obj: JValue): Validation[Error, Ingest] = {
      ( (obj \ "tokenId").validated[String] |@| 
        (obj \ "path").validated[Path] ) { (apiKey, path) => 
        val jv = (obj \ "data")
        Ingest(apiKey, path, None, if (jv == JUndefined) Vector() else Vector(jv), None) 
      }
    }
  }

  implicit val Decomposer: Decomposer[Ingest] = decomposerV1
  implicit val Extractor: Extractor[Ingest] = extractorV1 <+> extractorV0
}

case class Archive(apiKey: APIKey, path: Path, jobId: Option[JobId]) extends Action

object Archive {
  implicit val archiveIso = Iso.hlist(Archive.apply _, Archive.unapply _)

  val schemaV1 = "apiKey" :: "path" :: "jobId" :: HNil
  val schemaV0 = "tokenId" :: "path" :: Omit :: HNil
  
  val decomposerV1: Decomposer[Archive] = decomposerV[Archive](schemaV1, Some("1.0"))
  val extractorV1: Extractor[Archive] = extractorV[Archive](schemaV1, Some("1.0"))
  val extractorV0: Extractor[Archive] = extractorV[Archive](schemaV0, None)

  implicit val Decomposer: Decomposer[Archive] = decomposerV1
  implicit val Extractor: Extractor[Archive] = extractorV1 <+> extractorV0
}

// vim: set ts=4 sw=4 et:
