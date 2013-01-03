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

case class Ingest(apiKey: APIKey, path: Path, ownerAccountId: Option[AccountId], data: Vector[JValue], jobId: Option[JobId]) extends Event {
  def fold[A](ingest: Ingest => A, archive: Archive => A): A = ingest(this)
}

object Ingest {
  implicit val eventIso = Iso.hlist(Ingest.apply _, Ingest.unapply _)
  
  val schemaV1 = "apiKey" :: "path" :: "ownerAccountId" :: "data" :: "jobId" :: HNil
  
  val decomposerV1: Decomposer[Ingest] = decomposerV[Ingest](schemaV1, Some("1.0"))
  val extractorV1: Extractor[Ingest] = extractorV[Ingest](schemaV1, Some("1.0")) <+> extractorV1a

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
  implicit val Extractor: Extractor[Ingest] = extractorV1 <+> extractorV0
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
