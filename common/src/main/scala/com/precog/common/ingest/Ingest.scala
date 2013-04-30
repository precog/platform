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
import jobs.JobId

import blueeyes.json.{ JPath, JValue, JUndefined }
import blueeyes.json.serialization._
import blueeyes.json.serialization.Extractor._
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.json.serialization.IsoSerialization._
import blueeyes.json.serialization.Versioned._
import blueeyes.json.serialization.JodaSerializationImplicits.{InstantExtractor, InstantDecomposer}

import org.joda.time.Instant
import java.util.UUID

import scalaz._
import scalaz.Validation._
import scalaz.std.option._
import scalaz.syntax.plus._
import scalaz.syntax.applicative._
import scalaz.syntax.validation._

import shapeless._

object JavaSerialization {
  implicit val uuidDecomposer: Decomposer[UUID] = implicitly[Decomposer[String]].contramap((_: UUID).toString)
  implicit val uuidExtractor: Extractor[UUID] = implicitly[Extractor[String]].map(UUID.fromString)
}

import JavaSerialization._

sealed trait Event {
  def fold[A](ingest: Ingest => A, archive: Archive => A, storeFile: StoreFile => A): A
  def split(n: Int, streamId: UUID): List[Event]
  def length: Int
}

object Event {
  implicit val decomposer: Decomposer[Event] = new Decomposer[Event] {
    override def decompose(event: Event): JValue = {
      event.fold(_.serialize, _.serialize, _.serialize)
    }
  }
}

/**
 * If writeAs is None, then the downstream 
 */
case class Ingest(apiKey: APIKey, path: Path, writeAs: Option[Authorities], data: Seq[JValue], jobId: Option[JobId], timestamp: Instant, streamId: Option[UUID]) extends Event {
  def fold[A](ingest: Ingest => A, archive: Archive => A, storeFile: StoreFile => A): A = ingest(this)

  def split(n: Int, streamId: UUID): List[Event] = {
    val splitSize = (data.length / n) max 1
    data.grouped(splitSize).map(d => this.copy(data = d, streamId = Some(streamId))).toList
  }

  def length = data.length
}

object Ingest {
  implicit val eventIso = Iso.hlist(Ingest.apply _, Ingest.unapply _)

  val schemaV1 = "apiKey" :: "path" :: "writeAs" :: "data" :: "jobId" :: "timestamp" :: "streamId" :: HNil
  implicit def seqExtractor[A: Extractor]: Extractor[Seq[A]] = implicitly[Extractor[List[A]]].map(_.toSeq)

  val decomposerV1: Decomposer[Ingest] = decomposerV[Ingest](schemaV1, Some("1.1".v))
  val extractorV1: Extractor[Ingest] = extractorV[Ingest](schemaV1, Some("1.1".v))

  // A transitionary format similar to V1 structure, but lacks a version number and only carries a single data element
  val extractorV1a = new Extractor[Ingest] {
    def validated(obj: JValue): Validation[Error, Ingest] = {
      ( obj.validated[APIKey]("apiKey") |@|
        obj.validated[Path]("path") |@|
        obj.validated[Option[AccountId]]("ownerAccountId") ) { (apiKey, path, ownerAccountId) =>
          val jv = (obj \ "data")
          Ingest(apiKey, path, ownerAccountId.map(Authorities(_)), 
                 if (jv == JUndefined) Vector() else Vector(jv), 
                 None, EventMessage.defaultTimestamp, None)
        }
    }
  }

  val extractorV0 = new Extractor[Ingest] {
    def validated(obj: JValue): Validation[Error, Ingest] = {
      ( obj.validated[String]("tokenId") |@|
        obj.validated[Path]("path") ) { (apiKey, path) =>
          val jv = (obj \ "data")
          Ingest(apiKey, path, None, if (jv == JUndefined) Vector() else Vector(jv), None, EventMessage.defaultTimestamp, None)
        }
    }
  }

  implicit val decomposer: Decomposer[Ingest] = decomposerV1
  implicit val extractor: Extractor[Ingest] = extractorV1 <+> extractorV1a <+> extractorV0
}

case class Archive(apiKey: APIKey, path: Path, jobId: Option[JobId], timestamp: Instant) extends Event {
  def fold[A](ingest: Ingest => A, archive: Archive => A, storeFile: StoreFile => A): A = archive(this)
  def split(n: Int, streamId: UUID) = List(this) // can't split an archive
  def length = 1
}

object Archive {
  implicit val archiveIso = Iso.hlist(Archive.apply _, Archive.unapply _)

  val schemaV1 = "apiKey" :: "path" :: "jobId" :: ("timestamp" ||| EventMessage.defaultTimestamp) :: HNil
  val schemaV0 = "tokenId" :: "path" :: Omit :: ("timestamp" ||| EventMessage.defaultTimestamp) :: HNil

  val decomposerV1: Decomposer[Archive] = decomposerV[Archive](schemaV1, Some("1.0".v))
  val extractorV1: Extractor[Archive] = extractorV[Archive](schemaV1, Some("1.0".v)) <+> extractorV1a

  // Support un-versioned V1 schemas and out-of-order fields due to an earlier bug
  val extractorV1a: Extractor[Archive] = extractorV[Archive](schemaV1, None) map {
    // FIXME: This is a complete hack to work around an accidental mis-ordering of fields for serialization
    case Archive(apiKey, path, jobId, timestamp) if (apiKey.startsWith("/")) =>
      Archive(path.components.head.toString, Path(apiKey), jobId, timestamp)

    case ok => ok
  }

  val extractorV0: Extractor[Archive] = extractorV[Archive](schemaV0, None)

  implicit val decomposer: Decomposer[Archive] = decomposerV1
  implicit val extractor: Extractor[Archive] = extractorV1 <+> extractorV0
}

sealed trait StoreMode
object StoreMode {
  implicit val decomposer: Decomposer[StoreMode] = Decomposer[String].contramap[StoreMode] {
    case Create => "create"
    case Replace => "replace"
    case Append => "append"
  }

  implicit val extractor: Extractor[StoreMode] = Extractor[String].mapv {
    case "create" => Success(Create)
    case "replace" => Success(Replace)
    case "append" => Success(Append)
    case other => Failure(Invalid("Storage mode %s not recogized.".format(other)))
  }

  case object Create extends StoreMode
  case object Replace extends StoreMode
  case object Append extends StoreMode
}

case class StoreFile(apiKey: APIKey, path: Path, jobId: JobId, content: FileContent, timestamp: Instant, streamId: Option[UUID], mode: StoreMode) extends Event {
  def fold[A](ingest: Ingest => A, archive: Archive => A, storeFile: StoreFile => A): A = storeFile(this)
  def split(n: Int, streamId: UUID) = {
    val splitSize = content.data.length / n
    content.data.grouped(splitSize).map(d => this.copy(content = FileContent(d, content.mimeType, content.encoding), streamId = Some(streamId))).toList
  }
    
  def length = content.data.length
}

object StoreFile {
  import JavaSerialization._

  implicit val iso = Iso.hlist(StoreFile.apply _, StoreFile.unapply _)

  val schemaV1 = "apiKey" :: "path" :: "jobId" :: "content" :: "timestamp" :: "streamId" :: "mode" :: HNil

  implicit val decomposer: Decomposer[StoreFile] = decomposerV[StoreFile](schemaV1, Some("1.0".v))
  implicit val extractor: Extractor[StoreFile] = extractorV[StoreFile](schemaV1, Some("1.0".v))
}
