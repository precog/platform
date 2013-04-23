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
import blueeyes.json.serialization.Extractor.Error
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.json.serialization.IsoSerialization._
import blueeyes.json.serialization.Versioned._
import blueeyes.json.serialization.JodaSerializationImplicits.{InstantExtractor, InstantDecomposer}

import org.joda.time.Instant

import scalaz._
import scalaz.Validation._
import scalaz.std.option._
import scalaz.syntax.plus._
import scalaz.syntax.applicative._
import scalaz.syntax.validation._

import shapeless._

case class SplitMeta(idx: Int, total: Int)
object SplitMeta {
  implicit val splitMetaIso = Iso.hlist(SplitMeta.apply _, SplitMeta.unapply _)
  val schemaV1 = "idx" :: "total" :: HNil
  implicit def extractor = extractorV[SplitMeta](schemaV1, Some("1.0"))
  implicit def decomposer = decomposerV[SplitMeta](schemaV1, Some("1.0"))
}

sealed trait Event {
  def fold[A](ingest: Ingest => A, archive: Archive => A, storeFile: StoreFile => A): A
  def split(n: Int): List[Event]
  def length: Int
  def splitMeta: Option[SplitMeta]
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
case class Ingest(apiKey: APIKey, path: Path, writeAs: Option[Authorities], data: Seq[JValue], jobId: Option[JobId], timestamp: Instant, splitMeta: Option[SplitMeta]) extends Event {
  def fold[A](ingest: Ingest => A, archive: Archive => A, storeFile: StoreFile => A): A = ingest(this)

  def split(n: Int): List[Event] = {
    val splitSize = (data.length / n) max 1
    data.grouped(splitSize).map(d => this.copy(data = d))(collection.breakOut)
  }

  def length = data.length
}

object Ingest {
  implicit val eventIso = Iso.hlist(Ingest.apply _, Ingest.unapply _)

  val schemaV1 = "apiKey" :: "path" :: "writeAs" :: "data" :: "jobId" :: "timestamp" :: "splitMeta" :: HNil
  implicit def seqExtractor[A: Extractor]: Extractor[Seq[A]] = implicitly[Extractor[List[A]]].map(_.toSeq)

  val decomposerV1: Decomposer[Ingest] = decomposerV[Ingest](schemaV1, Some("1.0".v))
  val extractorV1: Extractor[Ingest] = extractorV[Ingest](schemaV1, Some("1.0".v))

  // A transitionary format similar to V1 structure, but lacks a version number and only carries a single data element
  val extractorV1a = new Extractor[Ingest] {
    def validated(obj: JValue): Validation[Error, Ingest] = {
      ( obj.validated[APIKey]("apiKey") |@|
        obj.validated[Path]("path") |@|
        obj.validated[Option[AccountId]]("ownerAccountId") ) { (apiKey, path, ownerAccountId) =>
          val jv = (obj \ "data")
          Ingest(apiKey, path, ownerAccountId.map(Authorities(_)), if (jv == JUndefined) Vector() else Vector(jv), None, EventMessage.defaultTimestamp)
        }
    }
  }

  val extractorV0 = new Extractor[Ingest] {
    def validated(obj: JValue): Validation[Error, Ingest] = {
      ( obj.validated[String]("tokenId") |@|
        obj.validated[Path]("path") ) { (apiKey, path) =>
          val jv = (obj \ "data")
          Ingest(apiKey, path, None, if (jv == JUndefined) Vector() else Vector(jv), None, EventMessage.defaultTimestamp)
        }
    }
  }

  implicit val Decomposer: Decomposer[Ingest] = decomposerV1
  implicit val Extractor: Extractor[Ingest] = extractorV1 <+> extractorV1a <+> extractorV0
}

case class Archive(apiKey: APIKey, path: Path, jobId: Option[JobId], timestamp: Instant) extends Event {
  def fold[A](ingest: Ingest => A, archive: Archive => A, storeFile: StoreFile => A): A = archive(this)
  def split(n: Int) = List(this) // can't split an archive
  val splitMeta = None
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

  implicit val Decomposer: Decomposer[Archive] = decomposerV1
  implicit val Extractor: Extractor[Archive] = extractorV1 <+> extractorV0
}

case class StoreFile(apiKey: APIKey, path: Path, jobId: JobId, content: Array[Byte], encoding: ContentEncoding, timestamp: Instant, splitMeta: Option[SplitMeta]) extends EventMessage {
  def fold[A](ingest: Ingest => A, archive: Archive => A, storeFile: StoreFile => A): A
  def split(n: Int) = {
    val splitSize = content.length / n
    content.grouped(splitSize).map(d => this.copy(data = d))(collection.breakOut)
  }
    
  def length = content.length
}

object StoreFile {

}


// vim: set ts=4 sw=4 et:
