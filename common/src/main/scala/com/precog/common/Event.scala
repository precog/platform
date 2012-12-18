package com.precog.common

import security._
import json._

import blueeyes.json.{ JPath, JValue }
import blueeyes.json.serialization.{Extractor, ValidatedExtraction}
import blueeyes.json.serialization.DefaultSerialization._

import scalaz._
import scalaz.Scalaz._
import scalaz.std.option._
import scalaz.std.map._
import scalaz.std.set._

import shapeless._

sealed trait Action

case class Event(apiKey: APIKey, path: Path, ownerAccountId: Option[AccountId], data: JValue, metadata: Map[JPath, Set[UserMetadata]]) extends Action 

object Event {
  implicit val eventIso = Iso.hlist(Event.apply _, Event.unapply _)
  implicit val accountIdMonoid = implicitly[Monoid[Option[AccountId]]]
  implicit val metadataMonoid = implicitly[Monoid[Map[JPath, Set[UserMetadata]]]]


  val v1Schema = "apiKey"  :: "path" :: "ownerAccountId" :: "data" :: ("metadata" ||| Map.empty[JPath, Set[UserMetadata]]) :: HNil
  @deprecated("V0 serialization schemas should be removed when legacy data is no longer needed", "2.1.5")
  val v0Schema = "tokenId" :: "path" :: Omit             :: "data" :: ("metadata" ||| Map.empty[JPath, Set[UserMetadata]]) :: HNil
  
  implicit val eventDecomposer = decomposer[Event](v1Schema)

  val v1EventExtractor = extractor[Event](v1Schema)
  val v0EventExtractor = extractor[Event](v0Schema)

  implicit val eventExtractor = new Extractor[Event] with ValidatedExtraction[Event] {
    override def validated(obj: JValue) = v1EventExtractor.validated(obj) orElse v0EventExtractor.validated(obj)
  }

  def fromJValue(apiKey: APIKey, path: Path, ownerAccountId: Option[AccountId], data: JValue): Event = {
    Event(apiKey, path, ownerAccountId, data, Map[JPath, Set[UserMetadata]]())
  }
}

case class Archive(path: Path, apiKey: String) extends Action

object Archive {
  implicit val archiveIso = Iso.hlist(Archive.apply _, Archive.unapply _)

  val v1Schema = "path" :: "apiKey" :: HNil
  @deprecated("V0 serialization schemas should be removed when legacy data is no longer needed", "2.1.5")
  val v0Schema = "path" :: "tokenId" :: HNil
  
  implicit val archiveDecomposer = decomposer[Archive](v1Schema)
  val v1ArchiveExtractor = extractor[Archive](v1Schema)
  val v0ArchiveExtractor = extractor[Archive](v0Schema)

  implicit val archiveExtractor = new Extractor[Archive] with ValidatedExtraction[Archive] {
    override def validated(obj: JValue) = (v1ArchiveExtractor.validated(obj) orElse v0ArchiveExtractor.validated(obj)).map {
      // FIXME: This is a complete hack to work around an accidental mis-ordering of fields for serialization
      archive => if (archive.apiKey.startsWith("/")) { 
        Archive(Path(archive.apiKey), archive.path.components.head.toString)
      } else { 
        archive
      }
    }
  }
}

// vim: set ts=4 sw=4 et:
