package com.precog.common

import security._
import json._

import blueeyes.json.{ JPath, JValue }
import blueeyes.json.serialization.DefaultSerialization._

import shapeless._

sealed trait Action

case class Event(apiKey: APIKey, path: Path, ownerAccountId: Option[AccountId], data: JValue, metadata: Map[JPath, Set[UserMetadata]]) extends Action 

object Event {
  implicit val eventIso = Iso.hlist(Event.apply _, Event.unapply _)
  
  val schema = "apiKey" :: "path" :: "ownerAccountId" :: "data" :: "metadata" :: HNil
  val legacySchema = "tokenId" :: "path" :: Omit :: "data" :: "metadata" :: HNil
  
  implicit val (eventDecomposer, eventExtractor) = serialization[Event](schema)
  val (_, legacyEventExtractor) = serialization[Event](legacySchema)

  def fromJValue(apiKey: APIKey, path: Path, ownerAccountId: Option[AccountId], data: JValue): Event = {
    Event(apiKey, path, ownerAccountId, data, Map[JPath, Set[UserMetadata]]())
  }
}

case class Archive(path: Path, apiKey: String) extends Action

object Archive {
  implicit val archiveIso = Iso.hlist(Archive.apply _, Archive.unapply _)

  val schema = "apiKey" :: "path" :: HNil
  val legacySchema = "tokenId" :: "path" :: HNil
  
  implicit val (archiveDecomposer, archiveExtractor) = serialization[Archive](schema)
  val (_, legacyArchiveExtractor) = serialization[Archive](legacySchema)
}

// vim: set ts=4 sw=4 et:
