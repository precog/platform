package com.precog.common
package security

import json._

import blueeyes.json.serialization.DefaultSerialization.{ DateTimeDecomposer => _, DateTimeExtractor => _, _ }

import scalaz.Scalaz._

import shapeless._

case class APIKeyRecord(
  apiKey:         APIKey,
  name:           Option[String],
  description:    Option[String],
  issuerKey:      Option[APIKey],
  grants:         Set[GrantID],
  isRoot:         Boolean)

object APIKeyRecord {
  implicit val apiKeyRecordIso = Iso.hlist(APIKeyRecord.apply _, APIKeyRecord.unapply _)
  
  val schema =     "apiKey" :: "name" :: "description" :: "issuerKey" :: "grants" :: "isRoot" :: HNil
  val safeSchema = "apiKey" :: "name" :: "description" :: Omit        :: "grants" :: "isRoot" :: HNil
  
  object Serialization {
    implicit val (apiKeyRecordDecomposer, apiKeyRecordExtractor) = serialization[APIKeyRecord](schema)
  }
  
  object SafeSerialization {
    implicit val (safeAPIKeyRecordDecomposer, safeAPIKeyRecordExtractor) = serialization[APIKeyRecord](safeSchema)
  }
}

// vim: set ts=4 sw=4 et:
