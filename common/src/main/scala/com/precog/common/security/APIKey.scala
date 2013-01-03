package com.precog.common
package security

import json._

import blueeyes.json.JValue
import blueeyes.json.serialization.{ Decomposer, Extractor } 
import blueeyes.json.serialization.IsoSerialization._
import blueeyes.json.serialization.DefaultSerialization.{ DateTimeDecomposer => _, DateTimeExtractor => _, _ }

import scalaz.Scalaz._
import scalaz.Validation

import shapeless._

case class APIKeyRecord(
  apiKey:         APIKey,
  name:           Option[String],
  description:    Option[String],
  issuerKey:      Option[APIKey],
  grants:         Set[GrantId],
  isRoot:         Boolean)

object APIKeyRecord {
  implicit val apiKeyRecordIso = Iso.hlist(APIKeyRecord.apply _, APIKeyRecord.unapply _)
  
  val schemaV1 =       "apiKey"  :: "name" :: "description" :: "issuerKey" :: "grants" :: "isRoot" :: HNil
  val safeSchemaV1 =   "apiKey"  :: "name" :: "description" :: Omit        :: "grants" :: "isRoot" :: HNil
  @deprecated("V0 serialization schemas should be removed when legacy data is no longer needed", "2.1.5")
  val schemaV0 =       "tid"     :: "name" :: "description" :: "cid"       :: "gids"   :: ("isRoot" ||| false) :: HNil
  
  val decomposerV1: Decomposer[APIKeyRecord]= decomposerV[APIKeyRecord](schemaV1, Some("1.0"))
  val extractorV2: Extractor[APIKeyRecord] = extractorV[APIKeyRecord](schemaV1, Some("1.0"))
  val extractorV1: Extractor[APIKeyRecord] = extractorV[APIKeyRecord](schemaV1, None)
  val extractorV0: Extractor[APIKeyRecord]  = extractorV[APIKeyRecord](schemaV0, None)

  object Serialization {
    implicit val APIKeyRecordDecomposer: Decomposer[APIKeyRecord] = decomposerV1
    implicit val APIKeyRecordExtractor: Extractor[APIKeyRecord] = extractorV2 <+> extractorV1 <+> extractorV0
  }
  
  object SafeSerialization {
    implicit val decomposer: Decomposer[APIKeyRecord] = decomposerV[APIKeyRecord](safeSchemaV1, None)
  }
}


case class WrappedAPIKey(apiKey: APIKey, name: Option[String], description: Option[String])

object WrappedAPIKey {
  implicit val wrappedAPIKeyIso = Iso.hlist(WrappedAPIKey.apply _, WrappedAPIKey.unapply _)
  
  val schema = "apiKey" :: "name" :: "description" :: HNil

  implicit val decomposerV1: Decomposer[WrappedAPIKey]= decomposer[WrappedAPIKey](schema)
  implicit val extractorV1: Extractor[WrappedAPIKey] = extractor[WrappedAPIKey](schema)
}


case class NewAPIKeyRequest(name: Option[String], description: Option[String], grants: Set[NewGrantRequest])

object NewAPIKeyRequest {
  implicit val newAPIKeyRequestIso = Iso.hlist(NewAPIKeyRequest.apply _, NewAPIKeyRequest.unapply _)
  
  val schemaV1 = "name" :: "description" :: "grants" :: HNil

  val decomposerV1: Decomposer[NewAPIKeyRequest]= decomposerV[NewAPIKeyRequest](schemaV1, Some("1.0"))
  val extractorV1: Extractor[NewAPIKeyRequest] = extractorV[NewAPIKeyRequest](schemaV1, Some("1.0"))

  implicit val decomposer: Decomposer[NewAPIKeyRequest] = decomposerV1
  implicit val dxtractor: Extractor[NewAPIKeyRequest] = extractorV1
  
  def newAccount(accountId: String, path: Path, name: Option[String] = None, description: Option[String] = None) = {
    val grants = NewGrantRequest.newAccount(accountId, path, name.map(_+"-grant"), description.map(_+" standard account grant"), Set.empty[GrantId], None)
    NewAPIKeyRequest(name, description, Set(grants))
  }
}

// vim: set ts=4 sw=4 et:
