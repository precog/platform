package com.precog.common
package security

import json._

import blueeyes.json.JValue
import blueeyes.json.serialization._
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
  
  val v1Schema =       "apiKey"  :: "name" :: "description" :: "issuerKey" :: "grants" :: "isRoot" :: HNil
  val v1SafeSchema =   "apiKey"  :: "name" :: "description" :: Omit        :: "grants" :: "isRoot" :: HNil
  val v0Schema = "tid"     :: "name" :: "description" :: "cid"       :: "gids"   :: ("isRoot" ||| false) :: HNil
  
  object Serialization {
    implicit val apiKeyRecordDecomposer = decomposer[APIKeyRecord](v1Schema)
    val v1ApiKeyRecordExtractor = extractor[APIKeyRecord](v1Schema)
  }
  
  object SafeSerialization {
    implicit val v1SafeAPIKeyRecordDecomposer = decomposer[APIKeyRecord](v1SafeSchema)
    val v1SafeAPIKeyRecordExtractor = extractor[APIKeyRecord](v1SafeSchema)
  }

  object LegacySerialization {
    val v0APIKeyRecordExtractor = extractor[APIKeyRecord](v0Schema)
  }

  implicit val apiKeyExtractor = new Extractor[APIKeyRecord] with ValidatedExtraction[APIKeyRecord] {
    override def validated(obj: JValue): Validation[Extractor.Error, APIKeyRecord] = {
      Serialization.v1ApiKeyRecordExtractor.validated(obj) orElse
      SafeSerialization.v1SafeAPIKeyRecordExtractor.validated(obj) orElse
      LegacySerialization.v0APIKeyRecordExtractor.validated(obj)
    }
  }
}

case class NewAPIKeyRequest(name: Option[String], description: Option[String], grants: Set[NewGrantRequest])

object NewAPIKeyRequest {
  implicit val newAPIKeyRequestIso = Iso.hlist(NewAPIKeyRequest.apply _, NewAPIKeyRequest.unapply _)
  
  val schema = "name" :: "description" :: "grants" :: HNil
  
  implicit val (newAPIKeyRequestDecomposer, newAPIKeyRequestExtractor) = serialization[NewAPIKeyRequest](schema)
  
  def newAccount(accountId: String, path: Path, name: Option[String] = None, description: Option[String] = None) = {
    val grants = NewGrantRequest.newAccount(accountId, path, name.map(_+"-grant"), description.map(_+" standard account grant"), Set.empty[GrantId], None)
    NewAPIKeyRequest(name, description, Set(grants))
  }
}

// vim: set ts=4 sw=4 et:
