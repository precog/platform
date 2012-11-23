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

case class NewAPIKeyRequest(name: Option[String], description: Option[String], grants: Set[NewGrantRequest])

object NewAPIKeyRequest {
  implicit val newAPIKeyRequestIso = Iso.hlist(NewAPIKeyRequest.apply _, NewAPIKeyRequest.unapply _)
  
  val schema = "name" :: "description" :: "grants" :: HNil
  
  implicit val (newAPIKeyRequestDecomposer, newAPIKeyRequestExtractor) = serialization[NewAPIKeyRequest](schema)
  
  def newAccount(accountId: String, path: Path, name: Option[String] = None, description: Option[String] = None) = {
    val grants = NewGrantRequest.newAccount(accountId, path, name.map(_+"-grant"), description.map(_+" standard account grant"), Set.empty[GrantID], None)
    NewAPIKeyRequest(name, description, Set(grants))
  }
}

// vim: set ts=4 sw=4 et:
