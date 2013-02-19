package com.precog.common
package security
package service

import com.precog.common.json._
import com.precog.common.accounts._
import com.precog.common.security._

import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._
import blueeyes.json._
import blueeyes.json.serialization.{ Extractor, Decomposer, IsoSerialization }
import blueeyes.json.serialization.IsoSerialization._
import blueeyes.json.serialization.DefaultSerialization.{ DateTimeDecomposer => _, DateTimeExtractor => _, _ }
import blueeyes.json.serialization.Extractor._

import org.joda.time.DateTime

import shapeless._

object v1 {
  case class GrantDetails(grantId: GrantId, name: Option[String], description: Option[String], permissions: Set[Permission], expirationDate: Option[DateTime])
  object GrantDetails {
    implicit val grantDetailsIso = Iso.hlist(GrantDetails.apply _, GrantDetails.unapply _)
    
    val schema = "grantId" :: "name" :: "description" :: "permissions" :: "expirationDate" :: HNil

    implicit val (decomposerV1, extractorV1) = IsoSerialization.serialization[GrantDetails](schema)
  }

  case class APIKeyDetails(apiKey: APIKey, name: Option[String], description: Option[String], grants: Set[GrantDetails])
  object APIKeyDetails {
    implicit val apiKeyDetailsIso = Iso.hlist(APIKeyDetails.apply _, APIKeyDetails.unapply _)
    
    val schema = "apiKey" :: "name" :: "description" :: "grants" :: HNil
    
    implicit val (decomposerV1, extractorV1) = IsoSerialization.serialization[APIKeyDetails](schema)
  }

  case class NewGrantRequest(name: Option[String], description: Option[String], parentIds: Set[GrantId], permissions: Set[Permission], expirationDate: Option[DateTime]) {
    def isExpired(at: Option[DateTime]) = (expirationDate, at) match {
      case (None, _) => false
      case (_, None) => true
      case (Some(expiry), Some(ref)) => expiry.isBefore(ref) 
    } 
  }

  object NewGrantRequest {
    private implicit val reqPermDecomposer = Permission.decomposerV1Base
    implicit val newGrantRequestIso = Iso.hlist(NewGrantRequest.apply _, NewGrantRequest.unapply _)
    
    val schemaV1 = "name" :: "description" :: ("parentIds" ||| Set.empty[GrantId]) :: "permissions" :: "expirationDate" :: HNil
    
    implicit val (decomposerV1, extractorV1) = IsoSerialization.serialization[NewGrantRequest](schemaV1)

    def newGrant(accountId: AccountId, path: Path, name: Option[String], description: Option[String], parentIds: Set[GrantId], expiration: Option[DateTime]): NewGrantRequest = {
      // Path is "/" so that an account may read data it owns no matter what path it exists under. See AccessControlSpec, APIKeyManager.newAccountGrant
      val readPerms =  Set(ReadPermission, ReducePermission).map(_(Path("/"), Set(accountId)) : Permission)
      val writePerms = Set(WritePermission, DeletePermission).map(_(path, Set()) : Permission)
      NewGrantRequest(name, description, parentIds, readPerms ++ writePerms, expiration)
    }
  }

  case class NewAPIKeyRequest(name: Option[String], description: Option[String], grants: Set[NewGrantRequest])

  object NewAPIKeyRequest {
    implicit val newAPIKeyRequestIso = Iso.hlist(NewAPIKeyRequest.apply _, NewAPIKeyRequest.unapply _)
    
    val schemaV1 = "name" :: "description" :: "grants" :: HNil

    implicit val (decomposerV1, extractorV1) = IsoSerialization.serialization[NewAPIKeyRequest](schemaV1)

    def newAccount(accountId: String, path: Path, name: Option[String] = None, description: Option[String] = None, parentIds: Set[GrantId] = Set()) = {
      val grants = NewGrantRequest.newGrant(accountId, path, name.map(_+"-grant"), description.map(_+" standard account grant"), parentIds, None)
      NewAPIKeyRequest(name, description, Set(grants))
    }
  }
}

// vim: set ts=4 sw=4 et:
