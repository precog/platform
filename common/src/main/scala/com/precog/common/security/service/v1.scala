package com.precog.common
package security
package service

import com.precog.common.accounts._
import com.precog.common.security._

import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._
import blueeyes.json._
import blueeyes.json.serialization.{ Extractor, Decomposer, IsoSerialization }
import blueeyes.json.serialization.IsoSerialization._
import blueeyes.json.serialization.DefaultSerialization.{ DateTimeDecomposer => _, DateTimeExtractor => _, _ }
import blueeyes.json.serialization.JodaSerializationImplicits.InstantDecomposer
import blueeyes.json.serialization.JodaSerializationImplicits.InstantExtractor
import blueeyes.json.serialization.Extractor._
import blueeyes.json.serialization.Versioned._

import org.joda.time.DateTime
import org.joda.time.Instant

import shapeless._
import scalaz.{NonEmptyList => NEL}

object v1 {
  case class GrantDetails(grantId: GrantId, name: Option[String], description: Option[String], permissions: Set[Permission], createdAt: Instant, expirationDate: Option[DateTime]) {
    def isValidAt(timestamp: Instant) = {
      createdAt.isBefore(timestamp) && expirationDate.forall(_.isAfter(timestamp))
    }
  }
  object GrantDetails {
    implicit val grantDetailsIso = Iso.hlist(GrantDetails.apply _, GrantDetails.unapply _)

    val schema = "grantId" :: "name" :: "description" :: "permissions" :: ("createdAt" ||| new Instant(0L)) :: "expirationDate" :: HNil

    implicit val (decomposerV1, extractorV1) = IsoSerialization.serialization[GrantDetails](schema)
  }

  case class APIKeyDetails(apiKey: APIKey, name: Option[String], description: Option[String], grants: Set[GrantDetails], issuerChain: List[APIKey])
  object APIKeyDetails {
    implicit val apiKeyDetailsIso = Iso.hlist(APIKeyDetails.apply _, APIKeyDetails.unapply _)

    val schema = "apiKey" :: "name" :: "description" :: "grants" :: "issuerChain" :: HNil

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

    implicit val decomposerV1 = IsoSerialization.decomposer[NewGrantRequest](schemaV1)
    implicit val extractorV1 = IsoSerialization.extractor[NewGrantRequest](schemaV1)
  }

  case class NewAPIKeyRequest(name: Option[String], description: Option[String], grants: Set[NewGrantRequest])

  object NewAPIKeyRequest {
    implicit val newAPIKeyRequestIso = Iso.hlist(NewAPIKeyRequest.apply _, NewAPIKeyRequest.unapply _)

    val schemaV1 = "name" :: "description" :: "grants" :: HNil

    implicit val (decomposerV1, extractorV1) = IsoSerialization.serialization[NewAPIKeyRequest](schemaV1)

    def newAccount(accountId: AccountId, name: Option[String] = None, description: Option[String] = None, parentIds: Set[GrantId] = Set()) = {
      val path = Path("/%s/".format(accountId))
      val permissions = Account.newAccountPermissions(accountId, path)
      val grantRequest = NewGrantRequest(name.map(_+"-grant"), description.map(_+" standard account grant"), parentIds, permissions, None)

      NewAPIKeyRequest(name, description, Set(grantRequest))
    }
  }
}

// vim: set ts=4 sw=4 et:
