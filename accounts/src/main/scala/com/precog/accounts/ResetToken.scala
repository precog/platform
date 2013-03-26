package com.precog
package accounts

import blueeyes.json.serialization._
import blueeyes.json.serialization.IsoSerialization._
import blueeyes.json.serialization.DefaultSerialization._

import com.precog.common.accounts._
import com.precog.common.json._

import org.joda.time.DateTime

import shapeless._

case class ResetToken(tokenId: ResetTokenId, accountId: AccountId, email: String, expiresAt: DateTime, usedAt: Option[DateTime] = None)

object ResetToken {
  implicit val iso = Iso.hlist(ResetToken.apply _, ResetToken.unapply _)

  val schemaV1 = "tokenId" :: "accountId" :: "email" :: "expiresAt" :: "usedAt" :: HNil

  implicit val (decomposerV1, extractorV1)  = serializationV(schemaV1, Some("1.0"))
}
