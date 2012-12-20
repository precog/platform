package com.precog.common
package accounts

import com.precog.common.json._
import com.precog.common.Path

import blueeyes.json._
import blueeyes.json.serialization.{ ValidatedExtraction, Extractor, Decomposer, IsoSerialization }
import blueeyes.json.serialization.IsoSerialization._
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.json.serialization.Extractor._

import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

import scalaz.Validation
import scalaz.syntax.apply._

import shapeless._

case class AccountPlan(planType: String) 
object AccountPlan {
  val Root = AccountPlan("Root")
  val Free = AccountPlan("Free")

  implicit val iso = Iso.hlist(AccountPlan.apply _, AccountPlan.unapply _)
  val schema = "type" :: HNil
  implicit val (decomposer, extractor) = IsoSerialization.serialization[AccountPlan](schema)
}

case class Account(accountId: String, 
                   email: String, 
                   passwordHash: String, 
                   passwordSalt: String, 
                   accountCreationDate: DateTime, 
                   apiKey: String, 
                   rootPath: Path, 
                   plan: AccountPlan, 
                   parentId: Option[String] = None,
                   lastPasswordChangeTime: Option[DateTime] = None)

object Account {
  implicit val iso = Iso.hlist(Account.apply _, Account.unapply _)
  val schemaV1   = "accountId" :: "email" :: "passwordHash" :: "passwordSalt" :: "accountCreationDate" :: "apiKey" :: "rootPath" :: "plan" :: "parentId" :: "lastPasswordChangeTime" :: HNil
  val safeSchema = "accountId" :: "email" ::           Omit ::           Omit :: "accountCreationDate" :: "apiKey" :: "rootPath" :: "plan" ::       Omit :: "lastPasswordChangeTime" :: HNil

  val (decomposerV1, extractorV1) = serializationV[Account](schemaV1, Some("1.0"))
  val safeDecomposerV1 = decomposer[Account](schemaV1)

  object Serialization {
    implicit val Decomposer = decomposerV1
    implicit val Extractor = extractorV1
  }

  object SafeSerialization {
    implicit val Decomposer = safeDecomposerV1
  }
}

case class WrappedAccountId(accountId: AccountId)

object WrappedAccountId {
  implicit val wrappedAccountIdIso = Iso.hlist(WrappedAccountId.apply _, WrappedAccountId.unapply _)
  
  val schema = "accountId" :: HNil

  implicit val (wrappedAccountIdDecomposer, wrappedAccountIdExtractor) = IsoSerialization.serialization[WrappedAccountId](schema)
}



