package com.precog.common
package accounts

import com.precog.common.json._
import com.precog.common.security.APIKey

import blueeyes.json._
import blueeyes.json.serialization._
import blueeyes.json.serialization.IsoSerialization._

// we want to serialize dates as ISO8601 not as numbers
import blueeyes.json.serialization.DefaultSerialization.{ DateTimeExtractor => _, DateTimeDecomposer => _, _ }
import com.precog.common.security.{TZDateTimeDecomposer, TZDateTimeExtractor}

import org.joda.time.DateTime

import shapeless._

case class AccountDetails(
  accountId: AccountId,
  email: String,
  accountCreationDate: DateTime,
  apiKey: APIKey,
  rootPath: Path,
  plan: AccountPlan,
  lastPasswordChangeTime: Option[DateTime] = None,
  profile: Option[JValue] = None)

object AccountDetails {
  def from(account: Account): AccountDetails = {
    import account._
    AccountDetails(accountId, email, accountCreationDate, apiKey, rootPath, plan, lastPasswordChangeTime, profile)
  }

  implicit val accountDetailsIso = Iso.hlist(AccountDetails.apply _, AccountDetails.unapply _)

  val schema = "accountId" :: "email" :: "accountCreationDate" :: "apiKey" :: "rootPath" :: "plan" :: "lastPasswordChangeTime" :: "profile" :: HNil

  implicit val accountDetailsDecomposer = decomposerV[AccountDetails](schema, None)
  implicit val accountDetailsExtractor = extractorV[AccountDetails](schema, None)
}
