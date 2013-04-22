/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
package com.precog.common
package accounts

import com.precog.common.security.APIKey

import blueeyes.json._
import blueeyes.json.serialization._
import blueeyes.json.serialization.IsoSerialization._
import blueeyes.json.serialization.Versioned._

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
