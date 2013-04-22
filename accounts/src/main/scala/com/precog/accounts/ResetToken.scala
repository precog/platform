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
package com.precog
package accounts

import blueeyes.json.serialization._
import blueeyes.json.serialization.IsoSerialization._
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.json.serialization.Versioned._

import com.precog.common.accounts._

import org.joda.time.DateTime

import shapeless._

case class ResetToken(tokenId: ResetTokenId, accountId: AccountId, email: String, expiresAt: DateTime, usedAt: Option[DateTime] = None)

object ResetToken {
  implicit val iso = Iso.hlist(ResetToken.apply _, ResetToken.unapply _)

  val schemaV1 = "tokenId" :: "accountId" :: "email" :: "expiresAt" :: "usedAt" :: HNil

  implicit val (decomposerV1, extractorV1)  = serializationV(schemaV1, Some("1.0".v))
}
