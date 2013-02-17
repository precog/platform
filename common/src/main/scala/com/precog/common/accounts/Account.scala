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

import com.precog.common.json._
import com.precog.common.Path

import blueeyes.json._
import blueeyes.json.serialization.{ Extractor, Decomposer, IsoSerialization }
import blueeyes.json.serialization.IsoSerialization._
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.json.serialization.Extractor._

import com.google.common.base.Charsets
import com.google.common.hash.Hashing

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
    implicit val accountDecomposer = decomposerV1
    implicit val accountExtractor = extractorV1
  }

  object SafeSerialization {
    implicit val accountDecomposer = safeDecomposerV1
  }

  private val randomSource = new java.security.SecureRandom

  def randomSalt() = {
    val saltBytes = new Array[Byte](256)
    randomSource.nextBytes(saltBytes)
    saltBytes.flatMap(byte => Integer.toHexString(0xFF & byte))(collection.breakOut) : String
  }

  // FIXME: Remove when there are no SHA1 hashes in the accounts db
  def saltAndHashSHA1(password: String, salt: String): String = {
    Hashing.sha1().hashString(password + salt, Charsets.UTF_8).toString
  }

  def saltAndHashSHA256(password: String, salt: String): String = {
    Hashing.sha256().hashString(password + salt, Charsets.UTF_8).toString
  }

  // FIXME: Remove when there are no old-style SHA256 hashes in the accounts db
  def saltAndHashLegacy(password: String, salt: String): String = {
    val md = java.security.MessageDigest.getInstance("SHA-256");
    val dataBytes = (password + salt).getBytes("UTF-8")
    md.update(dataBytes, 0, dataBytes.length)
    val hashBytes = md.digest()

    hashBytes.flatMap(byte => Integer.toHexString(0xFF & byte))(collection.breakOut) : String
  }
}

case class WrappedAccountId(accountId: AccountId)

object WrappedAccountId {
  implicit val wrappedAccountIdIso = Iso.hlist(WrappedAccountId.apply _, WrappedAccountId.unapply _)
  
  val schema = "accountId" :: HNil

  implicit val (wrappedAccountIdDecomposer, wrappedAccountIdExtractor) = IsoSerialization.serialization[WrappedAccountId](schema)
}



