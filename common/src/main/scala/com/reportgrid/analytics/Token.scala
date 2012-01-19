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
package com.reportgrid.analytics

import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.xschema._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.JodaSerializationImplicits._

import org.joda.time.DateTime
import scalaz.Scalaz._
import scalaz.Validation

import org.joda.time.{DateTime, DateTimeZone}

/** A token gives a user access to a path in the ReportGrid virtual file
 * system. Every customer of ReportGrid has a "root" token which gives
 * them access to a customer-specific path in the file system. They can
 * use this token to issue other tokens with access to subfolders of
 * their own root folder.
 *
 * Tokens have permissions (read/write/share), expiration dates, and limits.
 */
case class Token(tokenId: String, parentTokenId: Option[String], accountTokenId: String, path: Path, permissions: Permissions, expires: DateTime, limits: Limits) {
  def expired  = expires.getMillis <= new DateTime(DateTimeZone.UTC).getMillis
  def canRead  = permissions.read
  def canWrite = permissions.write
  def canShare = permissions.share

  def isAccountToken = tokenId == accountTokenId

  def selectorName = if (isAccountToken) "accountTokenId" else "parentTokenId"

  /** Issues a new token derived from this one. By default, the token will
   * have the same permissions, expiration date, and limit of this token.
   */
  def issue(relativePath: Path = Path("/"), permissions: Permissions = this.permissions, expires: DateTime = this.expires, limits: Limits = this.limits): Token = Token(
    tokenId        = Token.newUUID(),
    parentTokenId  = Some(this.tokenId),
    accountTokenId = this.accountTokenId,
    path           = this.path / relativePath,
    permissions    = permissions.limitTo(this.permissions),
    expires        = new DateTime(expires.getMillis.min(this.expires.getMillis), DateTimeZone.UTC),
    limits         = limits.limitTo(this.limits)
  )

  def relativeTo(owner: Token) = copy(path = (this.path - owner.path).getOrElse(this.path))

  def absoluteFrom(owner: Token) = copy(path = owner.path / this.path)
}

trait TokenSerialization {
    final implicit val TokenDecomposer = new Decomposer[Token] {
    def decompose(token: Token): JValue = JObject(
      JField("tokenId",         token.tokenId.serialize)  ::
      JField("parentTokenId",   token.parentTokenId.serialize) ::
      JField("accountTokenId",  token.accountTokenId.serialize) ::
      JField("path",            token.path.serialize) ::
      JField("permissions",     token.permissions.serialize) ::
      JField("expires",         token.expires.serialize) ::
      JField("limits",          token.limits.serialize) ::
      Nil
    )
  }

  final implicit val TokenExtractor = new Extractor[Token] {
    def extract(jvalue: JValue): Token = Token(
      tokenId         = (jvalue \ "tokenId").deserialize[String],
      parentTokenId   = (jvalue \ "parentTokenId").deserialize[Option[String]],
      accountTokenId  = (jvalue \ "accountTokenId").deserialize[String],
      path            = (jvalue \ "path").deserialize[Path],
      permissions     = (jvalue \ "permissions").deserialize[Permissions],
      expires         = (jvalue \ "expires").deserialize[DateTime],
      limits          = (jvalue \ "limits").deserialize[Limits]
    )
  }
}

object Token extends TokenSerialization {
  private def newUUID() = java.util.UUID.randomUUID().toString.toUpperCase

  val Never = new DateTime(java.lang.Long.MAX_VALUE, DateTimeZone.UTC)

  lazy val Root = Token("8E680858-329C-4F31-BEE3-2AD15FB67EED", None, "8E680858-329C-4F31-BEE3-2AD15FB67EED", Path("/"), Permissions(true, true, true, true), Never, Limits.None)

  lazy val Test = Token(
    tokenId        = "A3BC1539-E8A9-4207-BB41-3036EC2C6E6D",
    parentTokenId  = Some(Root.tokenId),
    accountTokenId = "A3BC1539-E8A9-4207-BB41-3036EC2C6E6D",
    path           = Path("test-account-root"),
    permissions    = Permissions(true, true, true, true),
    expires        = Never,
    limits         = Limits(order = 2, depth = 3, limit = 20, tags = 2)
  )

  def newAccount(path: Path, limits: Limits, permissions: Permissions = Permissions.All, expires: DateTime = Never): Token = {
    val newTokenId = newUUID()

    new Token(
      tokenId        = newTokenId,
      parentTokenId  = Some(Root.tokenId),
      accountTokenId = newTokenId,
      path           = path,
      permissions    = Permissions(true, true, true, true),
      expires        = Never,
      limits         = limits.limitTo(Token.Root.limits)
    )
  }
}
