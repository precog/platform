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
package com.precog.common.security

import com.google.common.base.Charsets
import com.google.common.hash.Hashing

import com.precog.common.accounts.AccountId

import blueeyes.json._
import blueeyes.json.serialization._
import blueeyes.json.serialization.Extractor._
import blueeyes.json.serialization.IsoSerialization._
import blueeyes.json.serialization.DefaultSerialization._

import scalaz._
import scalaz.syntax.std.boolean._

import scala.annotation.tailrec

case class Authorities private (accountIds: Set[AccountId]) {
  def expand(ownerAccountId: AccountId) =
    this.copy(accountIds = this.accountIds + ownerAccountId)

  def sha1 = Hashing.sha1().hashString(accountIds.toList.sorted.toString, Charsets.UTF_8).toString

  def render = accountIds.mkString("[", ", ", "]")
}

object Authorities {
  def apply(accountIds: NonEmptyList[AccountId]): Authorities = apply(accountIds.list.toSet)

  def apply(firstAccountId: AccountId, others: AccountId*): Authorities =
    apply(others.toSet + firstAccountId)

  def ifPresent(accountIds: Set[AccountId]): Option[Authorities] = accountIds.nonEmpty.option(apply(accountIds))

  implicit val AuthoritiesDecomposer: Decomposer[Authorities] = new Decomposer[Authorities] {
    override def decompose(authorities: Authorities): JValue = {
      JObject(JField("uids", JArray(authorities.accountIds.map(JString(_)).toList)) :: Nil)
    }
  }

  implicit val AuthoritiesExtractor: Extractor[Authorities] = new Extractor[Authorities] {
    override def validated(obj: JValue): Validation[Error, Authorities] =
      (obj \ "uids").validated[Set[String]].map(Authorities(_))
  }

  implicit object AuthoritiesSemigroup extends Semigroup[Authorities] {
    def append(a: Authorities, b: => Authorities): Authorities = {
      Authorities(a.accountIds ++ b.accountIds)
    }
  }
}
