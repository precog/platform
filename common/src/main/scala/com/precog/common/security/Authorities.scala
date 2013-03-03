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

import scala.annotation.tailrec

case class Authorities private (ownerAccountIds: Set[AccountId]) {
  def expand(ownerAccountId: AccountId) =
    this.copy(ownerAccountIds = this.ownerAccountIds + ownerAccountId)

  def sha1 = Hashing.sha1().hashString(ownerAccountIds.toList.sorted.toString, Charsets.UTF_8).toString
}

object Authorities {
  def apply(accountIds: NonEmptyList[AccountId]): Authorities = apply(accountIds.list.toSet)

  def apply(firstAccountId: AccountId, others: AccountId*): Authorities =
    apply(others.toSet + firstAccountId)

  implicit val AuthoritiesDecomposer: Decomposer[Authorities] = new Decomposer[Authorities] {
    override def decompose(authorities: Authorities): JValue = {
      JObject(JField("uids", JArray(authorities.ownerAccountIds.map(JString(_)).toList)) :: Nil)
    }
  }

  implicit val AuthoritiesExtractor: Extractor[Authorities] = new Extractor[Authorities] {
    override def validated(obj: JValue): Validation[Error, Authorities] =
      (obj \ "uids").validated[Set[String]].map(Authorities(_))
  }

  implicit object AuthoritiesSemigroup extends Semigroup[Authorities] {
    def append(a: Authorities, b: => Authorities): Authorities = {
      Authorities(a.ownerAccountIds ++ b.ownerAccountIds)
    }
  }
}
