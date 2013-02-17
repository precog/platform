package com.precog.common.security

import com.precog.common.accounts.AccountId

import blueeyes.json._
import blueeyes.json.serialization._
import blueeyes.json.serialization.Extractor._
import blueeyes.json.serialization.IsoSerialization._
import blueeyes.json.serialization.DefaultSerialization._

import scalaz._

import scala.annotation.tailrec

case class Authorities(ownerAccountIds: Set[AccountId]) {
  @tailrec
  final def hashSeq(l: Seq[String], hash: Int, i: Int = 0): Int = {
    if(i < l.length) {
      hashSeq(l, hash * 31 + l(i).hashCode, i+1)
    } else {
      hash
    }     
  }    

  lazy val hash = {
    if(ownerAccountIds.size == 0) 1 
    else if(ownerAccountIds.size == 1) ownerAccountIds.head.hashCode 
    else hashSeq(ownerAccountIds.toSeq, 1) 
  }

  override def hashCode(): Int = hash

  def expand(ownerAccountId: AccountId) = 
    this.copy(ownerAccountIds = this.ownerAccountIds + ownerAccountId)
}

object Authorities {
  implicit val AuthoritiesDecomposer: Decomposer[Authorities] = new Decomposer[Authorities] {
    override def decompose(authorities: Authorities): JValue = {
      JObject(JField("uids", JArray(authorities.ownerAccountIds.map(JString(_)).toList)) :: Nil)
    }
  }

  implicit val AuthoritiesExtractor: Extractor[Authorities] = new Extractor[Authorities] {
    override def validated(obj: JValue): Validation[Error, Authorities] =
      (obj \ "uids").validated[Set[String]].map(Authorities(_))
  }

  implicit object AuthoritiesMonoid extends Monoid[Authorities] {
    def append(a: Authorities, b: => Authorities): Authorities = {
      Authorities(a.ownerAccountIds ++ b.ownerAccountIds)
    }
    def zero: Authorities = Authorities.Empty
  }

  val Empty = Authorities(Set())
}
