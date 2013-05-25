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
package security

import accounts.AccountId

import org.joda.time.Instant
import org.joda.time.DateTime

import com.weiglewilczek.slf4s.Logging

import blueeyes.json.{ JValue, JString }
import blueeyes.json.serialization._
import blueeyes.json.serialization.IsoSerialization._
import blueeyes.json.serialization.DefaultSerialization.{ DateTimeDecomposer => _, DateTimeExtractor => _, _ }
import blueeyes.json.serialization.JodaSerializationImplicits.{ InstantExtractor, InstantDecomposer }
import blueeyes.json.serialization.Versioned._

import shapeless._

import scalaz._
import scalaz.syntax.plus._
import scalaz.syntax.applicative._
import scalaz.std.set._

case class Grant(
  grantId:        GrantId,
  name:           Option[String],
  description:    Option[String],
  issuerKey:      APIKey,
  parentIds:      Set[GrantId],
  permissions:    Set[Permission],
  createdAt:      Instant,
  expirationDate: Option[DateTime]) {
  
  def isExpired(at: Option[DateTime]) = (expirationDate, at) match {
    case (None, _) => false
    case (_, None) => true
    case (Some(expiry), Some(ref)) => expiry.isBefore(ref) 
  } 

  def implies(perm: Permission, at: Option[DateTime]): Boolean = {
    !isExpired(at) && permissions.exists(_.implies(perm))
  }
  
  def implies(perms: Set[Permission], at: Option[DateTime]): Boolean = {
    !isExpired(at) && perms.forall { perm => permissions.exists(_.implies(perm)) }
  }
  
  def implies(other: Grant): Boolean = {
    !isExpired(other.expirationDate) && other.permissions.forall { perm => permissions.exists(_.implies(perm)) }
  }
}

object Grant extends Logging {
  implicit val grantIso = Iso.hlist(Grant.apply _, Grant.unapply _)

  val schemaV1 =     "grantId" :: "name" :: "description" :: ("issuerKey" ||| "(undefined)") :: "parentIds" :: "permissions" :: ("createdAt" ||| new Instant(0L)) :: "expirationDate" :: HNil
  
  val decomposerV1: Decomposer[Grant] = decomposerV[Grant](schemaV1, Some("1.0".v))
  val extractorV2: Extractor[Grant] = extractorV[Grant](schemaV1, Some("1.0".v))
  val extractorV1: Extractor[Grant] = extractorV[Grant](schemaV1, None)

  @deprecated("V0 serialization schemas should be removed when legacy data is no longer needed", "2.1.5")
  val extractorV0: Extractor[Grant] = new Extractor[Grant] {
    override def validated(obj: JValue) = {
      (obj.validated[GrantId]("gid") |@|
       obj.validated[Option[APIKey]]("cid").map(_.getOrElse("(undefined)")) |@|
       obj.validated[Option[GrantId]]("issuer") |@|
       obj.validated[Permission]("permission")(Permission.extractorV0) |@|
       obj.validated[Option[DateTime]]("permission.expirationDate")).apply {
        (gid, cid, issuer, permission, expiration) => 
          Grant(gid, None, None, cid, issuer.toSet, Set(permission), new Instant(0L), expiration)
      }
    }
  }

  implicit val decomposer: Decomposer[Grant] = decomposerV1
  implicit val extractor: Extractor[Grant] = extractorV2 <+> extractorV1 <+> extractorV0

  def implies(grants: Set[Grant], perms: Set[Permission], at: Option[DateTime] = None) = {
    logger.trace("Checking implication of %s to %s".format(grants, perms))
    perms.nonEmpty && perms.forall(perm => grants.exists(_.implies(perm, at)))
  }
  
  /*
   * Computes the weakest subset of the supplied set of grants which is sufficient to support the supplied set
   * of permissions. Grant g1 is (weakly) weaker than grant g2 if g2 implies all the permissions implied by g1 and
   * g2 does not expire before g1 (nb. this relation is reflexive). The stragegy is greedy: the grants are
   * topologically sorted in order of decreasing strength, and then winnowed from strongest to weakest until it isn't
   * possible to remove any futher grants without undermining the support for the permissions. Where multiple solutions
   * are possible, one will be chosen arbitrarily.
   * 
   * If the supplied set of grants is insufficient to support the supplied set of permissions the result is the empty
   * set. 
   */
  def coveringGrants(grants: Set[Grant], perms: Set[Permission], at: Option[DateTime] = None): Set[Grant] = {
    if(!implies(grants, perms, at)) Set.empty[Grant]
    else {
      def tsort(grants : List[Grant]) : List[Grant] = grants.find(g1 => !grants.exists(g2 => g2 != g1 && g2.implies(g1))) match {
        case Some(undominated) => undominated +: tsort(grants.filterNot(_ == undominated))
        case _ => List()
      }
      
      def minimize(grants: Seq[Grant], perms: Seq[Permission]): Set[Grant] = grants match {
        case Seq(head, tail @ _*) => perms.partition { perm => tail.exists(_.implies(perm, at)) } match {
          case (Nil,  Nil)         => Set()
          case (rest, Nil)         => minimize(tail, rest)
          case (rest, requireHead) => minimize(tail, rest) + head
        }
        case _ => Set()
      }

      val distinct = grants.groupBy { g => (g.permissions, g.expirationDate) }.map(_._2.head).toList
      minimize(tsort(distinct), perms.toList)
    }
  }
}

