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

import json._

import org.joda.time.DateTime

import blueeyes.json.serialization.DefaultSerialization.{ DateTimeDecomposer => _, DateTimeExtractor => _, _ }

import scalaz.Scalaz._

import shapeless._

case class Grant(
  grantId:        GrantID,
  name:           Option[String],
  description:    Option[String],
  issuerKey:      Option[APIKey],
  parentIds:      Set[GrantID],
  permissions:    Set[Permission],
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

object Grant {
  implicit val grantIso = Iso.hlist(Grant.apply _, Grant.unapply _)

  val schema =     "grantId" :: "name" :: "description" :: "issuerKey" :: "parentIds" :: "permissions" :: "expirationDate" :: HNil
  val safeSchema = "grantId" :: "name" :: "description" :: Omit        :: Omit        :: "permissions" :: "expirationDate" :: HNil
  
  object Serialization {
    implicit val (grantDecomposer, grantExtractor) = serialization[Grant](schema)
  }
  
  object SafeSerialization {
    implicit val (safeGrantDecomposer, safeGrantExtractor) = serialization[Grant](safeSchema)
  }

  def implies(grants: Set[Grant], perms: Set[Permission], at: Option[DateTime] = None) =
    perms.forall(perm => grants.exists(_.implies(perm, at)))
  
  /*
   * Computes the weakest subset of the supplied set of grants which is sufficient to support the supplied set
   * of permissions. Grant g1 is (weakly) weaker than grant g2 if g2 implies all the permissions implied by g1 and
   * g2 does not expire before g1 (nb. this relation is reflexive). The stragegy is greedy: the gransts are
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

case class NewGrantRequest(name: Option[String], description: Option[String], parentIds: Set[GrantID], permissions: Set[Permission], expirationDate: Option[DateTime]) {
  def isExpired(at: Option[DateTime]) = (expirationDate, at) match {
    case (None, _) => false
    case (_, None) => true
    case (Some(expiry), Some(ref)) => expiry.isBefore(ref) 
  } 
}

object NewGrantRequest {
  implicit val newGrantRequestIso = Iso.hlist(NewGrantRequest.apply _, NewGrantRequest.unapply _)
  
  val schema = "name" :: "description" :: "parentIds" :: "permissions" :: "expirationDate" :: HNil
  
  implicit val (newGrantRequestDecomposer, newGrantRequestExtractor) = serialization[NewGrantRequest](schema)

  def newAccount(accountId: AccountID, path: Path, name: Option[String], description: Option[String], parentIds: Set[GrantID], expiration: Option[DateTime]): NewGrantRequest = {
    val readPerms =  Set(ReadPermission, ReducePermission).map(_(Path("/"), Set(accountId)) : Permission)
    val writePerms = Set(WritePermission, DeletePermission).map(_(path, Set()) : Permission)
    NewGrantRequest(name, description, parentIds, readPerms ++ writePerms, expiration)
  }
}
