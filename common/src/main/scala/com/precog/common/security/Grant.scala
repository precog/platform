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
