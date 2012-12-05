package com.precog.common
package security

import json._

import org.joda.time.DateTime

import com.weiglewilczek.slf4s.Logging

import blueeyes.json.{JValue, JString}
import blueeyes.json.serialization.{Extractor, ValidatedExtraction}
import blueeyes.json.serialization.DefaultSerialization.{ DateTimeDecomposer => _, DateTimeExtractor => _, _ }

import scalaz.Scalaz._
import scalaz.std.set._

import shapeless._

case class Grant(
  grantId:        GrantId,
  name:           Option[String],
  description:    Option[String],
  issuerKey:      Option[APIKey],
  parentIds:      Set[GrantId],
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

object Grant extends Logging {
  implicit val grantIso = Iso.hlist(Grant.apply _, Grant.unapply _)

  val schema =     "grantId" :: "name" :: "description" :: "issuerKey" :: "parentIds" :: "permissions" :: "expirationDate" :: HNil
  val safeSchema = "grantId" :: "name" :: "description" :: Omit        :: Omit        :: "permissions" :: "expirationDate" :: HNil
  
  object Serialization {
    implicit val grantDecomposer = decomposer[Grant](schema)
    val v1GrantExtractor = extractor[Grant](schema)
  }
  
  object SafeSerialization {
    implicit val safeGrantDecomposer = decomposer[Grant](safeSchema)
    val v1SafeGrantExtractor = extractor[Grant](safeSchema)
  }

  val v0GrantExtractor = new Extractor[Grant] with ValidatedExtraction[Grant] {
    override def validated(obj: JValue) = {
      ((obj \ "gid").validated[GrantId] |@|
       (obj \ "issuer").validated[Option[GrantId]] |@|
       {
         (obj \ "permission" \ "type") match {
           case JString("owner") => Permission.accessTypeExtractor.validated(JString("delete"))
           case other            => Permission.accessTypeExtractor.validated(other)
         }
       } |@|
       (obj \ "permission" \ "path").validated[Path] |@|
       (obj \ "permission" \ "ownerAccountId").validated[Option[String]] |@|
       (obj \ "permission" \ "expirationDate").validated[Option[DateTime]]).apply {
        (gid, issuer, permBuild, path, ownerId, expiration) => Grant(gid, None, None, None, issuer.toSet, Set(permBuild.apply(path, ownerId.toSet)), expiration)
      }
    }
  }

  implicit val grantExtractor = new Extractor[Grant] with ValidatedExtraction[Grant] {
    override def validated(obj: JValue) = {
      Serialization.v1GrantExtractor.validated(obj) orElse
      SafeSerialization.v1SafeGrantExtractor.validated(obj) orElse
      v0GrantExtractor.validated(obj)
    }
  }

  def implies(grants: Set[Grant], perms: Set[Permission], at: Option[DateTime] = None) = {
    logger.trace("Checking implication of %s to %s".format(grants, perms))
    perms.forall(perm => grants.exists(_.implies(perm, at)))
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

case class NewGrantRequest(name: Option[String], description: Option[String], parentIds: Set[GrantId], permissions: Set[Permission], expirationDate: Option[DateTime]) {
  def isExpired(at: Option[DateTime]) = (expirationDate, at) match {
    case (None, _) => false
    case (_, None) => true
    case (Some(expiry), Some(ref)) => expiry.isBefore(ref) 
  } 
}

object NewGrantRequest {
  implicit val newGrantRequestIso = Iso.hlist(NewGrantRequest.apply _, NewGrantRequest.unapply _)
  
  val schema = "name" :: "description" :: ("parentIds" ||| Set.empty[GrantId]) :: "permissions" :: "expirationDate" :: HNil
  
  implicit val (newGrantRequestDecomposer, newGrantRequestExtractor) = serialization[NewGrantRequest](schema)

  def newAccount(accountId: AccountId, path: Path, name: Option[String], description: Option[String], parentIds: Set[GrantId], expiration: Option[DateTime]): NewGrantRequest = {
    // Path is "/" so that an account may read data it owns no matter what path it exists under. See AccessControlSpec, APIKeyManager.newAccountGrant
    val readPerms =  Set(ReadPermission, ReducePermission).map(_(Path("/"), Set(accountId)) : Permission)
    val writePerms = Set(WritePermission, DeletePermission).map(_(path, Set()) : Permission)
    NewGrantRequest(name, description, parentIds, readPerms ++ writePerms, expiration)
  }
}
