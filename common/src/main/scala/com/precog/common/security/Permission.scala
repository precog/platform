package com.precog.common
package security

import accounts.AccountId

import blueeyes.json._
import blueeyes.json.serialization.{ Extractor, Decomposer }
import blueeyes.json.serialization.DefaultSerialization.{ DateTimeDecomposer => _, DateTimeExtractor => _, _ }
import blueeyes.json.serialization.Extractor.Invalid

import scalaz._
import scalaz.std.option._
import scalaz.syntax.apply._

sealed trait Permission {
  def path: Path
  def ownerAccountIds: Set[AccountId]
  
  def implies(other: Permission): Boolean
  
  protected def pathImplies(other: Permission): Boolean = (this, other) match {
    case (Permission(p1, o1), Permission(p2, o2)) if p1.isEqualOrParent(p2) && !o2.isEmpty && o2.subsetOf(o1) => true
    case (Permission(p1, o1), Permission(p2, _))  if p1.isEqualOrParent(p2) && o1.isEmpty => true
    case _ => false
  }
}

case class ReadPermission  (path: Path, ownerAccountIds: Set[AccountId]) extends Permission {
  def implies(other: Permission): Boolean = other match {
    case _ : ReadPermission | _ : ReducePermission => pathImplies(other)
    case _ => false
  }
}

case class ReducePermission(path: Path, ownerAccountIds: Set[AccountId]) extends Permission {
  def implies(other: Permission): Boolean = other match {
    case _ : ReducePermission => pathImplies(other)
    case _ => false
  }
}

case class WritePermission (path: Path, ownerAccountIds: Set[AccountId]) extends Permission {
  def implies(other: Permission): Boolean = other match {
    case _ : WritePermission => pathImplies(other)
    case _ => false
  }
}

case class DeletePermission(path: Path, ownerAccountIds: Set[AccountId]) extends Permission {
  def implies(other: Permission): Boolean = other match {
    case _ : DeletePermission => pathImplies(other)
    case _ => false
  }
}

object Permission {
  def accessType(p: Permission) = p match {
    case _ : ReadPermission =>   "read"  
    case _ : ReducePermission => "reduce"  
    case _ : WritePermission =>  "write"  
    case _ : DeletePermission => "delete"  
  }
  
  object accessTypeExtractor extends Extractor[(Path, Set[AccountId]) => Permission] {
    override def validated(label: JValue) =
      label.validated[String].flatMap {
        case "read" =>   Success(ReadPermission.apply) 
        case "reduce" => Success(ReducePermission.apply) 
        case "write" =>  Success(WritePermission.apply)
        case "delete" => Success(DeletePermission.apply) 
        case t =>        Failure(Invalid("Unknown permission type: " + t))
      }
  }
  
  implicit val permissionDecomposer: Decomposer[Permission] = new Decomposer[Permission] {
    override def decompose(p: Permission): JValue = {
      JObject(List(
        some(jfield("accessType", accessType(p))),
        some(jfield("path", p.path)),
        p.ownerAccountIds.headOption.map(_ => jfield("ownerAccountIds", p.ownerAccountIds))
      ).flatten)
    }
  }

  implicit val permissionExtractor: Extractor[Permission] = new Extractor[Permission] {    
    override def validated(obj: JValue) = 
      ((obj \ "accessType").validated(accessTypeExtractor) |@|
       (obj \ "path").validated[Path] |@|
       (obj \? "ownerAccountIds").map(_.validated[Set[AccountId]]).getOrElse(Success(Set.empty[AccountId]))).apply((c, p, o) => c(p, o))
  }
  
  def unapply(perm: Permission): Option[(Path, Set[AccountId])] = Some((perm.path, perm.ownerAccountIds))
}
