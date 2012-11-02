package com.precog.common
package security

import blueeyes.json.JsonAST._
import blueeyes.json.serialization.{ ValidatedExtraction, Extractor, Decomposer }
import blueeyes.json.serialization.DefaultSerialization.{ DateTimeDecomposer => _, DateTimeExtractor => _, _ }
import blueeyes.json.serialization.Extractor.Invalid

import scalaz._
import scalaz.std.option._
import scalaz.syntax.apply._

sealed trait Permission {
  def path: Path
  def ownerAccountId: Option[AccountID]
  
  def implies(other: Permission): Boolean
  
  protected def pathImplies(other: Permission): Boolean = (this, other) match {
    case (Permission(p1, Some(o1)), Permission(p2, Some(o2))) if p1.isEqualOrParent(p2) && o1 == o2 => true
    case (Permission(p1, None),     Permission(p2, _))        if p1.isEqualOrParent(p2)             => true
    case _ => false
  }
}

case class ReadPermission  (path: Path, ownerAccountId: Option[AccountID]) extends Permission {
  def implies(other: Permission): Boolean = other match {
    case _ : ReadPermission | _ : ReducePermission => pathImplies(other)
    case _ => false
  }
}

case class ReducePermission(path: Path, ownerAccountId: Option[AccountID]) extends Permission {
  def implies(other: Permission): Boolean = other match {
    case _ : ReducePermission => pathImplies(other)
    case _ => false
  }
}

case class WritePermission (path: Path, ownerAccountId: Option[AccountID]) extends Permission {
  def implies(other: Permission): Boolean = other match {
    case _ : WritePermission => pathImplies(other)
    case _ => false
  }
}

case class DeletePermission(path: Path, ownerAccountId: Option[AccountID]) extends Permission {
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
  
  object accessTypeExtractor extends Extractor[(Path, Option[AccountID]) => Permission] with ValidatedExtraction[(Path, Option[AccountID]) => Permission] {
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
        some(JField("accessType", accessType(p))),
        some(JField("path", p.path)),
        p.ownerAccountId.map(JField("ownerAccountId", _))
      ).flatten)
    }
  }

  implicit val permissionExtractor: Extractor[Permission] = new Extractor[Permission] with ValidatedExtraction[Permission] {    
    override def validated(obj: JValue) = 
      ((obj \ "accessType").validated(accessTypeExtractor) |@|
       (obj \ "path").validated[Path] |@|
       (obj \ "ownerAccountId").validated[Option[AccountID]]).apply((c, p, o) => c(p, o))
  }
  
  def unapply(perm: Permission): Option[(Path, Option[AccountID])] = Some((perm.path, perm.ownerAccountId))
}
