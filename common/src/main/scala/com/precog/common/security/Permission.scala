package com.precog.common
package security

import json._
import accounts.AccountId

import blueeyes.json._
import blueeyes.json.serialization.{ Extractor, Decomposer }
import blueeyes.json.serialization.Extractor.Error
import blueeyes.json.serialization.Extractor.Invalid
import blueeyes.json.serialization.DefaultSerialization.{ DateTimeDecomposer => _, DateTimeExtractor => _, _ }

import com.weiglewilczek.slf4s.Logging

import scalaz._
import scalaz.std.option._
import scalaz.syntax.apply._
import scalaz.syntax.plusEmpty._

sealed trait Permission extends Logging {
  def path: Path
  def ownerAccountIds: Set[AccountId]

  def implies(other: Permission): Boolean
  
  protected def pathImplies(other: Permission): Boolean = {
    logger.trace("Checking path impliciation of this (%s) against that (%s)".format(this, other))
    (this, other) match {
      case (Permission(p1, o1), Permission(p2, o2)) =>
        // semantics:
        // for the set of owner ids of this permission
        // -- if empty, this implies "as anyone"
        // -- if nonempty, this implies "as these individuals"
        // for the set of owner ids of the other
        // -- if empty, this implies "as someone" - therefore, o1 may be empty (implying any) or nonempty
        //    (in which case, the action must be taken as one of those)
        // -- if nonempty, this implies "as this specific" - therefore, o1 must be empty or contain this id
        p1.isEqualOrParent(p2) && (o1.isEmpty || o2.isEmpty || (o2.nonEmpty && o2.subsetOf(o1)))
    }
  }
}

case class ReadPermission(path: Path, ownerAccountIds: Set[AccountId]) extends Permission {
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

  implicit object accessTypeExtractor extends Extractor[(Path, Set[AccountId]) => Permission] {
    override def validated(label: JValue) =
      label.validated[String].flatMap {
        case "read" =>   Success(ReadPermission.apply)
        case "reduce" => Success(ReducePermission.apply)
        case "write" =>  Success(WritePermission.apply)
        case "delete" => Success(DeletePermission.apply)
        case t =>        Failure(Invalid("Unknown permission type: " + t))
      }
  }

  val decomposerV1Base: Decomposer[Permission] = new Decomposer[Permission] {
    override def decompose(p: Permission): JValue = {
      JObject(List(
        some(jfield("accessType", accessType(p))),
        some(jfield("path", p.path)),
        p.ownerAccountIds.headOption.map(_ => jfield("ownerAccountIds", p.ownerAccountIds))
      ).flatten)
    }
  }

  val extractorV1Base: Extractor[Permission] = new Extractor[Permission] {
    override def validated(obj: JValue) = {
      val accessTypeV = obj.validated[(Path, Set[AccountId]) => Permission]("accessType")
      val pathV = obj.validated[Path]("path")
      val ownerV = (obj \? "ownerAccountIds") map { _.validated[Set[AccountId]] } getOrElse { Success(Set.empty[AccountId]) }

      Apply[({ type l[a] = Validation[Error, a] })#l].apply3(accessTypeV, pathV, ownerV) { (c, p, o) =>
        c(p, o)
      }
    }
  }

  implicit val decomposer = Serialization.versioned(decomposerV1Base, Some("1.0"))
  implicit val extractor = Serialization.versioned(extractorV1Base, Some("1.0")) <+> extractorV1Base

  def unapply(perm: Permission): Option[(Path, Set[AccountId])] = Some((perm.path, perm.ownerAccountIds))
}
