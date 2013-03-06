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
import scalaz.Validation._
import scalaz.std.option._
import scalaz.syntax.apply._
import scalaz.syntax.plusEmpty._
import Permission._

sealed trait Permission extends Logging {
  def path: Path

  def implies(other: Permission): Boolean
}

sealed trait WrittenByPermission extends Permission {
  def writtenBy: WrittenBy
}

object WrittenByPermission {
  def unapply(perm: WrittenByPermission): Option[(Path, WrittenBy)] = Some((perm.path, perm.writtenBy))
}

case class WritePermission(path: Path, writeAs: WriteAs) extends Permission {
  def implies(other: Permission): Boolean = other match {
    case WritePermission(p0, w0) => path.isEqualOrParent(p0) && (writeAs == WriteAsAny || writeAs == w0)
    case _ => false
  }
}

case class ReadPermission(path: Path, writtenBy: WrittenBy) extends Permission with WrittenByPermission {
  def implies(other: Permission): Boolean = other match {
    case p : ReadPermission => WrittenBy.implies(this, p)
    case p : ReducePermission => WrittenBy.implies(this, p)
    case _ => false
  }
}

case class ReducePermission(path: Path, writtenBy: WrittenBy) extends Permission with WrittenByPermission {
  def implies(other: Permission): Boolean = other match {
    case p : ReducePermission => WrittenBy.implies(this, p)
    case _ => false
  }
}


case class DeletePermission(path: Path, writtenBy: WrittenBy) extends Permission with WrittenByPermission {
  def implies(other: Permission): Boolean = other match {
    case p : DeletePermission => WrittenBy.implies(this, p)
    case _ => false
  }
}

object Permission {
  sealed trait WriteAs 
  case object WriteAsAny extends WriteAs 
  case class WriteAsAll private[Permission] (accountIds: Set[AccountId]) extends WriteAs

  object WriteAs {
    def all(accountIds: NonEmptyList[AccountId]): WriteAs = WriteAsAll(accountIds.list.toSet)
    val any: WriteAs = WriteAsAny

    private[Permission] def apply(accountIds: Set[AccountId]): WriteAs = if (accountIds.isEmpty) WriteAsAny else WriteAsAll(accountIds)

    def apply(accountId: AccountId): WriteAs = apply(Set(accountId))
  }

  sealed trait WrittenBy
  case object WrittenByAny extends WrittenBy
  case class WrittenByAccount(accountId: AccountId) extends WrittenBy

  object WrittenBy {
    def implies(permission: WrittenByPermission, candidate: WrittenByPermission): Boolean = {
      permission.path.isEqualOrParent(candidate.path) &&
      (permission.writtenBy match {
        case WrittenByAny => true
        case WrittenByAccount(accountId) => candidate.writtenBy match {
          case WrittenByAny => false
          case WrittenByAccount(cid) => cid == accountId
        }
      })
    }
  }

  def accessType(p: Permission) = p match {
    case _ : ReadPermission =>   "read"
    case _ : ReducePermission => "reduce"
    case _ : WritePermission =>  "write"
    case _ : DeletePermission => "delete"
  }

  def ownerAccountIds(p: Permission): Set[AccountId] = p match {
    case WritePermission(_, WriteAsAll(ids)) => ids
    case WritePermission(_, WriteAsAny) => Set()
    case WrittenByPermission(_, WrittenByAccount(id)) => Set(id)
    case WrittenByPermission(_, WrittenByAny) => Set()
  }

  val decomposerV1Base: Decomposer[Permission] = new Decomposer[Permission] {
    override def decompose(p: Permission): JValue = {
      JObject(
        "accessType" -> accessType(p).serialize,
        "path" -> p.path.serialize,
        "ownerAccountIds" -> ownerAccountIds(p).serialize
      )
    }
  }

  val extractorV1Base: Extractor[Permission] = new Extractor[Permission] {
    private def writtenByPermission(obj: JValue, pathV: Validation[Error, Path])(f: (Path, WrittenBy) => Permission): Validation[Error, Permission] = {
      (obj \? "ownerAccountIds") map { ids => 
        Apply[({type l[a] = Validation[Error, a]})#l].zip.zip(pathV, ids.validated[Set[AccountId]]) flatMap {
          case (path, accountIds) =>
            if (accountIds.isEmpty) success(f(path, WrittenByAny))
            else if (accountIds.size == 1) success(f(path, WrittenByAccount(accountIds.head)))
            else failure(Invalid("Cannot extract read permission for more than one account ID."))
        }
      } getOrElse {
        pathV map { f(_:Path, WrittenByAny) }
      }
    }

    override def validated(obj: JValue) = {
      val pathV = obj.validated[Path]("path")
      obj.validated[String]("accessType") flatMap {
        case "write" =>  
          (obj \? "ownerAccountIds") map { ids => 
            (pathV |@| ids.validated[Set[AccountId]]) { (path, accountIds) => WritePermission(path, WriteAs(accountIds)) }
          } getOrElse {
            pathV map  { WritePermission(_: Path, WriteAsAny) }
          }

        case "read"   => writtenByPermission(obj, pathV) { ReadPermission.apply _ }
        case "reduce" => writtenByPermission(obj, pathV) { ReducePermission.apply _ }
        case "delete" => writtenByPermission(obj, pathV) { DeletePermission.apply _ }
      }
    }
  }

  val extractorV0: Extractor[Permission] = new Extractor[Permission] {
    private def writtenByPermission(obj: JValue, pathV: Validation[Error, Path])(f: (Path, WrittenBy) => Permission): Validation[Error, Permission] = {
      obj.validated[Option[String]]("ownerAccountId") flatMap { opt =>
        opt map {id => 
          pathV map { f(_:Path, WrittenByAccount(id)) }
        } getOrElse {
          pathV map { f(_:Path, WrittenByAny) }
        }
      }
    }

    override def validated(obj: JValue) = {
      val pathV = obj.validated[Path]("path")
      obj.validated[String]("type") flatMap {
        case "write" =>  
          obj.validated[Option[String]]("ownerAccountId") flatMap { opt => 
            opt map { id =>
              pathV map { WritePermission(_: Path, WriteAs(Set(id))) }
            } getOrElse {
              pathV map { WritePermission(_: Path, WriteAsAny) }
            }
          }

        case "read"   => writtenByPermission(obj, pathV) { ReadPermission.apply _ }
        case "reduce" => writtenByPermission(obj, pathV) { ReducePermission.apply _ }
        case "owner" | "delete" => writtenByPermission(obj, pathV) { DeletePermission.apply _ }
      }
    }
  }

  implicit val decomposer = Serialization.versioned(decomposerV1Base, Some("1.0"))
  implicit val extractor = Serialization.versioned(extractorV1Base, Some("1.0")) <+> extractorV1Base
}
