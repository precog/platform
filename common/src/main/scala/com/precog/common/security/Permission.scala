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

import blueeyes.json._
import blueeyes.json.serialization.{ Extractor, Decomposer }
import blueeyes.json.serialization.Extractor.Error
import blueeyes.json.serialization.Extractor.Invalid
import blueeyes.json.serialization.DefaultSerialization.{ DateTimeDecomposer => _, DateTimeExtractor => _, _ }
import blueeyes.json.serialization.Versioned._

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
    case WritePermission(p0, w0) => path.isEqualOrParentOf(p0) && (writeAs == WriteAsAny || writeAs == w0)
    case _ => false
  }
}

case class ExecutePermission(path: Path) extends Permission {
  def implies(other: Permission): Boolean = other match {
    case ExecutePermission(path0) => path.isEqualOrParentOf(path0)
    case _ => false
  }
}

case class ReadPermission(path: Path, writtenBy: WrittenBy) extends Permission with WrittenByPermission {
  def implies(other: Permission): Boolean = other match {
    case p : ReadPermission => WrittenBy.implies(this, p)
    case p : ReducePermission => WrittenBy.implies(this, p)
    case ExecutePermission(path0) => path.isEqualOrParentOf(path0)
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
      permission.path.isEqualOrParentOf(candidate.path) &&
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
    case _ : ExecutePermission => "execute"
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
      obj.validated[String]("accessType").map(_.toLowerCase.trim) flatMap {
        case "write" =>
          (obj \? "ownerAccountIds") map { ids =>
            (pathV |@| ids.validated[Set[AccountId]]) { (path, accountIds) => WritePermission(path, WriteAs(accountIds)) }
          } getOrElse {
            pathV map  { WritePermission(_: Path, WriteAsAny) }
          }

        case "read"   => writtenByPermission(obj, pathV) { ReadPermission.apply _ }
        case "reduce" => writtenByPermission(obj, pathV) { ReducePermission.apply _ }
        case "owner" | "delete" => writtenByPermission(obj, pathV) { DeletePermission.apply _ }
        case other => failure(Invalid("Unrecognized permission type: " + other))
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
      obj.validated[String]("type").map(_.toLowerCase.trim) flatMap {
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
        case other => failure(Invalid("Unrecognized permission type: " + other))
      }
    }
  }

  implicit val decomposer = decomposerV1Base.versioned(Some("1.0".v))
  implicit val extractor = extractorV1Base.versioned(Some("1.0".v)) <+> extractorV1Base
}
