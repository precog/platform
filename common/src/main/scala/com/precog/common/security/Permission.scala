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
  sealed trait WriteAs {
    def accountIds: Set[AccountId]
  }
  case object WriteAsAny extends WriteAs {
    def accountIds = Set()
  }
  case class WriteAsAll private[Permission] (accountIds: Set[AccountId]) extends WriteAs

  object WriteAs {
    def all(accountIds: NonEmptyList[AccountId]): WriteAs = WriteAsAll(accountIds.list.toSet)
    val any: WriteAs = WriteAsAny

    private[Permission] def apply(accountIds: Set[AccountId]): WriteAs = if (accountIds.isEmpty) WriteAsAny else WriteAsAll(accountIds)

    def apply(accountId: AccountId): WriteAs = apply(Set(accountId))
  }

  sealed trait WrittenBy {
    def accountIds: Set[AccountId]
  }
  case object WrittenByAny extends WrittenBy {
    def accountIds = Set()
  }
  case class WrittenByOneOf private[Permission] (accountIds: Set[AccountId]) extends WrittenBy

  object WrittenBy {
    def oneOf(accountIds: NonEmptyList[AccountId]): WrittenBy = WrittenByOneOf(accountIds.list.toSet)
    val any: WrittenBy = WrittenByAny

    private[Permission] def apply(accountIds: Set[AccountId]): WrittenBy = if (accountIds.isEmpty) WrittenByAny else WrittenByOneOf(accountIds)

    def apply(authorities: Authorities): WrittenBy = apply(authorities.ownerAccountIds)

    def apply(accountId: AccountId): WrittenBy = apply(Set(accountId))

    def implies(permission: WrittenByPermission, candidate: WrittenByPermission): Boolean = {
      permission.path.isEqualOrParent(candidate.path) &&
      (permission.writtenBy match {
        case WrittenByAny => true
        case WrittenByOneOf(accountIds) => candidate.writtenBy match {
          case WrittenByAny => false
          case WrittenByOneOf(cids) => cids.intersect(accountIds).nonEmpty
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

  def ownerAccountIds(p: Permission) = p match {
    case WritePermission(_, writeAs) => writeAs.accountIds
    case ReadPermission(_, writtenBy) => writtenBy.accountIds
    case ReducePermission(_, writtenBy) => writtenBy.accountIds
    case DeletePermission(_, writtenBy) => writtenBy.accountIds
  }

  implicit object accessTypeExtractor extends Extractor[(Path, Set[AccountId]) => Permission] {
    override def validated(label: JValue) =
      label.validated[String].flatMap {
        case "write" =>  Success((p: Path, ids: Set[AccountId]) => WritePermission(p, WriteAs(ids)))
        case "read" =>   Success((p: Path, ids: Set[AccountId]) => ReadPermission(p, WrittenBy(ids)))
        case "reduce" => Success((p: Path, ids: Set[AccountId]) => ReducePermission(p, WrittenBy(ids)))
        case "delete" => Success((p: Path, ids: Set[AccountId]) => DeletePermission(p, WrittenBy(ids)))
        case t =>        Failure(Invalid("Unknown permission type: " + t))
      }
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
}
