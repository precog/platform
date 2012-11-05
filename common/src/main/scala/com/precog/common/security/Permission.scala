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

import blueeyes.json.JsonAST._
import blueeyes.json.serialization.{ ValidatedExtraction, Extractor, Decomposer }
import blueeyes.json.serialization.DefaultSerialization.{ DateTimeDecomposer => _, DateTimeExtractor => _, _ }
import blueeyes.json.serialization.Extractor.Invalid

import scalaz._
import scalaz.std.option._
import scalaz.syntax.apply._

sealed trait Permission {
  def path: Path
  def ownerAccountIds: Set[AccountID]
  
  def implies(other: Permission): Boolean
  
  protected def pathImplies(other: Permission): Boolean = (this, other) match {
    case (Permission(p1, o1), Permission(p2, o2)) if p1.isEqualOrParent(p2) && !o2.isEmpty && o2.subsetOf(o1) => true
    case (Permission(p1, o1), Permission(p2, _))  if p1.isEqualOrParent(p2) && o1.isEmpty => true
    case _ => false
  }
}

case class ReadPermission  (path: Path, ownerAccountIds: Set[AccountID]) extends Permission {
  def implies(other: Permission): Boolean = other match {
    case _ : ReadPermission | _ : ReducePermission => pathImplies(other)
    case _ => false
  }
}

case class ReducePermission(path: Path, ownerAccountIds: Set[AccountID]) extends Permission {
  def implies(other: Permission): Boolean = other match {
    case _ : ReducePermission => pathImplies(other)
    case _ => false
  }
}

case class WritePermission (path: Path, ownerAccountIds: Set[AccountID]) extends Permission {
  def implies(other: Permission): Boolean = other match {
    case _ : WritePermission => pathImplies(other)
    case _ => false
  }
}

case class DeletePermission(path: Path, ownerAccountIds: Set[AccountID]) extends Permission {
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
  
  object accessTypeExtractor extends Extractor[(Path, Set[AccountID]) => Permission] with ValidatedExtraction[(Path, Set[AccountID]) => Permission] {
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
        p.ownerAccountIds.headOption.map(_ => JField("ownerAccountIds", p.ownerAccountIds.serialize))
      ).flatten)
    }
  }

  implicit val permissionExtractor: Extractor[Permission] = new Extractor[Permission] with ValidatedExtraction[Permission] {    
    override def validated(obj: JValue) = 
      ((obj \ "accessType").validated(accessTypeExtractor) |@|
       (obj \ "path").validated[Path] |@|
       (obj \ "ownerAccountIds").validated[Set[AccountID]]).apply((c, p, o) => c(p, o))
  }
  
  def unapply(perm: Permission): Option[(Path, Set[AccountID])] = Some((perm.path, perm.ownerAccountIds))
}
