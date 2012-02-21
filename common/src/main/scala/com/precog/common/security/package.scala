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

package object security {
  
  type Path = String
  type JPath = String
  type UID = String

  sealed trait PathRestriction {
    def matches(path: Path): Boolean
  }
  
  //case object AnyPath extends PathRestriction
  case class Subtree(path: Path) extends PathRestriction {
    def matches(test: Path) = { 
      test.startsWith(path) 
    }   
  }
 
  sealed trait OwnerRestriction

  //case class AnyOwner extends OwnerRestriction
  case class OwnerAndDescendants(owner: UID) extends OwnerRestriction

  sealed trait PathAccess

  case object PathRead extends PathAccess
  case object PathWrite extends PathAccess

  sealed trait DataAccess

  case object DataQuery extends DataAccess

  sealed trait Permission {
    def mayShare: Boolean
  }

  case class MayAccessPath(pathSpec: PathRestriction, pathAccess: PathAccess, mayShare: Boolean) extends Permission  
  case class MayAccessData(pathSpec: PathRestriction, ownershipSpec: OwnerRestriction, dataAccessType: DataAccess, mayShare: Boolean) extends Permission
  
  case class Permissions(path: Set[MayAccessPath], data: Set[MayAccessData])

  object Permissions {
    def apply(path: MayAccessPath*)(data: MayAccessData*): Permissions = Permissions(path.toSet, data.toSet)
  }

  case class Token(uid: UID, issuer: Option[UID], permissions: Permissions, grants: Set[UID], expired: Boolean) {
    val isValid = !expired
  }

}
