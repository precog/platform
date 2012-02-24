package com.precog.common

import blueeyes.json.JPath
import com.precog.analytics.Path

package object security {
  
  type UID = String

  sealed trait PathRestriction {
    def matches(path: Path): Boolean
  }
  
  //case object AnyPath extends PathRestriction
  case class Subtree(path: Path) extends PathRestriction {
    def matches(test: Path) = { 
      path.equalOrChild(test)
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
  
  case class Permissions(path: Set[MayAccessPath], data: Set[MayAccessData]) {
    def ++(that: Permissions) = Permissions(this.path ++ that.path, this.data ++ that.data)
  }

  object Permissions {
    def apply(path: MayAccessPath*)(data: MayAccessData*): Permissions = Permissions(path.toSet, data.toSet)
  }

  case class Token(uid: UID, issuer: Option[UID], permissions: Permissions, grants: Set[UID], expired: Boolean) {
    val isValid = !expired
  }

  case class Account(account: AccountToken)

  trait XToken {
    def uid: UID
    def permissions: Permissions
    def expired: Boolean
  }

  case class RootToken(uid: UID, permissions: Permissions) extends XToken {
    val expired = false
  }


  case class AccountToken(uid: UID, issuer: UID, permissions: Permissions, grants: Set[UID], expired: Boolean) extends XToken {
 
  }
  case class StandardToken(uid: UID, issuer: UID, permissions: Permissions, expired: Boolean) extends XToken {

  }

}
