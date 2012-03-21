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

import akka.dispatch.ExecutionContext
import akka.dispatch.Future

import blueeyes.json.JPath

trait AccessControl {
  def mayAccessPath(uid: UID, path: Path, pathAccess: PathAccess): Future[Boolean]
  def mayAccessData(uid: UID, path: Path, owners: Set[UID], dataAccess: DataAccess): Future[Boolean]
}

class UnlimitedAccessControl(implicit executionContext: ExecutionContext) extends AccessControl {
  def mayAccessPath(uid: UID, path: Path, pathAccess: PathAccess) = Future(true)
  def mayAccessData(uid: UID, path: Path, owners: Set[UID], dataAccess: DataAccess) = Future(true)
}

trait TokenBasedAccessControl extends AccessControl with TokenManagerComponent {

  implicit def executionContext: ExecutionContext

  def mayAccessPath(uid: UID, path: Path, pathAccess: PathAccess): Future[Boolean] = {
    mayAccessPath(uid, path, pathAccess, false)
  }

  def mayAccessPath(uid: UID, path: Path, pathAccess: PathAccess, sharingRequired: Boolean): Future[Boolean] = {
    tokenManager lookup(uid) flatMap { _.map { mayAccessPath(_, path, pathAccess, sharingRequired) } getOrElse { Future(false) } }
  }

  def sharedIfRequired(sharingRequired: Boolean, mayShare: Boolean) = !(sharingRequired && !mayShare)

  def mayAccessPath(token: Token, testPath: Path, testPathAccess: PathAccess, sharingRequired: Boolean): Future[Boolean] = {

    type Perm = (Option[UID], MayAccessPath)

    def findMatchingPermission: Future[Set[Perm]] = {
      def extractMatchingPermissions(issuer: Option[UID], perms: Set[MayAccessPath]): Set[Perm] =
        perms collect {
          case perm @ MayAccessPath(pathSpec, pathAccess, mayShare) 
            if pathAccess == testPathAccess && 
             pathSpec.matches(testPath) &&
             sharedIfRequired(sharingRequired, mayShare) => (issuer, perm)
        }
      
      val localPermissions = extractMatchingPermissions(token.issuer, token.permissions.path)
      
      val grantedPermissions: Future[Set[Perm]] = {
        val nested: Set[Future[Set[Perm]]] = token.grants map {
          tokenManager lookup(_) map { 
            _.map{ grantToken =>
              if(grantToken.isValid) {
                extractMatchingPermissions(grantToken.issuer, grantToken.permissions.path)     
              } else {
                Set.empty[Perm] 
              }
            }.getOrElse(Set.empty[Perm])
          }
        }
        Future.sequence(nested).map{ _.flatten }
      }

      grantedPermissions map { localPermissions ++ _ }
    } 

    def hasValidatedPermission: Future[Boolean] = {
      findMatchingPermission flatMap { perms =>
        if(perms.size == 0) { Future(false) } else {
          Future.reduce(perms.map {
            case (None, perm)         => Future(true)
            case (Some(issuer), perm) => mayAccessPath(issuer, testPath, testPathAccess, true)
          })(_ || _)
        }
      }
    }
    
    hasValidatedPermission map { _ && token.isValid } 
  }
 
  def mayAccessData(uid: UID, path: Path, owners: Set[UID], dataAccess: DataAccess): Future[Boolean] = {
    mayAccessData(uid, path, owners, dataAccess, false)
  }

  def mayAccessData(uid: UID, path: Path, owners: Set[UID], dataAccess: DataAccess, sharingRequired: Boolean): Future[Boolean] = {
    tokenManager lookup(uid) flatMap { _.map { mayAccessData(_, path, owners, dataAccess, sharingRequired) }.getOrElse{ Future(false) } }
  }

  def mayAccessData(token: Token, testPath: Path, testOwners: Set[UID], testDataAccess: DataAccess, sharingRequired: Boolean): Future[Boolean] = {
  
    type Perm = (Option[UID], MayAccessData)

    def findMatchingPermission(testOwner: UID): Future[Set[Perm]] = {
      def extractMatchingPermissions(issuer: Option[UID], perms: Set[MayAccessData]): Future[Set[Perm]] = { 
        val flagged = perms map {
          case perm @ MayAccessData(pathSpec, ownerSpec, dataAccess, mayShare)
            if dataAccess == testDataAccess && 
             pathSpec.matches(testPath) &&
             sharedIfRequired(sharingRequired, mayShare) =>
             checkOwnershipRestriction(testOwner, ownerSpec) map { b => (b, (issuer, perm)) }
          case perm @ MayAccessData(_,_,_,_) => Future((false, (issuer, perm)))
        }
        Future.fold(flagged)(Set.empty[Perm]){
          case (acc, (b, t)) => if(b) acc + t else acc
        }
      }
      
      
      val localPermissions = extractMatchingPermissions(token.issuer, token.permissions.data)
      
      val grantedPermissions: Future[Set[Perm]] = {
        val nested: Set[Future[Set[Perm]]] = token.grants map {
          tokenManager lookup(_) flatMap { 
            _.map{ grantToken =>
              if(grantToken.isValid) {
                extractMatchingPermissions(grantToken.issuer, grantToken.permissions.data)     
              } else {
                Future(Set.empty[Perm])
              }
            }.getOrElse(Future(Set.empty[Perm]))
          }
        }
        Future.sequence(nested).map{ _.flatten }
      }


      localPermissions flatMap { lp => grantedPermissions map { lp ++ _ }}
    } 

    def hasValidatedPermission: Future[Boolean] = {
      Future.reduce(testOwners map { testOwner =>
        findMatchingPermission(testOwner) flatMap { perms =>
          if(perms.size == 0) { Future(false) } else {
            Future.reduce(perms.map { 
              case (None, perm)         => Future(true)
              case (Some(issuer), perm) => mayAccessData(issuer, testPath, Set(testOwner), testDataAccess, true)
            })(_ || _)
          }
        }
      })(_ && _)
    }

    hasValidatedPermission map { _ && token.isValid } 
  }

  def checkOwnershipRestriction(testOwner: UID, restriction: OwnerRestriction): Future[Boolean] = restriction match {
    case OwnerAndDescendants(owner) => isChildOf(owner, testOwner)
  }
  
  def isChildOf(parent: UID, possibleChild: UID): Future[Boolean] = 
    if(parent == possibleChild) 
      Future(true)
    else
      tokenManager lookup(possibleChild) flatMap { 
        _.flatMap {
          _.issuer.map { isChildOf(parent, _) }
        } getOrElse(Future(false))
      } 

}
