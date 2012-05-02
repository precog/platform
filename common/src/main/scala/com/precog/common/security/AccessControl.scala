package com.precog.common
package security

import akka.dispatch.ExecutionContext
import akka.dispatch.Future

import blueeyes.json.JPath
import org.joda.time.DateTime

import nsecurity._

import scala.collection.mutable

trait AccessControl {
  def mayGrant(uid: UID, permissions: Permissions): Future[Boolean]
  def mayAccessPath(uid: UID, path: Path, pathAccess: PathAccess): Future[Boolean]
  def mayAccessData(uid: UID, path: Path, owners: Set[UID], dataAccess: DataAccess): Future[Boolean]
}

trait NewTokenManager {

  def newUUID() = java.util.UUID.randomUUID.toString

  def newTokenID(): String = (newUUID() + "-" + newUUID()).toUpperCase
  def newGrantID(): String = (newUUID() + newUUID() + newUUID()).toLowerCase.replace("-","")
 
  def newToken(name: String, grants: Set[GrantID]): NToken
  def newGrant(grant: Grant): ResolvedGrant

  def findToken(tid: TokenID): Option[NToken]
  def findGrant(gid: GrantID): Option[ResolvedGrant]
  def findGrantChildren(gid: GrantID): Set[ResolvedGrant]

  def addGrants(token: NToken, grants: Set[GrantID]): NToken
  def removeGrants(token: NToken, grants: Set[GrantID]): NToken

  def deleteToken(token: NToken): Unit 
  def deleteGrant(grant: ResolvedGrant): Unit

} 

class TransientTokenManager(tokens: mutable.Map[TokenID, NToken] = mutable.Map.empty, 
                       grants: mutable.Map[GrantID, Grant] = mutable.Map.empty) extends NewTokenManager {

  def newToken(name: String, grants: Set[GrantID]): NToken = {
    val newToken = NToken(newTokenID, name, grants)
    tokens.put(newToken.tid, newToken)
    newToken
  }

  def newGrant(grant: Grant): ResolvedGrant = {
    val newGrant = ResolvedGrant(newGrantID, grant)
    grants.put(newGrant.gid, grant)
    newGrant
  }

  def findToken(tid: TokenID): Option[NToken] = tokens.get(tid)

  def findGrant(gid: GrantID): Option[ResolvedGrant] = grants.get(gid).map { ResolvedGrant(gid, _) }
  def findGrantChildren(gid: GrantID): Set[ResolvedGrant] =
    grants.filter{ 
      case (_, grant) => grant.issuer.map { _ == gid }.getOrElse(false) 
    }.map{ ResolvedGrant.tupled.apply }(collection.breakOut)

  def addGrants(token: NToken, add: Set[GrantID]): NToken = {
    val updated = token.addGrants(add)
    tokens.put(updated.tid, updated)
    updated
  }
  def removeGrants(token: NToken, remove: Set[GrantID]): NToken = {
    val updated = token.removeGrants(remove)
    tokens.put(updated.tid, updated)
    updated
  }

  def deleteToken(token: NToken) = tokens.remove(token.tid)
  def deleteGrant(grant: ResolvedGrant) = grants.remove(grant.gid)
}

class TokenManagerAccessControl(tokens: NewTokenManager)(implicit execContext: ExecutionContext) extends AccessControl {
  def mayGrant(uid: UID, permissions: Permissions): Future[Boolean] = Future {
    sys.error("todo")
  }

  def mayAccessPath(uid: UID, path: Path, pathAccess: PathAccess): Future[Boolean] = Future {
    pathAccess match {
      case PathRead => mayAccess(uid, path, Set(uid), ReadGrant) 
      case PathWrite => mayAccess(uid, path, Set.empty, WriteGrant)
    }
  }

  def mayAccessData(uid: UID, path: Path, owners: Set[UID], dataAccess: DataAccess): Future[Boolean] = Future {
    mayAccess(uid, path, owners, ReadGrant)
  }
 
  def mayAccess(uid: TokenID, path: Path, owners: Set[UID], accessType: AccessType): Boolean = {
    tokens.findToken(uid).map { t => 
       hasValidGrants(t, path, owners, accessType)
    }.getOrElse(false)
  }

  def hasValidGrants(t: NToken, path: Path, owners: Set[UID], accessType: AccessType): Boolean = {
    accessType match {
      case WriteGrant =>
        t.grants.exists{ gid =>
          tokens.findGrant(gid).map { 
            case ResolvedGrant(_, grant @ WriteGrant(_, _, _)) =>
              grant.path.equalOrChild(path) && isValid(grant)
            case _ => false
          }.getOrElse(false)
        }
      case OwnerGrant =>
        t.grants.exists{ gid =>
          tokens.findGrant(gid).map { 
            case ResolvedGrant(_, grant @ OwnerGrant(_, _, _)) =>
              grant.path.equalOrChild(path) && isValid(grant)
            case _ => false
          }.getOrElse(false)
        }
      case ReadGrant =>
        if(owners.isEmpty) false
        else owners.forall { owner =>
          t.grants.exists{ gid =>
            tokens.findGrant(gid).map { 
              case ResolvedGrant(_, grant @ ReadGrant(_, _, o, _)) =>
                grant.path.equalOrChild(path) && isValid(grant) && owner == o 
              case _ => false
            }.getOrElse(false)
          }
        }
      case ReduceGrant =>
        if(owners.isEmpty) false
        else owners.forall { owner =>
          t.grants.exists{ gid =>
            tokens.findGrant(gid).map { 
              case ResolvedGrant(_, grant @ ReduceGrant(_, _, o, _)) =>
                grant.path.equalOrChild(path) && isValid(grant) && owner == o 
              case _ => false
            }.getOrElse(false)
          }
        }
      case ModifyGrant =>
        if(owners.isEmpty) false
        else owners.forall { owner =>
          t.grants.exists{ gid =>
            tokens.findGrant(gid).map { 
              case ResolvedGrant(_, grant @ ModifyGrant(_, _, o, _)) =>
                grant.path.equalOrChild(path) && isValid(grant) && owner == o 
              case _ => false
            }.getOrElse(false)
          }
        }
      case TransformGrant =>
        if(owners.isEmpty) false
        else owners.forall { owner =>
          t.grants.exists{ gid =>
            tokens.findGrant(gid).map { 
              case ResolvedGrant(_, grant @ TransformGrant(_, _, o, _)) =>
                grant.path.equalOrChild(path) && isValid(grant) && owner == o 
              case _ => false
            }.getOrElse(false)
          }
        }
    }
  }

  def isValid(grant: Grant): Boolean = {
    !grant.isExpired(new DateTime()) && grant.issuer.map { 
      tokens.findGrant(_).map { parentGrant => 
        grant.accessType == parentGrant.grant.accessType && isValid(parentGrant.grant) 
      }.getOrElse(false) 
    }.getOrElse(true) 
  }
}

class UnlimitedAccessControl(implicit executionContext: ExecutionContext) extends AccessControl {
  def mayGrant(uid: UID, permissions: Permissions) = Future(true)
  def mayAccessPath(uid: UID, path: Path, pathAccess: PathAccess) = Future(true)
  def mayAccessData(uid: UID, path: Path, owners: Set[UID], dataAccess: DataAccess) = Future(true)
}

trait TokenBasedAccessControl extends AccessControl with TokenManagerComponent {

  implicit def executionContext: ExecutionContext

  def mayGrant(uid: UID, permissions: Permissions) = {
    tokenManager.lookup(uid) flatMap {
      case None => Future(false)
      case Some(t) =>
        val pathsValid = Future.reduce(permissions.path.map { p => validPathGrant(t, p) }){ _ && _ }
        val dataValid = Future.reduce(permissions.data.map { p => validDataGrant(t, p) }){ _ && _ }
            
        (pathsValid zip dataValid) map { t => t._1 && t._2 }
    }
  }

  private def validPathGrant(token: Token, path: MayAccessPath): Future[Boolean] = Future {
    token.permissions.sharable.path.map { ref =>
      pathSpecSubset(ref.pathSpec, path.pathSpec) && ref.pathAccess == path.pathAccess
    }.reduce{ _ || _ }
  }

  private def sharablePermissions(token: Token): Future[Permissions] = {

    val foldPerms = Future.fold(_: Set[Future[Permissions]])(token.permissions.sharable) {
      case (acc, el) => acc ++ el
    }

    val extractPerms = (_: Token).grants.map { tokenManager.lookup(_).map {
      case Some(t) => t.permissions.sharable
      case None    => Permissions.empty
    }}

    extractPerms andThen foldPerms apply token
  }
  
  private def validDataGrant(token: Token, data: MayAccessData): Future[Boolean] = {
    val tests: Set[Future[Boolean]] = token.permissions.sharable.data.map { ref =>
      ownerSpecSubset(token.uid, ref.ownershipSpec, token.uid, data.ownershipSpec) map {
        _ && pathSpecSubset(ref.pathSpec, data.pathSpec) && ref.dataAccess == data.dataAccess
      }
    }
    Future.reduce(tests.toList){ _ || _ }
  }

  private def pathSpecSubset(ref: PathRestriction, test: PathRestriction): Boolean =
    (ref, test) match {
      case (Subtree(r), Subtree(t)) => r.equalOrChild(t)
      case _                        => false
    }

  private def ownerSpecSubset(refUID: UID, ref: OwnerRestriction, testUID: UID, test: OwnerRestriction): Future[Boolean] = {

    def effectiveUID(ownerSpec: OwnerRestriction, holder: UID) = ownerSpec match {
      case OwnerAndDescendants(o) => o
      case HolderAndDescendants   => holder 
    }

    def childOrEqual(ruid: UID, tuid: UID) = 
      if(ruid == tuid) {
        Future(true)
      } else {
        tokenManager.lookup(ruid) flatMap {
          case None    => Future(false)
          case Some(rt) =>
            tokenManager.getDescendant(rt, tuid) map { _.isDefined }
          }
      }

    childOrEqual(effectiveUID(ref, refUID), effectiveUID(test, testUID))
  }

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

    def findMatchingPermission(holderUID: UID, testOwner: UID): Future[Set[Perm]] = {
     
      // start work here!!!!

      def extractMatchingPermissions(issuer: Option[UID], perms: Set[MayAccessData]): Future[Set[Perm]] = { 
        val flagged = perms map {
          case perm @ MayAccessData(pathSpec, ownerSpec, dataAccess, mayShare)
            if dataAccess == testDataAccess && 
             pathSpec.matches(testPath) &&
             sharedIfRequired(sharingRequired, mayShare) =>
             checkOwnershipRestriction(holderUID, testOwner, ownerSpec) map { b => (b, (issuer, perm)) }
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

    def hasValidatedPermission(holderUID: UID): Future[Boolean] = {
      Future.reduce(testOwners map { testOwner =>
        findMatchingPermission(holderUID, testOwner) flatMap { perms =>
          if(perms.size == 0) { Future(false) } else {
            Future.reduce(perms.map { 
              case (None, perm)         => Future(true)
              case (Some(issuer), perm) => mayAccessData(issuer, testPath, Set(testOwner), testDataAccess, true)
            })(_ || _)
          }
        }
      })(_ && _)
    }

    hasValidatedPermission(token.uid) map { _ && token.isValid } 
  }

  def checkOwnershipRestriction(grantHolder: UID, testOwner: UID, restriction: OwnerRestriction): Future[Boolean] = restriction match {
    case OwnerAndDescendants(owner) => isChildOf(owner, testOwner)
    case HolderAndDescendants       => isChildOf(grantHolder, testOwner)
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
