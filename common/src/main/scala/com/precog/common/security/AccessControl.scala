package com.precog.common.security

trait AccessControl extends TokenManagerComponent {
  def mayAccessPath(uid: UID, path: Path, pathAccess: PathAccess, sharingRequired: Boolean = false): Boolean = {
    tokenManager lookup(uid) map { mayAccessPath(_, path, pathAccess, sharingRequired) } getOrElse { false }
  }

  def sharedIfRequired(sharingRequired: Boolean, mayShare: Boolean) = !(sharingRequired && !mayShare)

  def mayAccessPath(token: Token, testPath: Path, testPathAccess: PathAccess, sharingRequired: Boolean): Boolean = {
    def findMatchingPermission: Set[(Option[UID], MayAccessPath)] = {
      def extractMatchingPermissions(issuer: Option[UID], perms: Set[MayAccessPath]): Set[(Option[UID], MayAccessPath)] =
          perms collect {
            case perm @ MayAccessPath(pathSpec, pathAccess, mayShare) 
              if pathAccess == testPathAccess && 
               pathSpec.matches(testPath) &&
               sharedIfRequired(sharingRequired, mayShare) => (issuer, perm)
        }
      
      val localPermissions = extractMatchingPermissions(token.issuer, token.permissions.path)
      
      val grantedPermissions = token.grants flatMap { 
        tokenManager lookup(_) map { grantToken =>
          if(grantToken.isValid) {
            extractMatchingPermissions(grantToken.issuer, grantToken.permissions.path)     
          } else {
            Set[(Option[UID], MayAccessPath)]()
          }
        } getOrElse(Set())
      }

      localPermissions ++ grantedPermissions
    } 

    def hasValidatedPermission: Boolean = {
      findMatchingPermission exists {
        case (None, perm)         => true
        case (Some(issuer), perm) => mayAccessPath(issuer, testPath, testPathAccess, true)
      }
    }

    token.isValid && hasValidatedPermission
  }
  
  def mayAccessData(uid: UID, path: Path, owners: Set[UID], dataAccess: DataAccess, sharingRequired: Boolean = false): Boolean = {
    tokenManager lookup(uid) map { mayAccessData(_, path, owners, dataAccess, sharingRequired) } getOrElse { false }
  }

  def mayAccessData(token: Token, testPath: Path, testOwners: Set[UID], testDataAccess: DataAccess, sharingRequired: Boolean): Boolean = { 
    def mayAccessData0(uid: UID, testPath: Path, testOwners: Set[UID], testDataAccess: DataAccess, sharingRequired: Boolean): Boolean = {
      tokenManager lookup(uid) map { mayAccessData1(_, testPath, testOwners, testDataAccess, sharingRequired) } getOrElse { false }
    }

    def mayAccessData1(token: Token, testPath: Path, testOwners: Set[UID], testDataAccess: DataAccess, sharingRequired: Boolean): Boolean = {

      def findMatchingPermission(testOwner: UID): Set[(Option[UID], MayAccessData)] = {
        def extractMatchingPermissions(issuer: Option[UID], perms: Set[MayAccessData]): Set[(Option[UID], MayAccessData)] = {
          perms collect {
            case perm @ MayAccessData(pathSpec, ownerSpec, dataAccess, mayShare) 
                 if dataAccess == testDataAccess && 
                    pathSpec.matches(testPath) && 
                    checkOwnershipRestriction(testOwner, ownerSpec) => (issuer, perm)
          }
        }
     
        val localPermissions = extractMatchingPermissions(token.issuer, token.permissions.data)

        val grantedPermissions = token.grants flatMap {
          tokenManager lookup(_) map { grantToken =>
            extractMatchingPermissions(grantToken.issuer, grantToken.permissions.data)
          } getOrElse(Set())
        }

        localPermissions ++ grantedPermissions
      }

      def hasValidatedPermission: Boolean = {
        testOwners forall { testOwner =>
          if(testOwner == token.uid || isChildOf(token.uid, testOwner)) {
            true
          } else {
            findMatchingPermission(testOwner) exists {
              case (None, perm)         => true
              case (Some(issuer), perm) => 
                mayAccessData0(issuer, testPath, Set(testOwner), testDataAccess, true)
            }
          }
        }
      }

      token.isValid && hasValidatedPermission
    }

    mayAccessPath(token, testPath, PathRead, false) && mayAccessData1(token, testPath, testOwners, testDataAccess, sharingRequired)
  }

  def checkOwnershipRestriction(testOwner: UID, restriction: OwnerRestriction): Boolean = restriction match {
    case OwnerAndDescendants(owner) => isChildOf(owner, testOwner)
  }
  
  def isChildOf(parent: UID, possibleChild: UID): Boolean = if(parent == possibleChild) true else
    tokenManager lookup(possibleChild) flatMap {
      _.issuer.map { parent == _ }
    } getOrElse(false)

}
