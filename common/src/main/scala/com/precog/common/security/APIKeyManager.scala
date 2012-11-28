package com.precog.common
package security

import com.weiglewilczek.slf4s.Logging

import org.joda.time.DateTime

import scalaz._
import scalaz.std.option._
import scalaz.std.set._
import scalaz.syntax.monad._
import scalaz.syntax.traverse._

trait APIKeyManager[M[+_]] extends AccessControl[M] with Logging {
  implicit val M : Monad[M]
  
  def newUUID() = java.util.UUID.randomUUID.toString

  // 128 bit API key
  def newAPIKey(): String = newUUID().toUpperCase

  // 384 bit grant ID
  def newGrantId(): String = (newUUID() + newUUID() + newUUID()).toLowerCase.replace("-","")
  
  def rootGrantId: M[GrantId]
  def rootAPIKey:  M[APIKey]
 
  def newGrant(name: Option[String], description: Option[String], issuerKey: APIKey, parentIds: Set[GrantId], perms: Set[Permission], expiration: Option[DateTime]): M[Grant]
  
  def newAPIKey(name: Option[String], description: Option[String], issuerKey: APIKey, grants: Set[GrantId]): M[APIKeyRecord]

  def newAccountGrant(accountId: AccountId, name: Option[String] = None, description: Option[String] = None, issuerKey: APIKey, parentIds: Set[GrantId], expiration: Option[DateTime] = None): M[Grant] = {
    val path = "/"+accountId+"/"
    // Path is "/" so that an account may read data it owns no matter what path it exists under. See AccessControlSpec, NewGrantRequest
    val readPerms =  Set(ReadPermission, ReducePermission).map(_(Path("/"), Set(accountId)) : Permission)
    val writePerms = Set(WritePermission, DeletePermission).map(_(Path(path), Set()) : Permission)
    newGrant(name, description, issuerKey, parentIds, readPerms ++ writePerms, expiration)
  }
  
  def newStandardAccountGrant(accountId: String, path: Path, name: Option[String] = None, description: Option[String] = None): M[Grant] =
    for {
      rk <- rootAPIKey
      rg <- rootGrantId
      ng <- newAccountGrant(accountId, name, description, rk, Set(rg), None)
    } yield ng

  def newStandardAPIKeyRecord(accountId: String, path: Path, name: Option[String] = None, description: Option[String] = None): M[APIKeyRecord] = {
    val grantName = name.map(_+"-grant")
    val grantDescription = name.map(_+" account grant")
    val grant = newStandardAccountGrant(accountId, path: Path, grantName, grantDescription)
    for {
      rk <- rootAPIKey
      ng <- grant
      nk <- newAPIKey(name, description, rk, Set(ng.grantId))
    } yield nk
  }
  
  def listAPIKeys(): M[Seq[APIKeyRecord]]
  def listGrants(): M[Seq[Grant]]
  
  def findAPIKey(apiKey: APIKey): M[Option[APIKeyRecord]]
  def findGrant(gid: GrantId): M[Option[Grant]]
  def findGrantChildren(gid: GrantId): M[Set[Grant]]

  def listDeletedAPIKeys(): M[Seq[APIKeyRecord]]
  def listDeletedGrants(): M[Seq[Grant]]

  def findDeletedAPIKey(apiKey: APIKey): M[Option[APIKeyRecord]]
  def findDeletedGrant(gid: GrantId): M[Option[Grant]]
  def findDeletedGrantChildren(gid: GrantId): M[Set[Grant]]

  def addGrants(apiKey: APIKey, grants: Set[GrantId]): M[Option[APIKeyRecord]]
  def removeGrants(apiKey: APIKey, grants: Set[GrantId]): M[Option[APIKeyRecord]]


  def deleteAPIKey(apiKey: APIKey): M[Option[APIKeyRecord]]
  def deleteGrant(apiKey: GrantId): M[Set[Grant]]

  def close(): M[Unit]

  def isValidGrant(grantId: GrantId, at: Option[DateTime] = None): M[Option[Grant]] =
    findGrant(grantId).flatMap { grantOpt =>
      grantOpt.map { grant =>
        if(grant.isExpired(at)) None.point[M]
        else grant.parentIds.foldLeft(some(grant).point[M]) { case (accM, parentId) =>
          accM.flatMap(_.map { grant => isValidGrant(parentId, at).map(_ => grant) }.sequence)
        }
      }.getOrElse(None.point[M])
    }
  
  def validGrants(apiKey: APIKey, at: Option[DateTime] = None): M[Set[Grant]] = {
    logger.trace("Checking grant validity for apiKey " + apiKey)
    findAPIKey(apiKey).flatMap(_.map { apiKeyRecord =>
      apiKeyRecord.grants.map(isValidGrant(_, at)).sequence.map(_.flatten)
    }.getOrElse(Set.empty.point[M]))
  }

  def hasCapability(apiKey: APIKey, perms: Set[Permission], at: Option[DateTime] = None): M[Boolean] =
    validGrants(apiKey, at).map(Grant.implies(_, perms, at))

  def deriveGrant(name: Option[String], description: Option[String], issuerKey: APIKey, perms: Set[Permission], expiration: Option[DateTime] = None): M[Option[GrantId]] = {
    validGrants(issuerKey, expiration).flatMap { grants =>
      if(!Grant.implies(grants, perms, expiration)) none[GrantId].point[M]
      else {
        val minimized = Grant.coveringGrants(grants, perms, expiration).map(_.grantId)
        if(minimized.isEmpty) none[GrantId].point[M]
        else newGrant(name, description, issuerKey, minimized, perms, expiration).map { grant => some(grant.grantId) }
      }
    }
  }
  
  def deriveSingleParentGrant(name: Option[String], description: Option[String], issuerKey: APIKey, parentId: GrantId, perms: Set[Permission], expiration: Option[DateTime] = None): M[Option[GrantId]] = {
    validGrants(issuerKey, expiration).flatMap { validGrants =>
      validGrants.find(_.grantId == parentId) match {
        case Some(parent) if parent.implies(perms, expiration) =>
          newGrant(name, description, issuerKey, Set(parentId), perms, expiration).map { grant => some(grant.grantId) }
        case _ => none[GrantId].point[M]
      }
    }
  }
  
  def deriveAndAddGrant(name: Option[String], description: Option[String], issuerKey: APIKey, perms: Set[Permission], recipientKey: APIKey, expiration: Option[DateTime] = None): M[Option[GrantId]] = {
    deriveGrant(name, description, issuerKey, perms, expiration).flatMap(_ match {
      case Some(grantId) => addGrants(recipientKey, Set(grantId)).map(_.map(_ => grantId))
      case _ => none[GrantId].point[M]
    })
  }
  
  def newAPIKeyWithGrants(name: Option[String], description: Option[String], issuerKey: APIKey, grants: Set[NewGrantRequest]): M[Option[APIKey]] = {
    grants.map { grant =>
      hasCapability(issuerKey, grant.permissions, grant.expirationDate)
    }.sequence.map(_.foldLeft(true)(_ && _)).flatMap { mayGrant =>
      if (mayGrant) {
        val mgrantIds = grants.map(grant => deriveGrant(None, None, issuerKey, grant.permissions, grant.expirationDate)).sequence.map(_.flatten)
        mgrantIds.flatMap { grantIds => newAPIKey(name, description, issuerKey, grantIds) }.map { r => some(r.apiKey) }
      } else {
        none[APIKey].point[M]
      }
    }
  } 
} 
