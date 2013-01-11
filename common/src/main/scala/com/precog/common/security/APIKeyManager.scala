package com.precog.common
package security

import service._
import com.precog.common.accounts.AccountId
import com.precog.common.cache.Cache

import akka.util.Duration
import java.util.concurrent.TimeUnit._
import com.weiglewilczek.slf4s.Logging
import org.joda.time.DateTime

import scalaz._
import scalaz.std.option._
import scalaz.std.set._
import scalaz.syntax.id._
import scalaz.syntax.monad._
import scalaz.syntax.traverse._

object APIKeyManager {
  def newUUID() = java.util.UUID.randomUUID.toString

  // 128 bit API key
  def newAPIKey(): String = newUUID().toUpperCase

  // 384 bit grant ID
  def newGrantId(): String = (newUUID() + newUUID() + newUUID()).toLowerCase.replace("-","")
}

trait APIKeyManager[M[+_]] extends Logging {
  implicit def M: Monad[M]

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
  
  def listAPIKeys: M[Seq[APIKeyRecord]]
  def findAPIKey(apiKey: APIKey): M[Option[APIKeyRecord]]

  def listGrants: M[Seq[Grant]]
  def findGrant(gid: GrantId): M[Option[Grant]]
  def findGrantChildren(gid: GrantId): M[Set[Grant]]

  def listDeletedAPIKeys(): M[Seq[APIKeyRecord]]
  def findDeletedAPIKey(apiKey: APIKey): M[Option[APIKeyRecord]]

  def listDeletedGrants(): M[Seq[Grant]]
  def findDeletedGrant(gid: GrantId): M[Option[Grant]]
  def findDeletedGrantChildren(gid: GrantId): M[Set[Grant]]

  def addGrants(apiKey: APIKey, grants: Set[GrantId]): M[Option[APIKeyRecord]]
  def removeGrants(apiKey: APIKey, grants: Set[GrantId]): M[Option[APIKeyRecord]]

  def deleteAPIKey(apiKey: APIKey): M[Option[APIKeyRecord]]
  def deleteGrant(apiKey: GrantId): M[Set[Grant]]

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
  
  def newAPIKeyWithGrants(name: Option[String], description: Option[String], issuerKey: APIKey, grants: Set[v1.NewGrantRequest]): M[Option[APIKey]] = {
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

case class CachingAPIKeyManagerSettings(
  apiKeyCacheSettings: Seq[Cache.CacheOption],
  grantCacheSettings: Seq[Cache.CacheOption]
)

object CachingAPIKeyManagerSettings {
  val Default = CachingAPIKeyManagerSettings(
    Seq(Cache.ExpireAfterWrite(Duration(5, MINUTES)), Cache.MaxSize(1000)),
    Seq(Cache.ExpireAfterWrite(Duration(5, MINUTES)), Cache.MaxSize(1000))
  )
}

class CachingAPIKeyManager[M[+_]](manager: APIKeyManager[M], settings: CachingAPIKeyManagerSettings = CachingAPIKeyManagerSettings.Default) extends APIKeyManager[M] {
  implicit val M = manager.M

  private val apiKeyCache = Cache.simple[APIKey, APIKeyRecord](settings.apiKeyCacheSettings: _*)
  private val grantCache = Cache.simple[GrantId, Grant](settings.grantCacheSettings: _*)

  def findAPIKey(tid: APIKey) = apiKeyCache.get(tid) match {
    case None => manager.findAPIKey(tid).map { _.map { _ tap add } }
    case t    => M.point(t)
  }

  def findGrant(gid: GrantId) = grantCache.get(gid) match {
    case None        => manager.findGrant(gid).map { _.map { _ tap add } }
    case s @ Some(_) => M.point(s)
  }

  protected def add(r: APIKeyRecord) = apiKeyCache.put(r.apiKey, r)
  protected def add(g: Grant) = grantCache.put(g.grantId, g)

  protected def remove(r: APIKeyRecord) = apiKeyCache.remove(r.apiKey)
  protected def remove(g: Grant) = grantCache.remove(g.grantId)

  def rootGrantId: M[GrantId] = manager.rootGrantId
  def rootAPIKey: M[APIKey] = manager.rootAPIKey
  
  def newAPIKey(name: Option[String], description: Option[String], issuerKey: APIKey, grants: Set[GrantId]) =
    manager.newAPIKey(name, description, issuerKey, grants).map { _ tap add }

  def newGrant(name: Option[String], description: Option[String], issuerKey: APIKey, parentIds: Set[GrantId], perms: Set[Permission], expiration: Option[DateTime]) =
    manager.newGrant(name, description, issuerKey, parentIds, perms, expiration).map { _ tap add }

  def listAPIKeys() = manager.listAPIKeys
  def listGrants() = manager.listGrants

  def listDeletedAPIKeys() = manager.listDeletedAPIKeys
  def listDeletedGrants() = manager.listDeletedGrants

  def findGrantChildren(gid: GrantId) = manager.findGrantChildren(gid)

  def findDeletedAPIKey(tid: APIKey) = manager.findDeletedAPIKey(tid)
  def findDeletedGrant(gid: GrantId) = manager.findDeletedGrant(gid)
  def findDeletedGrantChildren(gid: GrantId) = manager.findDeletedGrantChildren(gid)

  def addGrants(tid: APIKey, grants: Set[GrantId]) =
    manager.addGrants(tid, grants).map { _.map { _ tap add } }
  def removeGrants(tid: APIKey, grants: Set[GrantId]) =
    manager.removeGrants(tid, grants).map { _.map { _ tap add } }

  def deleteAPIKey(tid: APIKey) =
    manager.deleteAPIKey(tid) map { _.map { _ tap remove } }
  def deleteGrant(gid: GrantId) =
    manager.deleteGrant(gid) map { _.map { _ tap remove } }
}

// vim: set ts=4 sw=4 et:
