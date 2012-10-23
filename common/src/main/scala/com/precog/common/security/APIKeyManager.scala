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

import blueeyes._
import blueeyes.bkka._
import blueeyes.json.JsonAST._
import blueeyes.persistence.mongo._
import blueeyes.persistence.cache._
import blueeyes.json.serialization.{ ValidatedExtraction, Extractor, Decomposer }
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.json.serialization.Extractor._

import akka.actor.ActorSystem
import akka.util.Timeout
import akka.dispatch.Future
import akka.dispatch.ExecutionContext

import org.joda.time.DateTime

import java.util.concurrent.TimeUnit._

import com.weiglewilczek.slf4s.Logging

import org.streum.configrity.Configuration

import scala.collection.mutable

import scalaz._
import scalaz.Validation._
import scalaz.syntax.monad._

trait APIKeyManager[M[+_]] {
  private[security] def newUUID() = java.util.UUID.randomUUID.toString

  // 256 bit API key
  //private[security] def newAPIKeyID(): String = (newUUID() + "-" + newUUID()).toUpperCase

  // 128 bit API key
  private[security] def newAPIKey(): String = newUUID().toUpperCase

  // 384 bit grant ID
  private[security] def newGrantID(): String = (newUUID() + newUUID() + newUUID()).toLowerCase.replace("-","")
 
  def newAPIKey(name: String, creator: APIKey, grants: Set[GrantID]): M[APIKeyRecord]
  def newGrant(issuer: Option[GrantID], permission: Permission): M[Grant]

  def listAPIKeys(): M[Seq[APIKeyRecord]]
  def listGrants(): M[Seq[Grant]]
  
  def findAPIKey(tid: APIKey): M[Option[APIKeyRecord]]
  def findGrant(gid: GrantID): M[Option[Grant]]
  def findGrantChildren(gid: GrantID): M[Set[Grant]]

  def listDeletedAPIKeys(): M[Seq[APIKeyRecord]]
  def listDeletedGrants(): M[Seq[Grant]]

  def findDeletedAPIKey(tid: APIKey): M[Option[APIKeyRecord]]
  def findDeletedGrant(gid: GrantID): M[Option[Grant]]
  def findDeletedGrantChildren(gid: GrantID): M[Set[Grant]]

  def addGrants(tid: APIKey, grants: Set[GrantID]): M[Option[APIKeyRecord]]
  def removeGrants(tid: APIKey, grants: Set[GrantID]): M[Option[APIKeyRecord]]

  def deleteAPIKey(tid: APIKey): M[Option[APIKeyRecord]]
  def deleteGrant(gid: GrantID): M[Set[Grant]]

  def close(): M[Unit]
} 


case class MongoAPIKeyManagerSettings(
  apiKeys: String = "tokens",
  grants: String = "grants",
  deletedAPIKeys: String = "tokens_deleted",
  deletedGrants: String = "grants_deleted",
  timeout: Timeout = new Timeout(30000))

object MongoAPIKeyManagerSettings {
  val defaults = MongoAPIKeyManagerSettings()
}

trait MongoAPIKeyManagerComponent extends Logging {
  implicit def asyncContext: ExecutionContext
  implicit lazy val M: Monad[Future] = AkkaTypeClasses.futureApplicative(asyncContext)

  def apiKeyManagerFactory(config: Configuration): APIKeyManager[Future] = {
    val mongo = RealMongo(config.detach("mongo"))
    
    val database = config[String]("mongo.database", "auth_v1")
    val apiKeys = config[String]("mongo.tokens", "tokens")
    val grants = config[String]("mongo.grants", "grants")
    val deletedAPIKeys = config[String]("mongo.deleted_tokens", apiKeys + "_deleted")
    val deletedGrants = config[String]("mongo.deleted_grants", grants + "_deleted")
    val timeoutMillis = config[Int]("mongo.query.timeout", 10000)

    val settings = MongoAPIKeyManagerSettings(
      apiKeys, grants, deletedAPIKeys, deletedGrants, timeoutMillis
    )

    val mongoAPIKeyManager = 
      new MongoAPIKeyManager(mongo, mongo.database(database), settings)

    val cached = config[Boolean]("cached", false)

    if(cached) {
      new CachingAPIKeyManager(mongoAPIKeyManager)
    } else {
      mongoAPIKeyManager
    }
  }
}

class MongoAPIKeyManager(
    mongo: Mongo, 
    database: Database, 
    settings: MongoAPIKeyManagerSettings = MongoAPIKeyManagerSettings.defaults)(implicit val execContext: ExecutionContext) extends APIKeyManager[Future] with Logging {

  private implicit val impTimeout = settings.timeout

  def newAPIKey(name: String, creator: APIKey, grants: Set[GrantID]) = {
    val apiKey = APIKeyRecord(name, newAPIKey(), creator, grants)
    database(insert(apiKey.serialize(APIKeyRecord.apiKeyRecordDecomposer).asInstanceOf[JObject]).into(settings.apiKeys)) map {
      _ => apiKey
    }
  }

  def newGrant(issuer: Option[GrantID], perm: Permission) = {
    val ng = Grant(newGrantID, issuer, perm)
    logger.debug("Adding grant: " + ng)
    database(insert(ng.serialize(Grant.GrantDecomposer).asInstanceOf[JObject]).into(settings.grants)) map {
      _ => logger.debug("Add complete for " + ng); ng
    }
  }

  private def findOneMatching[A](keyName: String, keyValue: String, collection: String)(implicit extractor: Extractor[A]): Future[Option[A]] = {
    database {
      selectOne().from(collection).where(keyName === keyValue)
    }.map {
      _.map(_.deserialize(extractor))
    }
  }

  private def findAllMatching[A](keyName: String, keyValue: String, collection: String)(implicit extractor: Extractor[A]): Future[Set[A]] = {
    database {
      selectAll.from(collection).where(keyName === keyValue)
    }.map {
      _.map(_.deserialize(extractor)).toSet
    }
  }

  private def findAll[A](collection: String)(implicit extract: Extractor[A]): Future[Seq[A]] =
    database { selectAll.from(collection) }.map { _.map(_.deserialize(extract)).toSeq }

  def listAPIKeys() = findAll[APIKeyRecord](settings.apiKeys)
  def listGrants() = findAll[Grant](settings.grants)

  def findAPIKey(tid: APIKey) = findOneMatching[APIKeyRecord]("tid", tid, settings.apiKeys)
  def findGrant(gid: GrantID) = findOneMatching[Grant]("gid", gid, settings.grants)

  def findGrantChildren(gid: GrantID) = findAllMatching("issuer", gid, settings.grants)

  def listDeletedAPIKeys() = findAll[APIKeyRecord](settings.apiKeys)
  def listDeletedGrants() = findAll[Grant](settings.grants)

  def findDeletedAPIKey(tid: APIKey) = findOneMatching[APIKeyRecord]("tid", tid, settings.deletedAPIKeys)
  def findDeletedGrant(gid: GrantID) = findOneMatching[Grant]("gid", gid, settings.deletedGrants)

  def findDeletedGrantChildren(gid: GrantID) = findAllMatching("issuer", gid, settings.deletedGrants)

  def addGrants(tid: APIKey, add: Set[GrantID]) = updateAPIKey(tid) { t =>
    Some(t.addGrants(add))
  }

  def removeGrants(tid: APIKey, remove: Set[GrantID]) = updateAPIKey(tid) { t =>
    if(remove.subsetOf(t.grants)) Some(t.removeGrants(remove)) else None
  }

  private def updateAPIKey(tid: APIKey)(f: APIKeyRecord => Option[APIKeyRecord]): Future[Option[APIKeyRecord]] = {
    findAPIKey(tid).flatMap {
      case Some(t) =>
        f(t) match {
          case Some(nt) if nt != t =>
            database {
              val updateObj = nt.serialize(APIKeyRecord.apiKeyRecordDecomposer).asInstanceOf[JObject]
              update(settings.apiKeys).set(updateObj).where("tid" === tid)
            }.map{ _ => Some(nt) }
          case _ => Future(Some(t))
        }
      case None    => Future(None)
    }
  }

  def deleteAPIKey(tid: APIKey): Future[Option[APIKeyRecord]] =
    findAPIKey(tid).flatMap { 
      case ot @ Some(t) =>
        for {
          _ <- database(insert(t.serialize(APIKeyRecord.apiKeyRecordDecomposer).asInstanceOf[JObject]).into(settings.deletedAPIKeys))
          _ <- database(remove.from(settings.apiKeys).where("tid" === tid))
        } yield { ot }
      case None    => Future(None)
    } 

  def deleteGrant(gid: GrantID): Future[Set[Grant]] = {
    findGrantChildren(gid).flatMap { gc =>
      Future.sequence(gc.map { g => deleteGrant(g.gid)}).map { _.flatten }.flatMap { gds =>
        findGrant(gid).flatMap {
          case og @ Some(g) =>
            for {
              _ <- database(insert(g.serialize(Grant.GrantDecomposer).asInstanceOf[JObject]).into(settings.deletedGrants))
              _ <- database(remove.from(settings.grants).where("gid" === gid))
            } yield { gds + g }
          case None    => Future(gds)
        }
      }
    }
  }
  
  def close() = database.disconnect.fallbackTo(Future(())).flatMap{_ => mongo.close}
}

case class CachingAPIKeyManagerSettings(
  apiKeyCacheSettings: CacheSettings[APIKey, APIKeyRecord],
  grantCacheSettings: CacheSettings[GrantID, Grant])

class CachingAPIKeyManager[M[+_]: Monad](manager: APIKeyManager[M], settings: CachingAPIKeyManagerSettings = CachingAPIKeyManager.defaultSettings) extends APIKeyManager[M] {

  private val apiKeyCache = Cache.concurrent[APIKey, APIKeyRecord](settings.apiKeyCacheSettings)
  private val grantCache = Cache.concurrent[GrantID, Grant](settings.grantCacheSettings)

  def newAPIKey(name: String, creator: APIKey, grants: Set[GrantID]) =
    manager.newAPIKey(name, creator, grants).map { _ ->- add }

  def newGrant(issuer: Option[GrantID], permission: Permission) =
    manager.newGrant(issuer, permission).map { _ ->- add }

  def listAPIKeys() = manager.listAPIKeys
  def listGrants() = manager.listGrants

  def listDeletedAPIKeys() = manager.listDeletedAPIKeys
  def listDeletedGrants() = manager.listDeletedGrants

  def findAPIKey(tid: APIKey) = apiKeyCache.get(tid) match {
    case None => manager.findAPIKey(tid).map { _.map { _ ->- add } }
    case t    => Monad[M].point(t)
  }
  def findGrant(gid: GrantID) = grantCache.get(gid) match {
    case None        => manager.findGrant(gid).map { _.map { _ ->- add } }
    case s @ Some(_) => Monad[M].point(s)
  }
  def findGrantChildren(gid: GrantID) = manager.findGrantChildren(gid)

  def findDeletedAPIKey(tid: APIKey) = manager.findDeletedAPIKey(tid)
  def findDeletedGrant(gid: GrantID) = manager.findDeletedGrant(gid)
  def findDeletedGrantChildren(gid: GrantID) = manager.findDeletedGrantChildren(gid)

  def addGrants(tid: APIKey, grants: Set[GrantID]) =
    manager.addGrants(tid, grants).map { _.map { _ ->- add } }
  def removeGrants(tid: APIKey, grants: Set[GrantID]) =
    manager.removeGrants(tid, grants).map { _.map { _ ->- add } }

  def deleteAPIKey(tid: APIKey) =
    manager.deleteAPIKey(tid) map { _.map { _ ->- remove } }
  def deleteGrant(gid: GrantID) =
    manager.deleteGrant(gid) map { _.map { _ ->- remove } }

  private def add(t: APIKeyRecord) = apiKeyCache.put(t.tid, t)
  private def add(g: Grant) = grantCache.put(g.gid, g)

  private def remove(t: APIKeyRecord) = apiKeyCache.remove(t.tid)
  private def remove(g: Grant) = grantCache.remove(g.gid)

  def close() = manager.close

}

object CachingAPIKeyManager {
  val defaultSettings = CachingAPIKeyManagerSettings(
    CacheSettings[APIKey, APIKeyRecord](ExpirationPolicy(Some(5), Some(5), MINUTES)),
    CacheSettings[GrantID, Grant](ExpirationPolicy(Some(5), Some(5), MINUTES))
  )
}



