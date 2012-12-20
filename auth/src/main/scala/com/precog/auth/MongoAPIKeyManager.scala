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
package com.precog.auth 

import com.precog.common._
import com.precog.common.accounts._
import com.precog.common.security._

import org.joda.time.DateTime

import akka.util.Timeout
import akka.dispatch.{ ExecutionContext, Future, Promise }

import blueeyes.bkka._
import blueeyes.json._
import blueeyes.persistence.mongo._
import blueeyes.json.serialization.Extractor
import blueeyes.json.serialization.DefaultSerialization._

import com.weiglewilczek.slf4s.Logging

import org.streum.configrity.Configuration

import scalaz._
import scalaz.PlusEmpty._
import scalaz.std.option._
import scalaz.std.set._
import scalaz.syntax.plus._

case class MongoAPIKeyManagerSettings(
  apiKeys: String = "tokens",
  grants: String = "grants",
  deletedAPIKeys: String = "tokens_deleted",
  deletedGrants: String = "grants_deleted",
  timeout: Timeout = new Timeout(30000),
  rootKeyId: String = "invalid"
)

object MongoAPIKeyManagerSettings {
  val defaults = MongoAPIKeyManagerSettings()
}

trait MongoAPIKeyManagerComponent extends Logging {
  implicit def asyncContext: ExecutionContext
  implicit val M: Monad[Future]

  def apiKeyManagerFactory(config: Configuration): APIKeyManager[Future] = {
    val mongo = RealMongo(config.detach("mongo"))
    
    val database = config[String]("mongo.database", "auth_v1")
    val apiKeys = config[String]("mongo.tokens", "tokens")
    val grants = config[String]("mongo.grants", "grants")
    val deletedAPIKeys = config[String]("mongo.deleted_tokens", apiKeys + "_deleted")
    val deletedGrants = config[String]("mongo.deleted_grants", grants + "_deleted")
    val timeoutMillis = config[Int]("mongo.query.timeout", 10000)
    val rootKeyId = config[String]("rootKey")

    val settings = MongoAPIKeyManagerSettings(
      apiKeys, grants, deletedAPIKeys, deletedGrants, timeoutMillis, rootKeyId
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

object MongoAPIKeyManager extends Logging {
  def findRootAPIKey(db: Database, keyCollection: String, grantCollection: String, createIfMissing: Boolean)(implicit context: ExecutionContext, timeout: Timeout): Future[APIKeyRecord] = {
    import Grant.Serialization._
    import APIKeyRecord.Serialization._

    db(selectOne().from(keyCollection).where("isRoot" === true)).flatMap {
      case Some(keyJv) => 
        logger.info("Retrieved existing root key")
        Promise.successful(keyJv.deserialize[APIKeyRecord])
      case None if createIfMissing => {
        logger.info("Creating new root key")
        // Set up a new root API Key
        val rootGrantId = APIKeyManager.newGrantId()
        val rootGrant = {
          def mkPerm(p: (Path, Set[AccountId]) => Permission) = p(Path("/"), Set())
        
          Grant(
            rootGrantId, Some("root-grant"), Some("The root grant"), None, Set(),
            Set(mkPerm(ReadPermission), mkPerm(ReducePermission), mkPerm(WritePermission), mkPerm(DeletePermission)),
            None
          )
        }
      
        val rootAPIKeyId = APIKeyManager.newAPIKey()
        val rootAPIKeyRecord =
          APIKeyRecord(rootAPIKeyId, Some("root-apiKey"), Some("The root API key"), None, Set(rootGrantId), true)

        db(insert(rootGrant.serialize.asInstanceOf[JObject]).into(grantCollection)) flatMap {
          _ => db(insert(rootAPIKeyRecord.serialize.asInstanceOf[JObject]).into(keyCollection)).map { _ => rootAPIKeyRecord }
        }
      }
      case _ => 
        logger.error("Could not locate existing root API key!")
        throw new Exception("Could not locate existing root API key!")
    }
  }
}

class MongoAPIKeyManager(mongo: Mongo, database: Database, settings: MongoAPIKeyManagerSettings = MongoAPIKeyManagerSettings.defaults)(implicit val executor: ExecutionContext) extends APIKeyManager[Future] with Logging {
  implicit val M = new FutureMonad(executor)

  private implicit val impTimeout = settings.timeout

  import Grant.Serialization._
  import APIKeyRecord.Serialization._

  val rootAPIKeyRecord : Future[APIKeyRecord] = findAPIKey(settings.rootKeyId).map { 
    _.getOrElse { throw new Exception("Could not locate root api key as specified in the configuration") }
  }
  
  def rootAPIKey: Future[APIKey] = rootAPIKeyRecord.map(_.apiKey)
  def rootGrantId: Future[GrantId] = rootAPIKeyRecord.map(_.grants.head) 

  def newAPIKey(name: Option[String], description: Option[String], issuerKey: APIKey, grants: Set[GrantId]): Future[APIKeyRecord] = {
    val apiKey = APIKeyRecord(APIKeyManager.newAPIKey(), name, description, some(issuerKey), grants, false)
    database(insert(apiKey.serialize.asInstanceOf[JObject]).into(settings.apiKeys)) map {
      _ => apiKey
    }
  }

  def newGrant(name: Option[String], description: Option[String], issuerKey: APIKey, parentIds: Set[GrantId], perms: Set[Permission], expiration: Option[DateTime]): Future[Grant] = {
    val ng = Grant(APIKeyManager.newGrantId(), name, description, some(issuerKey), parentIds, perms, expiration)
    logger.debug("Adding grant: " + ng)
    database(insert(ng.serialize.asInstanceOf[JObject]).into(settings.grants)) map {
      _ => logger.debug("Add complete for " + ng); ng
    }
  }

  private def findOneMatching[A](keyName: String, keyValue: MongoPrimitive, collection: String)(implicit extractor: Extractor[A]): Future[Option[A]] = {
    database {
      selectOne().from(collection).where(keyName === keyValue)
    } map {
      _.map(_.deserialize[A])
    }
  }

  private def findAllMatching[A](keyName: String, keyValue: MongoPrimitive, collection: String)(implicit extractor: Extractor[A]): Future[Set[A]] = {
    database {
      selectAll.from(collection).where(keyName === keyValue )
    } map {
      _.map(_.deserialize[A]).toSet
    }
  }

  private def findAllIncluding[A](keyName: String, keyValue: MongoPrimitive, collection: String)(implicit extractor: Extractor[A]): Future[Set[A]] = {
    database {
      selectAll.from(collection).where(stringToMongoFilterBuilder(keyName) contains keyValue)
    } map {
      _.map(_.deserialize[A]).toSet
    }
  }

  private def findAll[A](collection: String)(implicit extract: Extractor[A]): Future[Seq[A]] =
    database { selectAll.from(collection) } map { _.map(_.deserialize[A]).toSeq }

  def listAPIKeys() = findAll[APIKeyRecord](settings.apiKeys)
  def listGrants() = findAll[Grant](settings.grants)

  def findAPIKey(apiKey: APIKey) = 
    ToPlusOps[({ type λ[α] = Future[Option[α]] })#λ, APIKeyRecord](findOneMatching[APIKeyRecord]("apiKey", apiKey, settings.apiKeys)) <+>
    findOneMatching[APIKeyRecord]("tid", apiKey, settings.apiKeys)

  def findGrant(gid: GrantId) = 
    ToPlusOps[({ type λ[α] = Future[Option[α]] })#λ, Grant](findOneMatching[Grant]("grantId", gid, settings.grants)) <+>
    findOneMatching[Grant]("gid", gid, settings.grants)

  def findGrantChildren(gid: GrantId) = findGrantChildren(gid, settings.grants)

  def listDeletedAPIKeys() = findAll[APIKeyRecord](settings.apiKeys)
  def listDeletedGrants() = findAll[Grant](settings.grants)

  def findDeletedAPIKey(apiKey: APIKey) = 
    ToPlusOps[({ type λ[α] = Future[Option[α]] })#λ, APIKeyRecord](findOneMatching[APIKeyRecord]("apiKey", apiKey, settings.deletedAPIKeys)) <+>
    findOneMatching[APIKeyRecord]("tid", apiKey, settings.deletedAPIKeys)

  def findDeletedGrant(gid: GrantId) = 
    ToPlusOps[({ type λ[α] = Future[Option[α]] })#λ, Grant](findOneMatching[Grant]("grantId", gid, settings.deletedGrants)) <+>
    findOneMatching[Grant]("gid", gid, settings.deletedGrants)

  def findDeletedGrantChildren(gid: GrantId) = findGrantChildren(gid, settings.deletedGrants)
  
  // This has to account for structural changes between v0 and v1 grant documents
  private def findGrantChildren(gid: GrantId, collection: String) = 
    ToPlusOps[({ type λ[α] = Future[Set[α]] })#λ, Grant](findAllIncluding[Grant]("parentIds", gid, collection)) <+>
    findAllMatching[Grant]("issuer", gid, collection)

  def addGrants(apiKey: APIKey, add: Set[GrantId]) = updateAPIKey(apiKey) { r =>
    Some(r.copy(grants = r.grants ++ add))
  }

  def removeGrants(apiKey: APIKey, remove: Set[GrantId]) = updateAPIKey(apiKey) { r =>
    if(remove.subsetOf(r.grants)) Some(r.copy(grants = r.grants -- remove)) else None
  }

  private def updateAPIKey(apiKey: APIKey)(f: APIKeyRecord => Option[APIKeyRecord]): Future[Option[APIKeyRecord]] = {
    findAPIKey(apiKey).flatMap {
      case Some(t) =>
        f(t) match {
          case Some(nt) if nt != t =>
            database {
              val updateObj = nt.serialize.asInstanceOf[JObject]
              update(settings.apiKeys).set(updateObj).where("apiKey" === apiKey)
            }.map{ _ => Some(nt) }
          case _ => Future(Some(t))
        }
      case None    => Future(None)
    }
  }

  def deleteAPIKey(apiKey: APIKey): Future[Option[APIKeyRecord]] =
    findAPIKey(apiKey).flatMap { 
      case ot @ Some(t) =>
        for {
          _ <- database(insert(t.serialize.asInstanceOf[JObject]).into(settings.deletedAPIKeys))
          _ <- database(remove.from(settings.apiKeys).where("apiKey" === apiKey))
        } yield { ot }
      case None    => Future(None)
    } 

  def deleteGrant(gid: GrantId): Future[Set[Grant]] = {
    findGrantChildren(gid).flatMap { gc =>
      Future.sequence(gc.map { g => deleteGrant(g.grantId)}).map { _.flatten }.flatMap { gds =>
        findGrant(gid).flatMap {
          case og @ Some(g) =>
            for {
              _ <- database(insert(g.serialize.asInstanceOf[JObject]).into(settings.deletedGrants))
              _ <- database(remove.from(settings.grants).where("grantId" === gid))
            } yield { gds + g }
          case None    => Future(gds)
        }
      }
    }
  }
  
  def close() = database.disconnect.fallbackTo(Future(())).flatMap{_ => mongo.close}
}
