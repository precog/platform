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
import org.joda.time.Instant

import akka.util.Timeout
import akka.dispatch.{ ExecutionContext, Future, Promise }

import blueeyes.bkka._
import blueeyes.json._
import blueeyes.persistence.mongo._
import blueeyes.persistence.mongo.dsl._
import blueeyes.json.serialization.Extractor
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.util.Clock

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

object MongoAPIKeyManager extends Logging {
  def apply(config: Configuration)(implicit executor: ExecutionContext): (APIKeyManager[Future], Stoppable) = {
    // TODO: should only require either the executor or M, not both.
    implicit val M: Monad[Future] = new FutureMonad(executor)

    val dbName = config[String]("mongo.database", "auth_v1")
    val apiKeys = config[String]("mongo.tokens", "tokens")
    val grants = config[String]("mongo.grants", "grants")
    val deletedAPIKeys = config[String]("mongo.deleted_tokens", apiKeys + "_deleted")
    val deletedGrants = config[String]("mongo.deleted_grants", grants + "_deleted")
    val timeoutMillis = config[Int]("mongo.query.timeout", 10000)
    val rootKeyId = config[String]("rootKey")

    val settings = MongoAPIKeyManagerSettings(
      apiKeys, grants, deletedAPIKeys, deletedGrants, timeoutMillis, rootKeyId
    )

    val mongo = RealMongo(config.detach("mongo"))
    val database = mongo.database(dbName)
    val mongoAPIKeyManager = new MongoAPIKeyManager(mongo, database, settings)

    val cached = config[Boolean]("cached", false)

    val dbStop = Stoppable.fromFuture(database.disconnect.fallbackTo(Future(())) flatMap { _ => mongo.close })

    (if (cached) new CachingAPIKeyManager(mongoAPIKeyManager) else mongoAPIKeyManager, dbStop)
  }

  def createRootAPIKey(db: Database, keyCollection: String, grantCollection: String)(implicit timeout: Timeout): Future[APIKeyRecord] = {
    import Permission._
    logger.info("Creating new root key")
    // Set up a new root API Key
    val rootAPIKeyId = APIKeyManager.newAPIKey()
    val rootGrantId = APIKeyManager.newGrantId()
    val rootPermissions = Set[Permission](
      WritePermission(Path.Root, WriteAsAny),
      ReadPermission(Path.Root, WrittenByAny),
      DeletePermission(Path.Root, WrittenByAny)
    )

    val rootGrant = Grant(
      rootGrantId, Some("root-grant"), Some("The root grant"), rootAPIKeyId, Set(),
      rootPermissions,
      new Instant(0L),
      None
    )

    val rootAPIKeyRecord = APIKeyRecord(rootAPIKeyId, Some("root-apiKey"), Some("The root API key"), rootAPIKeyId, Set(rootGrantId), true)

    for {
      _ <- db(insert(rootGrant.serialize.asInstanceOf[JObject]).into(grantCollection))
      _ <- db(insert(rootAPIKeyRecord.serialize.asInstanceOf[JObject]).into(keyCollection))
    } yield rootAPIKeyRecord
  }

  def findRootAPIKey(db: Database, keyCollection: String)(implicit context: ExecutionContext, timeout: Timeout): Future[APIKeyRecord] = {
    db(selectOne().from(keyCollection).where("isRoot" === true)) flatMap {
      case Some(keyJv) =>
        logger.info("Retrieved existing root key")
        Promise.successful(keyJv.deserialize[APIKeyRecord])

      case None =>
        logger.error("Could not locate existing root API key!")
        Promise.failed(new IllegalStateException("Could not locate existing root API key!"))
    }
  }
}

class MongoAPIKeyManager(mongo: Mongo, database: Database, settings: MongoAPIKeyManagerSettings = MongoAPIKeyManagerSettings.defaults, clock: Clock = Clock.System)(implicit val executor: ExecutionContext) extends APIKeyManager[Future] with Logging {
  implicit val M = new FutureMonad(executor)

  private implicit val impTimeout = settings.timeout

  // Ensure proper mongo indices
  database(ensureIndex("apiKey_index").on(".apiKey").in(settings.apiKeys))
  database(ensureIndex("grantId_index").on(".grantId").in(settings.grants))

  val rootAPIKeyRecord : Future[APIKeyRecord] = findAPIKey(settings.rootKeyId).map {
    _.getOrElse { throw new Exception("Could not locate root api key as specified in the configuration") }
  }

  def rootAPIKey: Future[APIKey] = rootAPIKeyRecord.map(_.apiKey)
  def rootGrantId: Future[GrantId] = rootAPIKeyRecord.map(_.grants.head)

  def createAPIKey(name: Option[String], description: Option[String], issuerKey: APIKey, grants: Set[GrantId]): Future[APIKeyRecord] = {
    val apiKey = APIKeyRecord(APIKeyManager.newAPIKey(), name, description, issuerKey, grants, false)
    database(insert(apiKey.serialize.asInstanceOf[JObject]).into(settings.apiKeys)) map {
      _ => apiKey
    }
  }

  def createGrant(name: Option[String], description: Option[String], issuerKey: APIKey, parentIds: Set[GrantId], perms: Set[Permission], expiration: Option[DateTime]): Future[Grant] = {
    val ng = Grant(APIKeyManager.newGrantId(), name, description, issuerKey, parentIds, perms, clock.instant(), expiration)
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

  def findAPIKeyChildren(apiKey: APIKey): Future[Set[APIKeyRecord]] =
    findAllMatching[APIKeyRecord]("issuerKey", apiKey, settings.apiKeys)

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
    for {
      children <- findGrantChildren(gid)
      deletedChildren <- Future.sequence(children map { g => deleteGrant(g.grantId) }) map { _.flatten }
      leafOpt <- findGrant(gid)
      result <- leafOpt map { leafGrant =>
                  for {
                    _ <- database(insert(leafGrant.serialize.asInstanceOf[JObject]).into(settings.deletedGrants))
                    _ <- database(remove.from(settings.grants).where("grantId" === gid))
                  } yield { deletedChildren + leafGrant }
                } getOrElse {
                  Promise successful deletedChildren
                }
    } yield result
  }
}
