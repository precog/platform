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

import org.joda.time.DateTime

import akka.util.Timeout
import akka.dispatch.{ ExecutionContext, Future }

import blueeyes.bkka._
import blueeyes.json._
import blueeyes.persistence.mongo._
import blueeyes.json.serialization.Extractor
import blueeyes.json.serialization.DefaultSerialization._

import com.weiglewilczek.slf4s.Logging

import org.streum.configrity.Configuration

import scalaz._
import scalaz.std.option._

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

class MongoAPIKeyManager(mongo: Mongo, database: Database, settings: MongoAPIKeyManagerSettings = MongoAPIKeyManagerSettings.defaults)
  (implicit val execContext: ExecutionContext) extends APIKeyManager[Future] with Logging {
  
  implicit val M = AkkaTypeClasses.futureApplicative(execContext)

  private implicit val impTimeout = settings.timeout
  
  import Grant.Serialization._
  import APIKeyRecord.Serialization._

  val rootAPIKeyRecord : Future[APIKeyRecord] =
    findOneMatching[APIKeyRecord]("isRoot", "true", settings.apiKeys).map(_.getOrElse {
      val rootGrantId = newGrantID()
      val rootGrant = {
        def mkPerm(p: (Path, Set[AccountID]) => Permission) = p(Path("/"), Set())
        
        Grant(
          rootGrantId, some("root-grant"), some("The root grant"), None, Set(),
          Set(mkPerm(ReadPermission), mkPerm(ReducePermission), mkPerm(WritePermission), mkPerm(DeletePermission)),
          None
        )
      }
      database(insert(rootGrant.serialize.asInstanceOf[JObject]).into(settings.grants))
      
      val rootAPIKey = newAPIKey()
      val rootAPIKeyRecord = 
        APIKeyRecord(rootAPIKey, some("root-apiKey"), some("The root API key"), None, Set(rootGrantId), true)
        
      database(insert(rootAPIKeyRecord.serialize.asInstanceOf[JObject]).into(settings.apiKeys))
      rootAPIKeyRecord
    })
  
  def rootAPIKey: Future[APIKey] = rootAPIKeyRecord.map(_.apiKey)
  def rootGrantId: Future[GrantID] = rootAPIKeyRecord.map(_.grants.head) 

  def newAPIKey(name: Option[String], description: Option[String], issuerKey: APIKey, grants: Set[GrantID]): Future[APIKeyRecord] = {
    val apiKey = APIKeyRecord(newAPIKey(), name, description, some(issuerKey), grants, false)
    database(insert(apiKey.serialize.asInstanceOf[JObject]).into(settings.apiKeys)) map {
      _ => apiKey
    }
  }

  def newGrant(name: Option[String], description: Option[String], issuerKey: APIKey, parentIds: Set[GrantID], perms: Set[Permission], expiration: Option[DateTime]): Future[Grant] = {
    val ng = Grant(newGrantID(), name, description, some(issuerKey), parentIds, perms, expiration)
    logger.debug("Adding grant: " + ng)
    database(insert(ng.serialize.asInstanceOf[JObject]).into(settings.grants)) map {
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

  private def findAllIncluding[A](keyName: String, keyValue: String, collection: String)(implicit extractor: Extractor[A]): Future[Set[A]] = {
    database {
      val keyFilter = stringToMongoFilterBuilder(keyName)
      val valueFilter = stringToMongoPrimitive(keyValue)
      selectAll.from(collection).where(keyFilter contains valueFilter)
    }.map {
      _.map(_.deserialize(extractor)).toSet
    }
  }

  private def findAll[A](collection: String)(implicit extract: Extractor[A]): Future[Seq[A]] =
    database { selectAll.from(collection) }.map { _.map(_.deserialize(extract)).toSeq }

  def listAPIKeys() = findAll[APIKeyRecord](settings.apiKeys)
  def listGrants() = findAll[Grant](settings.grants)

  def findAPIKey(apiKey: APIKey) = findOneMatching[APIKeyRecord]("apiKey", apiKey, settings.apiKeys)
  def findGrant(gid: GrantID) = findOneMatching[Grant]("grantId", gid, settings.grants)
  def findGrantChildren(gid: GrantID) = findAllIncluding("parentIds", gid, settings.grants)

  def listDeletedAPIKeys() = findAll[APIKeyRecord](settings.apiKeys)
  def listDeletedGrants() = findAll[Grant](settings.grants)

  def findDeletedAPIKey(apiKey: APIKey) = findOneMatching[APIKeyRecord]("apiKey", apiKey, settings.deletedAPIKeys)
  def findDeletedGrant(gid: GrantID) = findOneMatching[Grant]("grantId", gid, settings.deletedGrants)

  def findDeletedGrantChildren(gid: GrantID) = findAllIncluding("parentIds", gid, settings.deletedGrants)

  def addGrants(apiKey: APIKey, add: Set[GrantID]) = updateAPIKey(apiKey) { r =>
    Some(r.copy(grants = r.grants ++ add))
  }

  def removeGrants(apiKey: APIKey, remove: Set[GrantID]) = updateAPIKey(apiKey) { r =>
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

  def deleteGrant(gid: GrantID): Future[Set[Grant]] = {
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
