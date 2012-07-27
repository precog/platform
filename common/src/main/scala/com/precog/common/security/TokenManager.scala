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
import blueeyes.json.xschema.{ ValidatedExtraction, Extractor, Decomposer }
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.Extractor._

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

trait TokenManager[M[+_]] {
  private[security] def newUUID() = java.util.UUID.randomUUID.toString

  // 256 bit token ID
  //private[security] def newTokenID(): String = (newUUID() + "-" + newUUID()).toUpperCase

  // 128 bit token ID
  private[security] def newTokenID(): String = newUUID().toUpperCase

  // 384 bit grant ID
  private[security] def newGrantID(): String = (newUUID() + newUUID() + newUUID()).toLowerCase.replace("-","")
 
  def newToken(name: String, grants: Set[GrantID]): M[Token]
  def newGrant(issuer: Option[GrantID], permission: Permission): M[Grant]

  def listTokens(): M[Seq[Token]]
  def listGrants(): M[Seq[Grant]]
  
  def findToken(tid: TokenID): M[Option[Token]]
  def findGrant(gid: GrantID): M[Option[Grant]]
  def findGrantChildren(gid: GrantID): M[Set[Grant]]
 

  def listDeletedTokens(): M[Seq[Token]]
  def listDeletedGrants(): M[Seq[Grant]]

  def findDeletedToken(tid: TokenID): M[Option[Token]]
  def findDeletedGrant(gid: GrantID): M[Option[Grant]]
  def findDeletedGrantChildren(gid: GrantID): M[Set[Grant]]

  def addGrants(tid: TokenID, grants: Set[GrantID]): M[Option[Token]]
  def removeGrants(tid: TokenID, grants: Set[GrantID]): M[Option[Token]]

  def deleteToken(tid: TokenID): M[Option[Token]]
  def deleteGrant(gid: GrantID): M[Set[Grant]]

  def close(): M[Unit]
} 


case class MongoTokenManagerSettings(
  tokens: String = "tokens",
  grants: String = "grants",
  deletedTokens: String = "tokens_deleted",
  deletedGrants: String = "grants_deleted",
  timeout: Timeout = new Timeout(30000))

object MongoTokenManagerSettings {
  val defaults = MongoTokenManagerSettings()
}

trait MongoTokenManagerComponent extends Logging {
  implicit def asyncContext: ExecutionContext
  implicit lazy val M: Monad[Future] = AkkaTypeClasses.futureApplicative(asyncContext)

  def tokenManagerFactory(config: Configuration): TokenManager[Future] = {
    val mongo = RealMongo(config.detach("mongo"))
    
    val database = config[String]("mongo.database", "auth_v1")
    val tokens = config[String]("mongo.tokens", "tokens")
    val grants = config[String]("mongo.grants", "grants")
    val deletedTokens = config[String]("mongo.deleted_tokens", tokens + "_deleted")
    val deletedGrants = config[String]("mongo.deleted_grants", grants + "_deleted")
    val timeoutMillis = config[Int]("mongo.query.timeout", 10000)

    val settings = MongoTokenManagerSettings(
      tokens, grants, deletedTokens, deletedGrants, timeoutMillis
    )

    val mongoTokenManager = 
      new MongoTokenManager(mongo, mongo.database(database), settings)

    val cached = config[Boolean]("cached", false)

    if(cached) {
      new CachingTokenManager(mongoTokenManager)
    } else {
      mongoTokenManager
    }
  }
}

class MongoTokenManager(
    mongo: Mongo,
    database: Database,
    settings: MongoTokenManagerSettings = MongoTokenManagerSettings.defaults)(implicit val execContext: ExecutionContext) extends TokenManager[Future] with Logging {

  private implicit val impTimeout = settings.timeout

  def newToken(name: String, grants: Set[GrantID]) = {
    val newToken = Token(newTokenID(), name, grants)
    database(insert(newToken.serialize(Token.UnsafeTokenDecomposer).asInstanceOf[JObject]).into(settings.tokens)) map {
      _ => newToken
    }
  }

  def newGrant(issuer: Option[GrantID], perm: Permission) = {
    val ng = Grant(newGrantID, issuer, perm)
    logger.debug("Adding grant: " + ng)
    database(insert(ng.serialize(Grant.UnsafeGrantDecomposer).asInstanceOf[JObject]).into(settings.grants)) map { 
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

  def listTokens() = findAll[Token](settings.tokens)
  def listGrants() = findAll[Grant](settings.grants)

  def findToken(tid: TokenID) = findOneMatching[Token]("tid", tid, settings.tokens)
  def findGrant(gid: GrantID) = findOneMatching[Grant]("gid", gid, settings.grants)

  def findGrantChildren(gid: GrantID) = findAllMatching("issuer", gid, settings.grants)

  def listDeletedTokens() = findAll[Token](settings.tokens)
  def listDeletedGrants() = findAll[Grant](settings.grants)

  def findDeletedToken(tid: TokenID) = findOneMatching[Token]("tid", tid, settings.deletedTokens)
  def findDeletedGrant(gid: GrantID) = findOneMatching[Grant]("gid", gid, settings.deletedGrants)

  def findDeletedGrantChildren(gid: GrantID) = findAllMatching("issuer", gid, settings.deletedGrants)

  def addGrants(tid: TokenID, add: Set[GrantID]) = updateToken(tid) { t =>
    Some(t.addGrants(add))
  }

  def removeGrants(tid: TokenID, remove: Set[GrantID]) = updateToken(tid) { t =>
    Some(t.removeGrants(remove))
  }

  private def updateToken(tid: TokenID)(f: Token => Option[Token]): Future[Option[Token]] = {
    findToken(tid).flatMap {
      case Some(t) =>
        f(t) match {
          case Some(nt) if nt != t =>
            database {
              val updateObj = nt.serialize(Token.UnsafeTokenDecomposer).asInstanceOf[JObject]
              update(settings.tokens).set(updateObj).where("tid" === tid)
            }.map{ _ => Some(nt) }
          case _ => Future(Some(t))
        }
      case None    => Future(None)
    }
  }

  def deleteToken(tid: TokenID): Future[Option[Token]] =
    findToken(tid).flatMap { 
      case ot @ Some(t) =>
        for {
          _ <- database(insert(t.serialize(Token.UnsafeTokenDecomposer).asInstanceOf[JObject]).into(settings.deletedTokens))
          _ <- database(remove.from(settings.tokens).where("tid" === tid))
        } yield { ot }
      case None    => Future(None)
    } 

  def deleteGrant(gid: GrantID): Future[Set[Grant]] = {
    findGrantChildren(gid).flatMap { gc =>
      Future.sequence(gc.map { g => deleteGrant(g.gid)}).map { _.flatten }.flatMap { gds =>
        findGrant(gid).flatMap {
          case og @ Some(g) =>
            for {
              _ <- database(insert(g.serialize(Grant.UnsafeGrantDecomposer).asInstanceOf[JObject]).into(settings.deletedGrants))
              _ <- database(remove.from(settings.grants).where("gid" === gid))
            } yield { gds + g }
          case None    => Future(gds)
        }
      }
    }
  }

  def close() = database.disconnect.fallbackTo(Future(())).flatMap{_ => mongo.close}
}

case class CachingTokenManagerSettings(
  tokenCacheSettings: CacheSettings[TokenID, Token],
  grantCacheSettings: CacheSettings[GrantID, Grant])

class CachingTokenManager[M[+_]: Monad](manager: TokenManager[M], settings: CachingTokenManagerSettings = CachingTokenManager.defaultSettings) extends TokenManager[M] {

  private val tokenCache = Cache.concurrent[TokenID, Token](settings.tokenCacheSettings)
  private val grantCache = Cache.concurrent[GrantID, Grant](settings.grantCacheSettings)

  def newToken(name: String, grants: Set[GrantID]) =
    manager.newToken(name, grants).map { _ ->- add }
  def newGrant(issuer: Option[GrantID], permission: Permission) =
    manager.newGrant(issuer, permission).map { _ ->- add }

  def listTokens() = manager.listTokens
  def listGrants() = manager.listGrants

  def listDeletedTokens() = manager.listDeletedTokens
  def listDeletedGrants() = manager.listDeletedGrants

  def findToken(tid: TokenID) = tokenCache.get(tid) match {
    case None => manager.findToken(tid).map { _.map { _ ->- add } }
    case t    => Monad[M].point(t)
  }
  def findGrant(gid: GrantID) = grantCache.get(gid) match {
    case None        => manager.findGrant(gid).map { _.map { _ ->- add } }
    case s @ Some(_) => Monad[M].point(s)
  }
  def findGrantChildren(gid: GrantID) = manager.findGrantChildren(gid)

  def findDeletedToken(tid: TokenID) = manager.findDeletedToken(tid)
  def findDeletedGrant(gid: GrantID) = manager.findDeletedGrant(gid)
  def findDeletedGrantChildren(gid: GrantID) = manager.findDeletedGrantChildren(gid)

  def addGrants(tid: TokenID, grants: Set[GrantID]) =
    manager.addGrants(tid, grants).map { _.map { _ ->- add } }
  def removeGrants(tid: TokenID, grants: Set[GrantID]) =
    manager.removeGrants(tid, grants).map { _.map { _ ->- add } }

  def deleteToken(tid: TokenID) =
    manager.deleteToken(tid) map { _.map { _ ->- remove } }
  def deleteGrant(gid: GrantID) =
    manager.deleteGrant(gid) map { _.map { _ ->- remove } }

  private def add(t: Token) = tokenCache.put(t.tid, t)
  private def add(g: Grant) = grantCache.put(g.gid, g)

  private def remove(t: Token) = tokenCache.remove(t.tid)
  private def remove(g: Grant) = grantCache.remove(g.gid)

  def close() = manager.close

}

object CachingTokenManager {
  val defaultSettings = CachingTokenManagerSettings(
    CacheSettings[TokenID, Token](ExpirationPolicy(Some(5), Some(5), MINUTES)),
    CacheSettings[GrantID, Grant](ExpirationPolicy(Some(5), Some(5), MINUTES))
  )
}



