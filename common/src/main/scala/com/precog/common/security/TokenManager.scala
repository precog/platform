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
import blueeyes.bkka.AkkaDefaults
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
import Scalaz._

trait TokenManagerComponent {
  def tokenManager: TokenManager
}

trait MongoTokenManagerComponent extends Logging {

  def defaultActorSystem: ActorSystem
  implicit def execContext = ExecutionContext.defaultExecutionContext(defaultActorSystem)

  def tokenManagerFactory(config: Configuration): TokenManager = {
    val test = config[Boolean]("test", false)   
    if(test) {
      println("Test token manager instantiated")
      TestTokenManager.testTokenManager
    } else {
      val mock = config[Boolean]("mongo.mock", false)
      
      val mongo = if(mock) new MockMongo else RealMongo(config.detach("mongo"))
      
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
        new MongoTokenManager(mongo, mongo.database(database), settings)(execContext)

      val cached = config[Boolean]("cached", false)

      if(cached) {
        new CachingTokenManager(mongoTokenManager)
      } else {
        mongoTokenManager
      }
    }
  }
}

trait TokenManager {

  private[security] def newUUID() = java.util.UUID.randomUUID.toString

  private[security] def newTokenID(): String = (newUUID() + "-" + newUUID()).toUpperCase
  private[security] def newGrantID(): String = (newUUID() + newUUID() + newUUID()).toLowerCase.replace("-","")
 
  def newToken(name: String, grants: Set[GrantID]): Future[Token]
  def newGrant(grant: Grant): Future[ResolvedGrant]

  def listTokens(): Future[Seq[Token]]
  def listGrants(): Future[Seq[ResolvedGrant]]
  
  def findToken(tid: TokenID): Future[Option[Token]]
  def findGrant(gid: GrantID): Future[Option[ResolvedGrant]]
  def findGrantChildren(gid: GrantID): Future[Set[ResolvedGrant]]
  
  def listDeletedTokens(): Future[Seq[Token]]
  def listDeletedGrants(): Future[Seq[ResolvedGrant]]

  def findDeletedToken(tid: TokenID): Future[Option[Token]]
  def findDeletedGrant(gid: GrantID): Future[Option[ResolvedGrant]]
  def findDeletedGrantChildren(gid: GrantID): Future[Set[ResolvedGrant]]

  def addGrants(tid: TokenID, grants: Set[GrantID]): Future[Option[Token]]
  def removeGrants(tid: TokenID, grants: Set[GrantID]): Future[Option[Token]]

  def deleteToken(tid: TokenID): Future[Option[Token]]
  def deleteGrant(gid: GrantID): Future[Set[ResolvedGrant]]

  def close(): Future[Unit]
} 

class InMemoryTokenManager(tokens: mutable.Map[TokenID, Token] = mutable.Map.empty, 
                           grants: mutable.Map[GrantID, Grant] = mutable.Map.empty)(implicit val execContext: ExecutionContext) extends TokenManager {

  private val deletedTokens = mutable.Map.empty[TokenID, Token]
  private val deletedGrants = mutable.Map.empty[GrantID, Grant]

  def newToken(name: String, grants: Set[GrantID]) = Future {
    val newToken = Token(newTokenID, name, grants)
    tokens.put(newToken.tid, newToken)
    newToken
  }

  def newGrant(grant: Grant) = Future {
    val newGrant = ResolvedGrant(newGrantID, grant)
    grants.put(newGrant.gid, grant)
    newGrant
  }

  def listTokens() = Future[Seq[Token]] { tokens.values.toList }
  def listGrants() = Future[Seq[ResolvedGrant]] { grants.map {
    case (gid, grant) => ResolvedGrant(gid, grant)
  }.toSeq }

  def findToken(tid: TokenID) = Future { tokens.get(tid) }

  def findGrant(gid: GrantID) = Future { grants.get(gid).map { ResolvedGrant(gid, _) } } 
  def findGrantChildren(gid: GrantID) = Future {
    grants.filter{ 
      case (_, grant) => grant.issuer.map { _ == gid }.getOrElse(false) 
    }.map{ ResolvedGrant.tupled.apply }(collection.breakOut)
  }

  def addGrants(tid: TokenID, add: Set[GrantID]) = Future {
    tokens.get(tid).map { t =>
      val updated = t.addGrants(add)
      tokens.put(tid, updated)
      updated
    }
  }

  def listDeletedTokens() = Future[Seq[Token]] { deletedTokens.values.toList }
  def listDeletedGrants() = Future[Seq[ResolvedGrant]] { deletedGrants.map {
    case (gid, grant) => ResolvedGrant(gid, grant)
  }.toSeq }

  def findDeletedToken(tid: TokenID) = Future { deletedTokens.get(tid) }

  def findDeletedGrant(gid: GrantID) = Future { deletedGrants.get(gid).map { ResolvedGrant(gid, _) } }
  def findDeletedGrantChildren(gid: GrantID) = Future {
    deletedGrants.filter{
      case (_, grant) => grant.issuer.map { _ == gid }.getOrElse(false)
    }.map{ ResolvedGrant.tupled.apply }(collection.breakOut)
  }

 def removeGrants(tid: TokenID, remove: Set[GrantID]) = Future {
    tokens.get(tid).map { t =>
      val updated = t.removeGrants(remove)
      tokens.put(tid, updated)
      updated
    }
  }

  def deleteToken(tid: TokenID) = Future {
    tokens.get(tid).flatMap { t =>
      deletedTokens.put(tid, t)
      tokens.remove(tid)
    }
  }
  def deleteGrant(gid: GrantID) = Future { grants.remove(gid) match {
    case Some(x) =>
      deletedGrants.put(gid, x)
      Set(ResolvedGrant(gid, x))
    case _       =>
      Set.empty
  }}

  def close() = Future(())
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

class MongoTokenManager(
    private[security] val mongo: Mongo,
    private[security] val database: Database,
    settings: MongoTokenManagerSettings = MongoTokenManagerSettings.defaults)(implicit val execContext: ExecutionContext) extends TokenManager {

  private implicit val impTimeout = settings.timeout

  def newToken(name: String, grants: Set[GrantID]) = {
    val newToken = Token(newTokenID, name, grants)
    database(insert(newToken.serialize(Token.UnsafeTokenDecomposer).asInstanceOf[JObject]).into(settings.tokens)) map {
      _ => newToken
    }
  }

  def newGrant(grant: Grant) = {
    val ng = ResolvedGrant(newGrantID, grant)
    database(insert(ng.serialize(ResolvedGrant.UnsafeResolvedGrantDecomposer).asInstanceOf[JObject]).into(settings.grants)) map { _ => ng }
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
  def listGrants() = findAll[ResolvedGrant](settings.grants)

  def findToken(tid: TokenID) = findOneMatching[Token]("tid", tid, settings.tokens)
  def findGrant(gid: GrantID) = findOneMatching[ResolvedGrant]("gid", gid, settings.grants)

  def findGrantChildren(gid: GrantID) = findAllMatching("issuer", gid, settings.grants)

  def listDeletedTokens() = findAll[Token](settings.tokens)
  def listDeletedGrants() = findAll[ResolvedGrant](settings.grants)

  def findDeletedToken(tid: TokenID) = findOneMatching[Token]("tid", tid, settings.deletedTokens)
  def findDeletedGrant(gid: GrantID) = findOneMatching[ResolvedGrant]("gid", gid, settings.deletedGrants)

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

  def deleteGrant(gid: GrantID): Future[Set[ResolvedGrant]] = {
    findGrantChildren(gid).flatMap { gc =>
      Future.sequence(gc.map { g => deleteGrant(g.gid)}).map { _.flatten }.flatMap { gds =>
        findGrant(gid).flatMap {
          case og @ Some(g) =>
            for {
              _ <- database(insert(g.serialize(ResolvedGrant.UnsafeResolvedGrantDecomposer).asInstanceOf[JObject]).into(settings.deletedGrants))
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

class CachingTokenManager(manager: TokenManager, settings: CachingTokenManagerSettings = CachingTokenManager.defaultSettings)(implicit execContext: ExecutionContext) extends TokenManager {

  private val tokenCache = Cache.concurrent[TokenID, Token](settings.tokenCacheSettings)
  private val grantCache = Cache.concurrent[GrantID, Grant](settings.grantCacheSettings)

  def newToken(name: String, grants: Set[GrantID]) =
    manager.newToken(name, grants).map { _ ->- add }
  def newGrant(grant: Grant) =
    manager.newGrant(grant).map { _ ->- add }

  def listTokens() = manager.listTokens
  def listGrants() = manager.listGrants

  def listDeletedTokens() = manager.listDeletedTokens
  def listDeletedGrants() = manager.listDeletedGrants

  def findToken(tid: TokenID) = tokenCache.get(tid) match {
    case None => manager.findToken(tid).map { _.map { _ ->- add } }
    case t    => Future(t)
  }
  def findGrant(gid: GrantID) = grantCache.get(gid) match {
    case None    => manager.findGrant(gid).map { _.map { _ ->- add } }
    case Some(g) => Future(Some(ResolvedGrant(gid, g)))
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
  private def add(g: ResolvedGrant) = grantCache.put(g.gid, g.grant)

  private def remove(t: Token) = tokenCache.remove(t.tid)
  private def remove(g: ResolvedGrant) = grantCache.remove(g.gid)

  def close() = manager.close

}

object CachingTokenManager {
  val defaultSettings = CachingTokenManagerSettings(
    CacheSettings[TokenID, Token](ExpirationPolicy(Some(5), Some(5), MINUTES)),
    CacheSettings[GrantID, Grant](ExpirationPolicy(Some(5), Some(5), MINUTES))
  )
}

object TestTokenManager extends AkkaDefaults {

  val rootUID = "root"

  val testUID = "unittest"
  val usageUID = "usage"

  val cust1UID = "user1"
  val cust2UID = "user2"

  val expiredUID = "expired"

  def standardAccountPerms(prefix: String, issuerPrefix: Option[String], p: String, owner: TokenID, expiration: Option[DateTime]) = {
    val path = Path(p)
    List(
      ResolvedGrant(prefix + "_write", WriteGrant(issuerPrefix.map{ _ + "_write" }, path, expiration)),
      ResolvedGrant(prefix + "_owner", OwnerGrant(issuerPrefix.map{ _ + "_owner" }, path, expiration)),
      ResolvedGrant(prefix + "_read", ReadGrant(issuerPrefix.map{ _ + "_read" }, path, owner, expiration)),
      ResolvedGrant(prefix + "_reduce", ReduceGrant(issuerPrefix.map{ _ + "_reduce" }, path, owner, expiration)),
      ResolvedGrant(prefix + "_modify", ModifyGrant(issuerPrefix.map{ _ + "_modify" }, path, owner, expiration)),
      ResolvedGrant(prefix + "_transform", TransformGrant(issuerPrefix.map{ _ + "_transform" }, path, owner, expiration))
    )
  }

  def publishPathPerms(prefix: String, issuerPrefix: Option[String], p: String, owner: TokenID, expiration: Option[DateTime]) = {
    val path = Path(p)
    List(
      ResolvedGrant(prefix + "_read", ReadGrant(issuerPrefix.map{ _ + "_read" }, path, owner, expiration)),
      ResolvedGrant(prefix + "_reduce", ReduceGrant(issuerPrefix.map{ _ + "_reduce" }, path, owner, expiration))
    )
  }

  val grantList = List(
    standardAccountPerms("root", None, "/", "root", None),
    standardAccountPerms("unittest", Some("root"), "/unittest/", "unittest", None),
    standardAccountPerms("usage", Some("root"), "/__usage_tracking__", "usage_tracking", None),
    standardAccountPerms("user1", Some("root"), "/user1", "user1", None),
    standardAccountPerms("user2", Some("root"), "/user2", "user2", None),
    standardAccountPerms("expired", Some("root"), "/expired", "expired", Some(new DateTime().minusYears(1000))),
    publishPathPerms("public", Some("root"), "/public", "public", None),
    publishPathPerms("opublic", Some("root"), "/opublic", "opublic", None)
  )

  val grants: mutable.Map[GrantID, Grant] = grantList.flatten.map { g => (g.gid -> g.grant) }(collection.breakOut)

  val tokens: mutable.Map[TokenID, Token] = List[Token](
    Token("root", "root", grantList(0).map { _.gid }(collection.breakOut)),
    Token("unittest", "unittest", grantList(1).map { _.gid }(collection.breakOut)),
    Token("usage", "usage", grantList(2).map { _.gid }(collection.breakOut)),
    Token("user1", "user1", (grantList(3) ++ grantList(6)).map{ _.gid}(collection.breakOut)),
    Token("user2", "user2", (grantList(4) ++ grantList(6)).map{ _.gid}(collection.breakOut)),
    Token("expired", "expired", (grantList(5) ++ grantList(6)).map{ _.gid}(collection.breakOut))
  ).map { t => (t.tid -> t) }(collection.breakOut)

  def testTokenManager(): TokenManager = new InMemoryTokenManager(tokens, grants)

}

