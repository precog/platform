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
import blueeyes.json.JsonAST._
import blueeyes.persistence.mongo._
import blueeyes.persistence.cache._
import blueeyes.json.xschema.Extractor._
import blueeyes.json.xschema.DefaultSerialization._

import akka.util.Timeout
import akka.dispatch.Future
import akka.dispatch.ExecutionContext

import java.util.concurrent.TimeUnit._

import org.streum.configrity.Configuration

import scalaz._
import Scalaz._

trait TokenManagerComponent {
  def tokenManager: TokenManager
}

trait TokenManager {

  implicit def execContext: ExecutionContext

  def lookup(uid: UID): Future[Option[Token]]
  def lookupDeleted(uid: UID): Future[Option[Token]]
  def listChildren(parent: Token): Future[List[Token]]
  def issueNew(uid: UID, issuer: Option[UID], permissions: Permissions, grants: Set[UID], expired: Boolean): Future[Validation[String, Token]]
  def deleteToken(token: Token): Future[Token]

  private def newUUID() = java.util.UUID.randomUUID().toString.toUpperCase

  def issueNew(issuer: Option[UID], permissions: Permissions, grants: Set[UID], expired: Boolean): Future[Validation[String, Token]] = {
    issueNew(newUUID(), issuer, permissions, grants, expired)
  }

  def listDescendants(parent: Token): Future[List[Token]] = {
    listChildren(parent) flatMap { c =>
      Future.sequence(c.map(child => listDescendants(child) map { child :: _ })).map(_.flatten)
    }
  }

  def getDescendant(parent: Token, descendantTokenId: UID): Future[Option[Token]] = {
    listDescendants(parent).map(_.find(_.uid == descendantTokenId))
  }

  def deleteDescendant(parent: Token, descendantTokenId: UID): Future[List[Token]] = {
    getDescendant(parent, descendantTokenId).flatMap { parent =>
      val removals = parent.toList.map { tok =>
        listDescendants(tok) flatMap { descendants =>
          Future.sequence((tok :: descendants) map (deleteToken))
        }
      }

      Future.sequence(removals).map(_.flatten)
    }
  }
}

class MongoTokenManager(private[security] val database: Database, collection: String, deleted: String, timeout: Timeout)(implicit val execContext: ExecutionContext) extends TokenManager {

  private implicit val impTimeout = timeout

  def lookup(uid: UID): Future[Option[Token]] = find(uid, collection) 
  def lookupDeleted(uid: UID): Future[Option[Token]] = find(uid, deleted) 

  private def find(uid: UID, col: String) = database {
    selectOne().from(col).where("uid" === uid)
  }.map{
    _.map(_.deserialize[Token])
  }

  def listChildren(parent: Token): Future[List[Token]] =  {
    database {
      selectAll.from(collection).where{
        "issuer" === parent.uid
      }
    }.map{ result =>
      result.toList.map(_.deserialize[Token])
    }
  }

  def issueNew(uid: UID, issuer: Option[UID], permissions: Permissions, grants: Set[UID], expired: Boolean): Future[Validation[String, Token]] = {
    lookup(uid) flatMap { 
      case Some(_) => Future("A token with this id already exists.".fail)
      case None    =>
        val newToken = Token(uid, issuer, permissions, grants, expired)
        val newTokenSerialized = newToken.serialize.asInstanceOf[JObject]
        database(insert(newTokenSerialized).into(collection)) map (_ => newToken.success)
    }
  }

  def deleteToken(token: Token): Future[Token] = for {
    _ <- database(insert(token.serialize.asInstanceOf[JObject]).into(deleted))
    _ <- database(remove.from(collection).where("uid" === token.uid))
  } yield {
    token
  }
}

trait CachedMongoTokenManagerComponent {
  def tokenManagerFactory(config: Configuration): TokenManager = {
    sys.error("todo")
  }
}

class CachingTokenManager(delegate: TokenManager, settings: CacheSettings[String, Token] = CachingTokenManager.defaultSettings)(implicit val execContext: ExecutionContext) extends TokenManager {

  private val tokenCache = Cache.concurrent[String, Token](settings)

  def lookup(uid: UID): Future[Option[Token]] =
    tokenCache.get(uid) match {
      case sm @Some(t) => Future(sm: Option[Token])
      case None        => delegate.lookup(uid).map{ ot => ot ->- { _.foreach{ t => tokenCache.put(uid, t) }}}
    }
  
  def lookupDeleted(uid: UID): Future[Option[Token]] = delegate.lookupDeleted(uid)
  
  def listChildren(parent: Token): Future[List[Token]] = 
    delegate.listChildren(parent) 

  def issueNew(uid: UID, issuer: Option[UID], permissions: Permissions, grants: Set[UID], expired: Boolean): Future[Validation[String, Token]] = 
    delegate.issueNew(uid, issuer, permissions, grants, expired) 

  def deleteToken(token: Token): Future[Token] = 
    delegate.deleteToken(token) map { t => t ->- remove }

  private def remove(t: Token) = tokenCache.remove(t.uid) 
}

object CachingTokenManager {
  val defaultSettings = CacheSettings[String, Token](ExpirationPolicy(Some(5), Some(5), MINUTES))
}

class StaticTokenManager(implicit val execContext: ExecutionContext) extends TokenManager {
  import StaticTokenManager._
  
  def lookup(uid: UID) = Future(map.get(uid))
  def lookupDeleted(uid: UID) = Future(None)
  
  def listChildren(parent: Token): Future[List[Token]] = Future {
    map.values.filter( _.issuer match {
      case Some(`parent`) => true
      case _              => false
    }).toList
  }

  def issueNew(uid: UID, issuer: Option[UID], permissions: Permissions, grants: Set[UID], expired: Boolean): Future[Validation[String, Token]] = {
    val newToken = Token(uid, issuer, permissions, grants, expired)
    map += (uid -> newToken)
    Future(Success(newToken))
  }

  def deleteToken(token: Token): Future[Token] = Future {
    map -= token.uid
    token
  }
}

object StaticTokenManager {
  
  val rootUID = "C18ED787-BF07-4097-B819-0415C759C8D5"
  
  val testUID = "03C4F5FE-69E2-4151-9D93-7C986936CB86"
  val usageUID = "6EF2E81E-D9E8-4DC6-AD66-DEB30A164F73"
  
  val publicUID = "B88E82F0-B78B-49D9-A36B-78E0E61C4EDC"
 
  val cust1UID = "C5EF0038-A2A2-47EB-88A4-AAFCE59EC22B"
  val cust2UID = "1B10E413-FB5B-4769-A887-8AFB587CF00A"
  
  val expiredUID = "expired"
  
  def standardAccountPerms(path: String, owner: UID, mayShare: Boolean = true) =
    Permissions(
      MayAccessPath(Subtree(Path(path)), PathRead, mayShare), 
      MayAccessPath(Subtree(Path(path)), PathWrite, mayShare)
    )(
      MayAccessData(Subtree(Path("/")), OwnerAndDescendants(owner), DataQuery, mayShare)
    )

  def publishPathPerms(path: String, owner: UID, mayShare: Boolean = true) =
    Permissions(
      MayAccessPath(Subtree(Path(path)), PathRead, mayShare)
    )(
      MayAccessData(Subtree(Path(path)), OwnerAndDescendants(owner), DataQuery, mayShare)
    )

  val config = List[(UID, Option[UID], Permissions, Set[UID], Boolean)](
    (rootUID, None, standardAccountPerms("/", rootUID, true), Set(), false),
    (publicUID, Some(rootUID), publishPathPerms("/public", rootUID, true), Set(), false),
    (testUID, Some(rootUID), standardAccountPerms("/unittest", testUID, true), Set(), false),
    (usageUID, Some(rootUID), standardAccountPerms("/__usage_tracking__", usageUID, true), Set(), false),
    (cust1UID, Some(rootUID), standardAccountPerms("/user1", cust1UID, true), Set(publicUID), false),
    (cust2UID, Some(rootUID), standardAccountPerms("/user2", cust2UID, true), Set(publicUID), false),
    (expiredUID, Some(rootUID), standardAccountPerms("/expired", expiredUID, true), Set(publicUID), true)
  )

  private var map = Map(config map Token.tupled map { t => (t.uid, t) }: _*)

}
