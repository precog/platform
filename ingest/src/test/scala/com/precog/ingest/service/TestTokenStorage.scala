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
package com.precog.ingest.service

import blueeyes._
import blueeyes.concurrent.test._
import blueeyes.json.JsonAST._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.JPathImplicits._
import blueeyes.persistence.mongo._
import blueeyes.util.Clock

import akka.actor.ActorSystem
import akka.dispatch.MessageDispatcher
import akka.dispatch.Future
import akka.util.Timeout
import akka.util.duration._

import org.joda.time._
import com.weiglewilczek.slf4s.Logger

import org.specs2.mutable.Specification
import org.specs2.specification.{Outside, Scope}
import org.scalacheck.Gen._

import scalaz.{Success, Validation}
import scalaz.Scalaz._

import com.precog.analytics._

trait TestTokenStorage {

  implicit def dispatcher: MessageDispatcher
  implicit val timeout: Timeout = 5 seconds

  def TestToken: Token

  def populateTestTokens(database: Database, tokensCollection: MongoCollection) = {
    val RootTokenJ: JObject      = Token.Root.serialize.asInstanceOf[JObject]
    val TestTokenJ: JObject      = Token.Test.serialize.asInstanceOf[JObject]

    val rootTokenFuture  = database(upsert(tokensCollection).set(RootTokenJ))
    val testTokenFuture  = database(upsert(tokensCollection).set(TestTokenJ))

    Future.sequence(rootTokenFuture :: testTokenFuture :: Nil) 
  }

  val tokenCache = new scala.collection.mutable.HashMap[String, Token]
  lazy val tokenManager = new TokenStorage {
    val actorSystem = ActorSystem("test_token_storage")
    implicit def dispatcher: MessageDispatcher = actorSystem.dispatcher

    tokenCache.put(Token.Root.tokenId, Token.Root)
    tokenCache.put(TestToken.tokenId, TestToken)

    def lookup(tokenId: String): Future[Option[Token]] = Future(tokenCache.get(tokenId))
    def listChildren(parent: Token): Future[List[Token]] = Future {
      tokenCache flatMap { case (_, v) => v.parentTokenId.exists(_ == parent.tokenId).option(v) } toList 
    }

    def issueNew(parent: Token, path: Path, permissions: Permissions, expires: DateTime, limits: Limits): Future[Validation[String, Token]] = {
      val newToken = parent.issue(path, permissions, expires, limits)
      tokenCache.put(newToken.tokenId, newToken)
      Future(newToken.success[String])
    }

    protected def deleteToken(token: Token): Future[Token] = {
      Future(tokenCache.remove(token.tokenId).getOrElse(token))
    }
  }
}

// vim: set ts=4 sw=4 et:
