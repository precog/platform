package com.querio.ingest.service

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
import net.lag.configgy.ConfigMap
import com.weiglewilczek.slf4s.Logger

import org.specs2.mutable.Specification
import org.specs2.specification.{Outside, Scope}
import org.scalacheck.Gen._

import scalaz.{Success, Validation}
import scalaz.Scalaz._

import com.reportgrid.analytics._

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
    val actorSystem = ActorSystem()
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
