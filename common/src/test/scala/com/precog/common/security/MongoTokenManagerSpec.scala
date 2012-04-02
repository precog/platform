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

import org.specs2.execute.Result
import org.specs2.mutable.{After, Specification}
import org.specs2.specification._

import akka.actor.ActorSystem
import akka.util.Duration
import akka.util.Timeout
import akka.dispatch.Future
import akka.dispatch.Await
import akka.dispatch.ExecutionContext

import blueeyes.bkka.AkkaDefaults
import blueeyes.persistence.mongo._

import blueeyes.json.JsonParser
import blueeyes.json.Printer
import blueeyes.json.xschema.{ ValidatedExtraction, Extractor, Decomposer }
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.Extractor._

import org.streum.configrity._

import scalaz._

object MongoTokenManagerSpec extends Specification {
 
  val foundToken = "C18ED787-BF07-4097-B819-0415C759C8D5" 
  val child1Token = "03C4F5FE-69E2-4151-9D93-7C986936CB86-C1" 
  val child2Token = "03C4F5FE-69E2-4151-9D93-7C986936CB86-C2" 
  val grandChild1Token = "03C4F5FE-69E2-4151-9D93-7C986936CB86-GC1" 
  val notFoundToken = "NOT-GOING-TO-FIND"

  val timeout = Duration(30, "seconds")

  "mongo token manager" should {
    "find token present" in new tokenManager { 

      lazy val result = Await.result(tokenManager.lookup(foundToken), timeout)

      result must beLike {
        case Some(Token(uid,_,_,_,_)) => uid must_== foundToken
      }
    }
    "not find missing token" in new tokenManager { 

      val result = Await.result(tokenManager.lookup(notFoundToken), timeout)

      result must beLike {
        case None => ok 
      }
    }
    "issue new token" in new tokenManager { 
      val fResult = tokenManager.issueNew(Some(foundToken), Permissions(Set.empty, Set.empty), Set.empty, false)

      val result = Await.result(fResult, timeout)

      result must beLike {
        case Success(Token(_,Some(issuer),_,_,_)) => issuer must_== foundToken
      }
    }
    "refuse to issue new token with conflicting uid" in new tokenManager { 
      val fut = tokenManager.issueNew(foundToken, None, Permissions(Set.empty, Set.empty), Set.empty, false)

      val result = Await.result(fut, timeout)

      result must beLike {
        case Failure(_) => ok
      }
    }
    "move token to deleted pool on deletion" in new tokenManager { 

      type Results = (Option[Token], Token, Option[Token], Option[Token])

      val fut: Future[Results] = for { 
        before <- tokenManager.lookup(foundToken)
        deleted <- tokenManager.deleteToken(before.get)
        after <- tokenManager.lookup(foundToken)
        deleteCol <- tokenManager.lookupDeleted(foundToken)
      } yield {
        (before, deleted, after, deleteCol)
      }

      val result = Await.result(fut, timeout)

      result must beLike {
        case (Some(t1), t2, None, Some(t3)) => 
          t1 must_== t2
          t1 must_== t3
      }
    }
    "no failure on deleting token that is already deleted" in new tokenManager { 
      type Results = (Option[Token], Token, Token, Option[Token], Option[Token])

      val fut: Future[Results] = for { 
        before <- tokenManager.lookup(foundToken)
        deleted1 <- tokenManager.deleteToken(before.get)
        deleted2 <- tokenManager.deleteToken(before.get)
        after <- tokenManager.lookup(foundToken)
        deleteCol <- tokenManager.lookupDeleted(foundToken)
      } yield {
        (before, deleted1, deleted2, after, deleteCol)
      }

      val result = Await.result(fut, timeout)

      result must beLike {
        case (Some(t1), t2, t3, None, Some(t4)) => 
          t1 must_== t2
          t1 must_== t3
          t1 must_== t4
      }
    }
    "list direct children of a given token" in new tokenManager { 
      val fut = tokenManager.lookup(foundToken) flatMap { 
        case Some(t) => tokenManager.listChildren(t).map(Some(_))
        case None    => Future(None)
      }

      val result = Await.result(fut, timeout)

      result must beLike {
        case Some(l) => l must haveSize(2) 
      }
    }
    "succeed with empty list when token has no children" in new tokenManager { 
      val fut = tokenManager.lookup(child1Token) flatMap { 
        case Some(t) => tokenManager.listChildren(t).map(Some(_))
        case None    => Future(None)
      }

      val result = Await.result(fut, timeout)

      result must beLike {
        case Some(l) => l must haveSize(0) 
      }
    }
    "list children and grandchild as descendants" in new tokenManager { 
      val fut = tokenManager.lookup(foundToken) flatMap { 
        case Some(t) => tokenManager.listDescendants(t).map(Some(_))
        case None    => Future(None)
      }

      val result = Await.result(fut, timeout)

      result must beLike {
        case Some(l) => l must haveSize(4) 
      }
    }
    "return empty list when no children or grandchild" in new tokenManager { 
      val fut = tokenManager.lookup(child1Token) flatMap { 
        case Some(t) => tokenManager.listDescendants(t).map(Some(_))
        case None    => Future(None)
      }

      val result = Await.result(fut, timeout)

      result must beLike {
        case Some(l) => l must haveSize(0) 
      }
    }
    "get descendant with a given uid" in new tokenManager { 
      val fut = tokenManager.lookup(foundToken) flatMap { 
        case Some(t) => tokenManager.getDescendant(t, grandChild1Token)
        case None    => Future(None)
      }

      val result = Await.result(fut, timeout)

      result must beLike {
        case Some(Token(uid,_,_,_,_)) => uid must_== grandChild1Token 
      }
    }
    "return none when no match get descendant with the given uid" in new tokenManager { 
      val fut = tokenManager.lookup(child1Token) flatMap { 
        case Some(t) => tokenManager.getDescendant(t, grandChild1Token)
        case None    => Future(None)
      }

      val result = Await.result(fut, timeout)

      result must beLike {
        case None => ok 
      }
    }
  }

  trait tokenManager extends After with MongoTokenManagerComponent {
    lazy val defaultActorSystem = ActorSystem("token_manager_test")

    val config = Configuration.parse("""
    tokenManager {
      mongo {
        mock = true
        database = "test"
        collection = "tokens"
        deleted = "deleted_tokens"
      }
      cached = true
    }
    """)

    lazy val tokenManager = tokenManagerFactory(config.detach("tokenManager"))

    def readTestTokens(): List[Token] = {
      val rawTestTokens = scala.io.Source.fromInputStream(getClass.getResourceAsStream("test_tokens.json")).mkString
      JsonParser.parse(rawTestTokens).deserialize[List[Token]]
    }

    def insert(ts: List[Token]): Future[List[Token]] = {
      Future.traverse(ts)(insert)
    }

    def insert(t: Token): Future[Token] = {
      tokenManager.issueNew(t.uid, t.issuer, t.permissions, t.grants, t.expired) map { _.toOption.get }
    }

    val f = insert(readTestTokens)
    Await.result(f, Duration(120, "seconds"))
    
    def after = { 
      defaultActorSystem.shutdown 
    }
  }

}
