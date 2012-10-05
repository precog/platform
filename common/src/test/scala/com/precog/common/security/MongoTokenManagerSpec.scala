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

import blueeyes.json.JsonAST
import blueeyes.json.JsonParser
import blueeyes.json.Printer
import blueeyes.json.xschema.{ ValidatedExtraction, Extractor, Decomposer }
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.Extractor._

import org.streum.configrity._

import scalaz._

object MongoTokenManagerSpec extends Specification {
 

  val timeout = Duration(30, "seconds")

  "mongo token manager" should {
    "find token present" in new tokenManager { 

      lazy val result = Await.result(tokenManager.findToken(root.tid), timeout)

      result must beLike {
        case Some(Token(_,tid,_,_)) => tid must_== root.tid
      }
    }
    "not find missing token" in new tokenManager { 

      val result = Await.result(tokenManager.findToken(notFoundTokenID), timeout)

      result must beLike {
        case None => ok 
      }
    }
    "issue new token" in new tokenManager { 
      val name = "newToken"
      val fResult = tokenManager.newToken(name, "", Set.empty)

      val result = Await.result(fResult, timeout)

      result must beLike {
        case Token(n,_,_,g) => 
          name must_== n 
          Set.empty must_== g
      }
    }
    "move token to deleted pool on deletion" in new tokenManager { 

      type Results = (Option[Token], Option[Token], Option[Token], Option[Token])

      val fut: Future[Results] = for { 
        before <- tokenManager.findToken(root.tid)
        deleted <- tokenManager.deleteToken(before.get.tid)
        after <- tokenManager.findToken(root.tid)
        deleteCol <- tokenManager.findDeletedToken(root.tid)
      } yield {
        (before, deleted, after, deleteCol)
      }

      val result = Await.result(fut, timeout)

      result must beLike {
        case (Some(t1), Some(t2), None, Some(t3)) => 
          t1 must_== t2
          t1 must_== t3
      }
    }
    "no failure on deleting token that is already deleted" in new tokenManager { 
      type Results = (Option[Token], Option[Token], Option[Token], Option[Token], Option[Token])

      val fut: Future[Results] = for { 
        before <- tokenManager.findToken(root.tid)
        deleted1 <- tokenManager.deleteToken(before.get.tid)
        deleted2 <- tokenManager.deleteToken(before.get.tid)
        after <- tokenManager.findToken(root.tid)
        deleteCol <- tokenManager.findDeletedToken(root.tid)
      } yield {
        (before, deleted1, deleted2, after, deleteCol)
      }

      val result = Await.result(fut, timeout)

      result must beLike {
        case (Some(t1), Some(t2), None, None, Some(t4)) => 
          t1 must_== t2
          t1 must_== t4
      }
    }
  }

  object Counter {
    val cnt = new java.util.concurrent.atomic.AtomicLong
  }

  class tokenManager extends After {
    val defaultActorSystem = ActorSystem("tokenManagerTest")
    implicit val execContext = ExecutionContext.defaultExecutionContext(defaultActorSystem)

    val mongo = new MockMongo
    val tokenManager = new MongoTokenManager(mongo, mongo.database("test_v1"), MongoTokenManagerSettings.defaults)

    val to = Duration(30, "seconds")
  
    val notFoundTokenID = "NOT-GOING-TO-FIND"

    val root = Await.result(tokenManager.newToken("root", "", Set.empty), to)
    val child1 = Await.result(tokenManager.newToken("child1", root.tid, Set.empty), to)
    val child2 = Await.result(tokenManager.newToken("child2", root.tid, Set.empty), to)
    val grantChild1 = Await.result(tokenManager.newToken("grandChild1", child1.tid, Set.empty), to)

    def after = { 
      defaultActorSystem.shutdown 
    }
  }
}
