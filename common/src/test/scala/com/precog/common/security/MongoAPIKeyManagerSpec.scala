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

import com.mongodb.{Mongo => TGMongo}

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
import blueeyes.concurrent.test.FutureMatchers
import blueeyes.persistence.mongo._

import blueeyes.json._

import org.streum.configrity._

import org.slf4j.LoggerFactory

import scalaz._

class MongoAPIKeyManagerSpec extends Specification with RealMongoSpecSupport with FutureMatchers {
  sequential
  val timeout = Duration(30, "seconds")

  lazy val logger = LoggerFactory.getLogger("com.precog.common.security.MongoAPIKeyManagerSpec")

  "mongo API key manager" should {
    "find API key present" in new apiKeyManager { 
      logger.debug("Starting test run")
      val result = Await.result(apiKeyManager.findAPIKey(root.tid), timeout)

      result must beLike {
        case Some(APIKeyRecord(_,tid,_,_)) => tid must_== root.tid
      }
    }
    "not find missing API key" in new apiKeyManager { 

      val result = Await.result(apiKeyManager.findAPIKey(notFoundAPIKeyID), timeout)

      result must beLike {
        case None => ok 
      }
    }
    "issue new API key" in new apiKeyManager { 
      val name = "newAPIKey"
      val fResult = apiKeyManager.newAPIKey(name, "", Set.empty)

      val result = Await.result(fResult, timeout)

      result must beLike {
        case APIKeyRecord(n,_,_,g) => 
          name must_== n 
          Set.empty must_== g
      }
    }
    "move API key to deleted pool on deletion" in new apiKeyManager { 

      type Results = (Option[APIKeyRecord], Option[APIKeyRecord], Option[APIKeyRecord], Option[APIKeyRecord])

      val fut: Future[Results] = for { 
        before <- apiKeyManager.findAPIKey(root.tid)
        deleted <- apiKeyManager.deleteAPIKey(before.get.tid)
        after <- apiKeyManager.findAPIKey(root.tid)
        deleteCol <- apiKeyManager.findDeletedAPIKey(root.tid)
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
    "no failure on deleting API key that is already deleted" in new apiKeyManager { 
      type Results = (Option[APIKeyRecord], Option[APIKeyRecord], Option[APIKeyRecord], Option[APIKeyRecord], Option[APIKeyRecord])

      val fut: Future[Results] = for { 
        before <- apiKeyManager.findAPIKey(root.tid)
        deleted1 <- apiKeyManager.deleteAPIKey(before.get.tid)
        deleted2 <- apiKeyManager.deleteAPIKey(before.get.tid)
        after <- apiKeyManager.findAPIKey(root.tid)
        deleteCol <- apiKeyManager.findDeletedAPIKey(root.tid)
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

  trait apiKeyManager extends After {
    import MongoAPIKeyManagerSpec.dbId
    val defaultActorSystem = ActorSystem("apiKeyManagerTest")
    implicit val execContext = ExecutionContext.defaultExecutionContext(defaultActorSystem)

    val dbName = "test_v1_" + dbId.getAndIncrement()
    val testDB = try {
      mongo.database(dbName)
    } catch {
      case t => logger.error("Error during DB setup: " + t); throw t
    }
    val apiKeyManager = new MongoAPIKeyManager(mongo, testDB, MongoAPIKeyManagerSettings.defaults)

    val to = Duration(30, "seconds")
  
    val notFoundAPIKeyID = "NOT-GOING-TO-FIND"

    logger.debug("Starting base setup")

    val root = Await.result(apiKeyManager.newAPIKey("root", "", Set.empty), to)
    val child1 = Await.result(apiKeyManager.newAPIKey("child1", root.tid, Set.empty), to)
    val child2 = Await.result(apiKeyManager.newAPIKey("child2", root.tid, Set.empty), to)
    val grantChild1 = Await.result(apiKeyManager.newAPIKey("grandChild1", child1.tid, Set.empty), to)

    // wait until the keys appear in the DB (some delay between insert request and actor insert)
    def waitForAppearance(key: APIKeyRecord) {
      while (Await.result(apiKeyManager.findAPIKey(key.tid), to) == None) {
        logger.debug("Waiting for " + key.name)
        Thread.sleep(100)
      }
    }

    waitForAppearance(root)
    waitForAppearance(child1)
    waitForAppearance(child2)
    waitForAppearance(grantChild1)

    logger.debug("Base setup complete")

    def after = {
      logger.debug("Cleaning up run for " + dbName)
      // Wipe the DB out
      realMongo.dropDatabase(dbName)
      defaultActorSystem.shutdown 
    }
  }
}

object MongoAPIKeyManagerSpec {
  val dbId = new java.util.concurrent.atomic.AtomicInteger
}
