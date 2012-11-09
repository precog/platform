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
  
  override def mongoStartupPause = Some(0l)
  val timeout = Duration(10, "seconds")

  lazy val logger = LoggerFactory.getLogger("com.precog.common.security.MongoAPIKeyManagerSpec")

  "mongo API key manager" should {
    
    "find API key present" in new TestAPIKeyManager { 
      val result = Await.result(apiKeyManager.findAPIKey(rootAPIKey), timeout)

      result must beLike {
        case Some(APIKeyRecord(apiKey, _, _, _, _, _)) => apiKey must_== rootAPIKey
      }
    }
    
    "not find missing API key" in new TestAPIKeyManager { 

      val result = Await.result(apiKeyManager.findAPIKey(notFoundAPIKeyID), timeout)

      result must beLike {
        case None => ok 
      }
    }
    
    "issue new API key" in new TestAPIKeyManager { 
      val name = "newAPIKey"
      val fResult = apiKeyManager.newAPIKey(Some(name), None, rootAPIKey, Set.empty)

      val result = Await.result(fResult, timeout)

      result must beLike {
        case APIKeyRecord(_, n, _, _, g, _) => 
          Some(name) must_== n
          Set.empty must_== g
      }
    }
    
    "move API key to deleted pool on deletion" in new TestAPIKeyManager { 

      type Results = (Option[APIKeyRecord], Option[APIKeyRecord], Option[APIKeyRecord], Option[APIKeyRecord])

      val fut: Future[Results] = for { 
        before <- apiKeyManager.findAPIKey(child2.apiKey)
        deleted <- apiKeyManager.deleteAPIKey(before.get.apiKey)
        after <- apiKeyManager.findAPIKey(child2.apiKey)
        deleteCol <- apiKeyManager.findDeletedAPIKey(child2.apiKey)
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
    
    "no failure on deleting API key that is already deleted" in new TestAPIKeyManager { 
      type Results = (Option[APIKeyRecord], Option[APIKeyRecord], Option[APIKeyRecord], Option[APIKeyRecord], Option[APIKeyRecord])

      val fut: Future[Results] = for { 
        before <- apiKeyManager.findAPIKey(child2.apiKey)
        deleted1 <- apiKeyManager.deleteAPIKey(before.get.apiKey)
        deleted2 <- apiKeyManager.deleteAPIKey(before.get.apiKey)
        after <- apiKeyManager.findAPIKey(child2.apiKey)
        deleteCol <- apiKeyManager.findDeletedAPIKey(child2.apiKey)
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

  trait TestAPIKeyManager extends After {
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

    val rootAPIKey = Await.result(apiKeyManager.rootAPIKey, to)
    val child1 = Await.result(apiKeyManager.newAPIKey(Some("child1"), None, rootAPIKey, Set.empty), to)
    val child2 = Await.result(apiKeyManager.newAPIKey(Some("child2"), None, rootAPIKey, Set.empty), to)
    val grantChild1 = Await.result(apiKeyManager.newAPIKey(Some("grantChild1"), None, child1.apiKey, Set.empty), to)

    // wait until the keys appear in the DB (some delay between insert request and actor insert)
    def waitForAppearance(apiKey: APIKey, name: String) {
      while (Await.result(apiKeyManager.findAPIKey(apiKey), to) == None) {
        logger.debug("Waiting for " + name)
        Thread.sleep(100)
      }
    }

    logger.debug("Starting base setup")

    waitForAppearance(rootAPIKey, "root")
    waitForAppearance(child1.apiKey, "child1")
    waitForAppearance(child2.apiKey, "child2")
    waitForAppearance(grantChild1.apiKey, "grantChild1")

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
