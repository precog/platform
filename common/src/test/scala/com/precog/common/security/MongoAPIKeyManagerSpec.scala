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

import org.streum.configrity._

import scalaz._

object MongoAPIKeyManagerSpec extends Specification {
 

  val timeout = Duration(30, "seconds")

  "mongo API key manager" should {
    
    "find API key present" in new apiKeyManager { 

      lazy val result = Await.result(apiKeyManager.findAPIKey(rootAPIKey), timeout)

      result must beLike {
        case Some(APIKeyRecord(apiKey, _, _, _, _, _)) => apiKey must_== rootAPIKey
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
      val fResult = apiKeyManager.newAPIKey(Some(name), None, rootAPIKey, Set.empty)

      val result = Await.result(fResult, timeout)

      result must beLike {
        case APIKeyRecord(_, n, _, _, g, _) => 
          Some(name) must_== n
          Set.empty must_== g
      }
    }
    
    "move API key to deleted pool on deletion" in new apiKeyManager { 

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
    
    "no failure on deleting API key that is already deleted" in new apiKeyManager { 
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

  object Counter {
    val cnt = new java.util.concurrent.atomic.AtomicLong
  }

  class apiKeyManager extends After {
    val defaultActorSystem = ActorSystem("apiKeyManagerTest")
    implicit val execContext = ExecutionContext.defaultExecutionContext(defaultActorSystem)

    val mongo = new MockMongo
    val apiKeyManager = new MongoAPIKeyManager(mongo, mongo.database("test_v1"), MongoAPIKeyManagerSettings.defaults)

    val to = Duration(30, "seconds")
  
    val notFoundAPIKeyID = "NOT-GOING-TO-FIND"

    val rootAPIKey = Await.result(apiKeyManager.rootAPIKey, to)
    val child1 = Await.result(apiKeyManager.newAPIKey(Some("child1"), None, rootAPIKey, Set.empty), to)
    val child2 = Await.result(apiKeyManager.newAPIKey(Some("child2"), None, rootAPIKey, Set.empty), to)
    val grantChild1 = Await.result(apiKeyManager.newAPIKey(Some("grandChild1"), None, child1.apiKey, Set.empty), to)

    def after = { 
      defaultActorSystem.shutdown 
    }
  }
}
