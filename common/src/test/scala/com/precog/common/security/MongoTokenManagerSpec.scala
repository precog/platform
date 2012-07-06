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
        case Some(Token(tid,_,_)) => tid must_== root.tid
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
      val fResult = tokenManager.newToken(name, Set.empty)

      val result = Await.result(fResult, timeout)

      result must beLike {
        case Token(_,n,g) => 
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

    val root = Await.result(tokenManager.newToken("root", Set.empty), to)
    val child1 = Await.result(tokenManager.newToken("child1", Set.empty), to)
    val child2 = Await.result(tokenManager.newToken("child2", Set.empty), to)
    val grantChild1 = Await.result(tokenManager.newToken("grandChild1", Set.empty), to)

    def after = { 
      defaultActorSystem.shutdown 
    }
  }
}
