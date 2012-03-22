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

  trait tokenManager extends After {
    private lazy val actorSystem = ActorSystem("token_manager_test")
    implicit lazy val executionContext = ExecutionContext.defaultExecutionContext(actorSystem)
    lazy val database = new MockMongo().database("test")
    lazy val tokenManager = new MongoTokenManager(database, "tokens", "deleted_tokens", Timeout(15000))

    def readTestTokens(): List[Token] = {
      val rawTestTokens = io.Source.fromInputStream(getClass.getResourceAsStream("test_tokens.json")).mkString
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
      actorSystem.shutdown 
    }
  }

}
