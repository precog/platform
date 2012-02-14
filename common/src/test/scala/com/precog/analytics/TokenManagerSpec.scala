package com.precog.analytics

import blueeyes.concurrent.test._
import blueeyes.bkka.AkkaDefaults
import blueeyes.persistence.mongo._

import akka.util.Timeout
import akka.util.Duration

import org.specs2.mutable.Specification
import scalaz.Success

class TokenManagerSpec extends Specification with FutureMatchers {
  
  implicit val dispatcher = AkkaDefaults.defaultFutureDispatch
  implicit val timeout: Timeout = Duration(30, "seconds")

  val mongo = new MockMongo()
  val tokenManager = new TokenManager(mongo.database("test"), "tokens", "deleted_tokens")

  implicit val futureTimes = FutureTimeouts(25, Duration(250, "millis"))

  "Token Manager" should {
    "automatically populate the test token" in {
      tokenManager.lookup(Token.Test.tokenId) must whenDelivered {
        beLike {
          case Some(token) => token must_== Token.Test
        }
      }
    }

    "support token creation" in {
      tokenManager.issueNew(Token.Test, Path("/testchild/"), Token.Test.permissions, Token.Test.expires, Token.Test.limits) must whenDelivered {
        beLike {
          case Success(token) =>  (token.permissions must_== Token.Test.permissions) and
                                  (token.limits must_== Token.Test.limits) and
                                  (token.path must_== (Token.Test.path / Path("testchild")))
        }
      }
    }

    "retrieve undeleted tokens" in {
      val exchange = tokenManager.issueNew(Token.Test, Path("/testchild/"), Token.Test.permissions, Token.Test.expires, Token.Test.limits) flatMap {
        case Success(token) => tokenManager.lookup(token.tokenId)
      }

      exchange must whenDelivered {
        beSome
      }
    }

    "support token deletion" in {
      val exchange = tokenManager.issueNew(Token.Test, Path("/testchild/"), Token.Test.permissions, Token.Test.expires, Token.Test.limits) flatMap {
        case Success(token) => tokenManager.deleteDescendant(Token.Test, token.tokenId) flatMap {
          case _ => tokenManager.lookup(token.tokenId)
        }
      }

      exchange must whenDelivered {
        beNone
      }
    }

    "retrieve deleted tokens from the deleted tokens collection" in {
      val exchange = tokenManager.issueNew(Token.Test, Path("/testchild/"), Token.Test.permissions, Token.Test.expires, Token.Test.limits) flatMap {
        case Success(token) => tokenManager.deleteDescendant(Token.Test, token.tokenId) flatMap {
          case _ => tokenManager.findDeleted(token.tokenId)
        }
      }

      exchange must whenDelivered {
        beSome
      }
    }
  }
}

// vim: set ts=4 sw=4 et:
