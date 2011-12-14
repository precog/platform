package com.reportgrid.analytics

import blueeyes.concurrent.test._
import blueeyes.persistence.mongo._

import org.specs2.mutable.Specification
import scalaz.Success

class TokenManagerSpec extends Specification with FutureMatchers {
  val mongo = new MockMongo()
  val tokenManager = new TokenManager(mongo.database("test"), "tokens", "deleted_tokens")

  "Token Manager" should {
    "automatically populate the test token" in {
      tokenManager.lookup(Token.Test.tokenId) must whenDelivered {
        beLike {
          case Some(token) => token must_== Token.Test
        }
      }
    }

    "support token creation" in {
      tokenManager.issueNew(Token.Test, "/testchild/", Token.Test.permissions, Token.Test.expires, Token.Test.limits) must whenDelivered {
        beLike {
          case Success(token) =>  (token.permissions must_== Token.Test.permissions) and
                                  (token.limits must_== Token.Test.limits) and
                                  (token.path must_== (Token.Test.path / "testchild"))
        }
      }
    }

    "retrieve undeleted tokens" in {
      val exchange = tokenManager.issueNew(Token.Test, "/testchild/", Token.Test.permissions, Token.Test.expires, Token.Test.limits) flatMap {
        case Success(token) => tokenManager.lookup(token.tokenId)
      }

      exchange must whenDelivered {
        beSome
      }
    }

    "support token deletion" in {
      val exchange = tokenManager.issueNew(Token.Test, "/testchild/", Token.Test.permissions, Token.Test.expires, Token.Test.limits) flatMap {
        case Success(token) => tokenManager.deleteDescendant(Token.Test, token.tokenId) flatMap {
          case _ => tokenManager.lookup(token.tokenId)
        }
      }

      exchange must whenDelivered {
        beNone
      }
    }

    "retrieve deleted tokens from the deleted tokens collection" in {
      val exchange = tokenManager.issueNew(Token.Test, "/testchild/", Token.Test.permissions, Token.Test.expires, Token.Test.limits) flatMap {
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
