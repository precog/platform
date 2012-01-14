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
package com.reportgrid.analytics

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
