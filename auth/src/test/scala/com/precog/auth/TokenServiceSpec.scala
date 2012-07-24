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
package com.precog
package auth

import com.precog.common.Path
import com.precog.common.security._

import org.specs2.mutable._
import org.scalacheck.Gen._

import akka.actor.ActorSystem
import akka.dispatch.ExecutionContext
import akka.dispatch.Future
import akka.util.Duration

import org.joda.time._

import org.streum.configrity.Configuration
import org.streum.configrity.io.BlockFormat

import scalaz.{Validation, Success, NonEmptyList}
import scalaz.Scalaz._
import scalaz.Validation._

import blueeyes.concurrent.test._

import blueeyes.core.data._
import blueeyes.bkka.AkkaDefaults
import blueeyes.core.service.test.BlueEyesServiceSpecification
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.http.MimeTypes
import blueeyes.core.http.MimeTypes._

import blueeyes.json.xschema.{ ValidatedExtraction, Extractor, Decomposer }
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.Extractor._

import blueeyes.json.JsonAST._

import blueeyes.util.Clock

trait TestTokenService extends BlueEyesServiceSpecification with TokenService with AkkaDefaults with MongoTokenManagerComponent {

  import BijectionsChunkJson._

  val config = """ 
    security {
      test = true
      mongo {
        mock = true
        servers = [localhost]
        database = test
      }
    }   
  """

  override val configuration = "services { auth { v1 { " + config + " } } }"

  lazy val client = service.contentType[JValue](application/(MimeTypes.json)).path("/tokens")

  override implicit val defaultFutureTimeouts: FutureTimeouts = FutureTimeouts(20, Duration(1, "second"))
  val shortFutureTimeouts = FutureTimeouts(5, Duration(50, "millis"))

  val asyncContext = defaultFutureDispatch
}

class TokenServiceSpec extends TestTokenService with FutureMatchers with Tags {
  import TestTokenManager._

  def get(tokenId: String) = 
    client.query("tokenId", tokenId).get("")

//  def create(tokenId: String, create: TokenCreate) =
//    createRaw(tokenId, create.serialize)
 
  def createRaw(tokenId: String, create: JValue) = 
    client.query("tokenId", tokenId).post("")(create)

  def delete(tokenId: String, target: String) =
    client.query("tokenId", tokenId).query("delete", target).delete("")

//  def update(tokenId: String, target: String, update: TokenUpdate) =
//    updateRaw(tokenId, target, update.serialize)

  def updateRaw(tokenId: String, target: String, update: JValue) =
    client.query("tokenId", tokenId).query("update", target).post("")(update)

  "Token service" should {
    "get existing token" in {
      get(rootUID) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(jt), _) => ok
      }}
    }.pendingUntilFixed
    "return error on get and token not found" in {
      get("not-gonna-find-it") must whenDelivered { beLike {
        case HttpResponse(HttpStatus(BadRequest, _), _, Some(JString("The specified token does not exist")), _) => ok
      }}
    }.pendingUntilFixed
    "create token with defaults" in {
//      val createToken = TokenCreate(None, None, None)
//      create(rootUID, createToken) must whenDelivered { beLike {
//        case HttpResponse(HttpStatus(OK, _), _, Some(jdl), _) => 
//          val dl = jdl.deserialize[Token]
//          dl must beLike {
//            case Token(_, Some(rootUID), perms, grants, false) => 
//              grants must haveSize(0)
//              tokenMap(rootUID).permissions.sharable must_== perms
//          }
//      }}
      todo
    }
    "create token with overrides" in {
//      val perms = standardAccountPerms("/overrides/")
//      val createToken = TokenCreate(Some(perms), Some(Set(publicUID)), Some(true))
//      create(rootUID, createToken) must whenDelivered { beLike {
//        case HttpResponse(HttpStatus(OK, _), _, Some(jdl), _) => 
//          val dl = jdl.deserialize[Token]
//          dl must beLike {
//            case Token(_, Some(rootUID), tperms, grants, true) =>
//              grants must haveSize(1)
//              grants.head must_== publicUID 
//              perms must_== perms 
//          }
//      }}
      todo
    }
    "don't create when new token is invalid" in {
      val createToken = JObject(List(JField("create", JString("invalid")))) 
      createRaw(rootUID, createToken) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(BadRequest, _), _, Some(JString("Unexpected fields in token create object.")), _) => ok
      }}
    }.pendingUntilFixed
    "don't create if token is expired" in {
//      val createToken = TokenCreate(None, None, Some(true))
//      create(expiredUID, createToken) must whenDelivered { beLike {
//        case HttpResponse(HttpStatus(Unauthorized, _), _, Some(JString("The specified token has expired")), _) => ok
//      }}
      todo
    }
    "don't create if token cannot grant permissions" in {
//      val perms = standardAccountPerms("/")
//      val createToken = TokenCreate(Some(perms), None, None)
//      create(cust1UID, createToken) must whenDelivered { beLike {
//        case HttpResponse(HttpStatus(Unauthorized, _), _, Some(JString("The specified token may not grant the requested permissions")), _) => ok
//      }}
      todo
    }
    "delete token" in {
      delete(rootUID, cust2UID) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(jdl), _) => 
          val dl = jdl.deserialize[List[String]]
          dl must beLike {
            case List(cust2UID) => ok
          }
      }}
    }.pendingUntilFixed
    "return error if no permission to delete" in {
      delete(cust1UID, rootUID) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(jdl), _) => 
          val dl = jdl.deserialize[List[String]]
          dl must beLike {
            case List() => ok
          }
      }}
    }.pendingUntilFixed
    "return error if token to delete doesn't exist" in {
      delete(rootUID, "not-going-to-be-there") must whenDelivered { beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(jdl), _) => 
          val dl = jdl.deserialize[List[String]]
          dl must beLike {
            case List() => ok
          }
      }}
    }.pendingUntilFixed
    "with parent token update child token" in {
//      val updateToken = TokenUpdate(Set(otherPublicUID), Set(publicUID), None)
//      update(rootUID, cust1UID, updateToken) must whenDelivered { beLike {
//        case HttpResponse(HttpStatus(OK, _), _, Some(jdl), _) => 
//          val dl = jdl.deserialize[Token]
//          dl must beLike {
//            case Token(_, Some(rootUID), _, grants, _) =>
//              grants.contains(otherPublicUID) must beTrue 
//              grants.contains(publicUID) must beFalse 
//          }
//      }}
      todo
    }
    "as self update token" in {
//      val updateToken = TokenUpdate(Set(otherPublicUID), Set(publicUID), None)
//      update(cust1UID, cust1UID, updateToken) must whenDelivered { beLike {
//        case HttpResponse(HttpStatus(OK, _), _, Some(jdl), _) => 
//          val dl = jdl.deserialize[Token]
//          dl must beLike {
//            case Token(_, Some(rootUID), _, grants, _) =>
//              grants.contains(otherPublicUID) must beTrue 
//              grants.contains(publicUID) must beFalse 
//          }
//      }}
      todo
    }
    "reject updates to non-child token" in {
//      val updateToken = TokenUpdate(Set.empty, Set.empty, None)
//      update(cust1UID, rootUID, updateToken) must whenDelivered { beLike {
//        case HttpResponse(HttpStatus(BadRequest, _), _, Some(JString("Unable to update the specified token.")), _) => ok
//      }}
      todo
    }
  }
}
