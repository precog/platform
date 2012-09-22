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

  val asyncContext = defaultFutureDispatch

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

  override def tokenManagerFactory(config: Configuration) = TestTokenManager.testTokenManager[Future]

  lazy val authService = service.contentType[JValue](application/(MimeTypes.json)).path("/auth")

  override implicit val defaultFutureTimeouts: FutureTimeouts = FutureTimeouts(0, Duration(1, "second"))

  val shortFutureTimeouts = FutureTimeouts(5, Duration(50, "millis"))
}

class TokenServiceSpec extends TestTokenService with FutureMatchers with Tags {
  import TestTokenManager._

  def createToken(authAPIKey: String, request: NewTokenRequest) =
    createTokenRaw(authAPIKey, request.serialize)

  def createTokenRaw(authAPIKey: String, request: JValue) = 
    authService.query("apiKey", authAPIKey).post("/apikeys/")(request)

  def getTokenDetails(authAPIKey: String, queryKey: String) = 
    authService.query("apiKey", authAPIKey).get("/apikeys/"+queryKey)

  def getTokenGrants(authAPIKey: String, queryKey: String) = 
    authService.query("apiKey", authAPIKey).get("/apikeys/"+queryKey+"/grants/")

  def addTokenGrant(authAPIKey: String, updateKey: String, grantId: String) = 
    authService.query("apiKey", authAPIKey).post("/apikeys/"+updateKey+"/grants/")(JString(grantId) : JValue)

  def removeTokenGrant(authAPIKey: String, updateKey: String, grantId: String) = 
    authService.query("apiKey", authAPIKey).delete("/apikeys/"+updateKey+"/grants/"+grantId)

  def getGrantDetails(authAPIKey: String, grantId: String) = 
    authService.query("apiKey", authAPIKey).get("/grants/"+grantId)

  def getGrantChildren(authAPIKey: String, grantId: String) = 
    authService.query("apiKey", authAPIKey).get("/grants/"+grantId+"/children/")

  def addGrantChild(authAPIKey: String, grantId: String, permission: Permission) =
    addGrantChildRaw(authAPIKey, grantId, permission.serialize)
    
  def addGrantChildRaw(authAPIKey: String, grantId: String, permission: JValue) = 
    authService.query("apiKey", authAPIKey).post("/grants/"+grantId+"/children/")(permission)

  "Token service" should {
    "get existing token" in {
      getTokenDetails(rootUID, rootUID) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(jtd), _) =>
          val td = jtd.deserialize[TokenDetails]
          td must beLike {
            case TokenDetails(token, grants) if (token.tid == rootUID) && (grants sameElements grantList(0)) => ok
          }
      }}
    }

    "return error on get and token not found" in {
      getTokenDetails(rootUID, "not-gonna-find-it") must whenDelivered { beLike {
        case HttpResponse(HttpStatus(NotFound, _), _, Some(JString("Unable to find token not-gonna-find-it")), _) => ok
      }}
    }
    
    "create root token with defaults" in {
      val request = NewTokenRequest(grantList(0).map(_.permission))
      createToken(rootUID, request) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(jid), _) => 
          val id = jid.deserialize[String]
          id.length must be_>(0)
      }}
    }

    "create non-root token with overrides" in {
      val request = NewTokenRequest(grantList(1).map(_.permission))
      createToken(testUID, request) flatMap {
        case HttpResponse(HttpStatus(OK, _), _, Some(jid), _) => ok 
          val id = jid.deserialize[String]
          getTokenDetails(rootUID, id) map ((Some(id), _))
        case other => Future((None, other))
      } must whenDelivered { beLike {
        case (Some(id), HttpResponse(HttpStatus(OK, _), _, Some(jtd), _)) =>
          val td = jtd.deserialize[TokenDetails]
          td must beLike {
            case TokenDetails(token, grants) if (token.tid == id) && grantList(1).map(_.permission).forall {
              case Permission(accessType, path, _, expiration) =>
                grants.map(_.permission).exists {
                  case Permission(`accessType`, `path`, _, `expiration`) => true
                  case _ => false
                }} => ok
          }
      }}
    }

    "don't create when new token is invalid" in {
      val request = JObject(List(JField("create", JString("invalid")))) 
      createTokenRaw(rootUID, request) must whenDelivered { beLike {
        case
          HttpResponse(HttpStatus(BadRequest, _), _,
            Some(JObject(List(JField("error", JString(msg))))), _) if msg startsWith "Invalid new token request body" => ok
      }}
    }

    "don't create if token is expired" in {
      val request = NewTokenRequest(grantList(5).map(_.permission))
      createToken(expiredUID, request) must whenDelivered { beLike {
        case
          HttpResponse(HttpStatus(BadRequest, _), _,
            Some(JObject(List(JField("error", JString("Unable to create token with expired permission"))))), _) => ok
      }}
    }

    "don't create if token cannot grant permissions" in {
      val request = NewTokenRequest(grantList(0).map(_.permission))
      createToken(cust1UID, request) must whenDelivered { beLike {
        case
          HttpResponse(HttpStatus(BadRequest, _), _,
            Some(JObject(List(JField("error", JString(msg))))), _) if msg startsWith "Error creating new token: Unable to assign given grants to token" => ok
      }}
    }
    
    "retrieve the grants associated with a given token" in {
      getTokenGrants(rootUID, rootUID) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(jgs), _) =>
          val gs = jgs.deserialize[GrantSet]
          gs must beLike {
            case GrantSet(grants) if grants sameElements grantList(0) => ok
          }
      }}
    }
    
    "add a specified grant to a token" in {
      addTokenGrant(cust1UID, testUID, "user1_read") must whenDelivered { beLike {
        case HttpResponse(HttpStatus(Created, _), _, None, _) => ok
      }}
    }

    "get existing grant" in {
      getGrantDetails(cust1UID, "user2_read") must whenDelivered { beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(jgd), _) =>
          val gd = jgd.deserialize[GrantDetails]
          gd must beLike {
            case GrantDetails(grant) if (grant == grants("user2_read"))=> ok
          }
      }}
    }

    "report an error on get and grant not found" in {
      getGrantDetails(cust1UID, "not-gonna-find-it") must whenDelivered { beLike {
        case HttpResponse(HttpStatus(NotFound, _), _, Some(JString("Unable to find grant not-gonna-find-it")), _) => ok
      }}
    }

    "remove a specified grant from a token" in {
      removeTokenGrant(cust1UID, cust1UID, "user1_read") must whenDelivered { beLike {
        case HttpResponse(HttpStatus(NoContent, _), _, None, _) => ok
      }}
    }
    
    "retrieve the child grants of the given grant" in {
      getGrantChildren(rootUID, "root_read") must whenDelivered { beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(jgs), _) =>
          val gs = jgs.deserialize[GrantSet]
          gs must beLike {
            case GrantSet(grants) if grants sameElements rootReadChildren => ok
          }
      }}
    }
    
    "add a child grant to the given grant" in {
      val permission = WritePermission(Path("/user1"), None)
      addGrantChild(rootUID, "root_read", permission) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(jid), _) =>
          val id = jid.deserialize[String]
          id.length must be_>(0)
      }}
    }
  }
}
