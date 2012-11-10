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

import blueeyes.json.serialization.{ ValidatedExtraction, Extractor, Decomposer }
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.json.serialization.Extractor._

import blueeyes.json._

import blueeyes.util.Clock

trait TestAPIKeyService extends BlueEyesServiceSpecification with SecurityService with AkkaDefaults with MongoAPIKeyManagerComponent {

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

  override def apiKeyManagerFactory(config: Configuration) = TestAPIKeyManager.testAPIKeyManager[Future]

  lazy val authService = service.contentType[JValue](application/(MimeTypes.json))

  override implicit val defaultFutureTimeouts: FutureTimeouts = FutureTimeouts(0, Duration(1, "second"))

  val shortFutureTimeouts = FutureTimeouts(5, Duration(50, "millis"))
}

class SecurityServiceSpec extends TestAPIKeyService with FutureMatchers with Tags {
  import TestAPIKeyManager._

  def getAPIKeys(authAPIKey: String) = 
    authService.query("apiKey", authAPIKey).get("/apikeys/")
    
  def createAPIKey(authAPIKey: String, request: NewAPIKeyRequest) =
    createAPIKeyRaw(authAPIKey, request.serialize)

  def createAPIKeyRaw(authAPIKey: String, request: JValue) = 
    authService.query("apiKey", authAPIKey).post("/apikeys/")(request)

  def getAPIKeyDetails(authAPIKey: String, queryKey: String) = 
    authService.query("apiKey", authAPIKey).get("/apikeys/"+queryKey)

  def getAPIKeyGrants(authAPIKey: String, queryKey: String) = 
    authService.query("apiKey", authAPIKey).get("/apikeys/"+queryKey+"/grants/")

  def addAPIKeyGrant(authAPIKey: String, updateKey: String, grantId: WrappedGrantId) = 
    addAPIKeyGrantRaw(authAPIKey, updateKey, grantId.serialize)

  def addAPIKeyGrantRaw(authAPIKey: String, updateKey: String, grantId: JValue) = 
    authService.query("apiKey", authAPIKey).post("/apikeys/"+updateKey+"/grants/")(grantId)

  def createAPIKeyGrant(authAPIKey: String, permission: Permission) = 
    createAPIKeyGrantRaw(authAPIKey, permission.serialize)

  def createAPIKeyGrantRaw(authAPIKey: String, permission: JValue) = 
    authService.query("apiKey", authAPIKey).post("/grants/")(permission)

  def removeAPIKeyGrant(authAPIKey: String, updateKey: String, grantId: String) = 
    authService.query("apiKey", authAPIKey).delete("/apikeys/"+updateKey+"/grants/"+grantId)

  def getGrantDetails(authAPIKey: String, grantId: String) = 
    authService.query("apiKey", authAPIKey).get("/grants/"+grantId)

  def getGrantChildren(authAPIKey: String, grantId: String) = 
    authService.query("apiKey", authAPIKey).get("/grants/"+grantId+"/children/")

  def addGrantChild(authAPIKey: String, grantId: String, permission: Permission) =
    addGrantChildRaw(authAPIKey, grantId, permission.serialize)
    
  def addGrantChildRaw(authAPIKey: String, grantId: String, permission: JValue) = 
    authService.query("apiKey", authAPIKey).post("/grants/"+grantId+"/children/")(permission)
    
  def deleteGrant(authAPIKey: String, grantId: String) =
    authService.query("apiKey", authAPIKey).delete("/grants/"+grantId)

  def equalGrant(g1: Grant, g2: Grant) = (g1.gid == g2.gid) && (g1.permission == g2.permission)

  "Security service" should {
    "get existing API key" in {
      getAPIKeyDetails(rootUID, rootUID) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(jtd), _) =>
          val td = jtd.deserialize[APIKeyDetails]
          td must beLike {
            case APIKeyDetails(apiKey, grants) if (apiKey.tid == rootUID) && (grants sameElements grantList(0)) => ok
          }
      }}
    }

    "return error on get and API key not found" in {
      getAPIKeyDetails(rootUID, "not-gonna-find-it") must whenDelivered { beLike {
        case HttpResponse(HttpStatus(NotFound, _), _, Some(JString("Unable to find API key not-gonna-find-it")), _) => ok
      }}
    }
    
    "enumerate existing API keys" in {
      getAPIKeys(rootUID) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(jts), _) =>
          val ks = jts.deserialize[APIKeySet]
          ks.apiKeys must containTheSameElementsAs(apiKeys.values.filter(_.cid == rootUID).map(t => WrappedAPIKey(t.name, t.tid)).toSeq)
      }}
    }

    "create root API key with defaults" in {
      val request = NewAPIKeyRequest("root", grantList(0).map(_.permission))
      createAPIKey(rootUID, request) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(jid), _) => 
          val id = jid.deserialize[WrappedAPIKey]
          id.apiKey.length must be_>(0)
      }}
    }

    "create non-root API key with overrides" in {
      val request = NewAPIKeyRequest("non-root", grantList(1).map(_.permission))
      (for {
        HttpResponse(HttpStatus(OK, _), _, Some(jid), _)    <- createAPIKey(testUID, request)
        WrappedAPIKey("non-root", id) = jid.deserialize[WrappedAPIKey]
        HttpResponse(HttpStatus(OK, _), _, Some(jtd), _)    <- getAPIKeyDetails(rootUID, id)
        APIKeyDetails(apiKey, grants) = jtd.deserialize[APIKeyDetails]
        if (apiKey.tid == id)
      } yield
        grantList(1).map(_.permission).forall {
          case Permission(accessType, path, _, expiration) =>
            grants.map(_.permission).exists {
              case Permission(`accessType`, `path`, _, `expiration`) => true
              case _ => false
            }
        }) must whenDelivered { beTrue }
    }

    "don't create when new API key is invalid" in {
      val request = JObject(List(JField("create", JString("invalid")))) 
      createAPIKeyRaw(rootUID, request) must whenDelivered { beLike {
        case
          HttpResponse(HttpStatus(BadRequest, _), _,
            Some(JObject(elems)), _) if elems.contains("error") && elems("error").startsWith("Invalid new API key request body") => ok
      }}
    }

    "don't create if API key is expired" in {
      val request = NewAPIKeyRequest("expired", grantList(5).map(_.permission))
      createAPIKey(expiredUID, request) must whenDelivered { beLike {
        case
          HttpResponse(HttpStatus(BadRequest, _), _,
            Some(JObject(elems)), _) if elems.contains("error") && elems("error") == JString("Unable to create API key with expired permission") => ok
      }}
    }

    "don't create if API key cannot grant permissions" in {
      val request = NewAPIKeyRequest("unauthorized", grantList(0).map(_.permission))
      createAPIKey(cust1UID, request) must whenDelivered { beLike {
        case
          HttpResponse(HttpStatus(BadRequest, _), _,
            Some(JObject(elems)), _) if elems.contains("error") && elems("error").startsWith("Error creating new API key: Requestor lacks permissions to give grants to API key") => ok
      }}
    }

    "retrieve the grants associated with a given API key" in {
      getAPIKeyGrants(rootUID, rootUID) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(jgs), _) =>
          val gs = jgs.deserialize[GrantSet]
          gs must beLike {
            case GrantSet(grants) if grants sameElements grantList(0) => ok
          }
      }}
    }
    
    "add a specified grant to an API key" in {
      addAPIKeyGrant(cust1UID, testUID, WrappedGrantId("user1_read")) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(Created, _), _, None, _) => ok
      }}
    }

    "get existing grant" in {
      getGrantDetails(cust1UID, "user2_read") must whenDelivered { beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(jgd), _) =>
          val gd = jgd.deserialize[GrantDetails]
          gd must beLike {
            case GrantDetails(grant) if (grant.gid == grants("user2_read").gid) && (grant.permission == grants("user2_read").permission) => ok
          }
      }}
    }

    "report an error on get and grant not found" in {
      getGrantDetails(cust1UID, "not-gonna-find-it") must whenDelivered { beLike {
        case HttpResponse(HttpStatus(NotFound, _), _, Some(JString("Unable to find grant not-gonna-find-it")), _) => ok
      }}
    }

    "create a new grant derived from the grants of the authorization API key" in {
      createAPIKeyGrant(cust1UID, ReadPermission(Path("/user1/read-me"), cust1UID, None)) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(jid), _) =>
          val id = jid.deserialize[WrappedGrantId]
          id.grantId.length must be_>(0)
      }}
    }
    
    "don't create a new grant if it can't be derived from the authorization API keys grants" in {
      createAPIKeyGrant(cust1UID, ReadPermission(Path("/user2/secret"), cust1UID, None)) must whenDelivered { beLike {
        case
          HttpResponse(HttpStatus(BadRequest, _), _,
            Some(JObject(elems)), _) if elems.contains("error") => ok
      }}
    }
    
    "remove a specified grant from an API key" in {
      removeAPIKeyGrant(cust1UID, cust1UID, "user1_read") must whenDelivered { beLike {
        case HttpResponse(HttpStatus(NoContent, _), _, None, _) => ok
      }}
    }
    
    "retrieve the child grants of the given grant" in {
      getGrantChildren(rootUID, "root_read") must whenDelivered { beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(jgs), _) =>
          val gs = jgs.deserialize[GrantSet]
          gs must beLike {
            case GrantSet(grants) if grants.forall(g => rootReadChildren.exists(equalGrant(g, _))) => ok
          }
      }}
    }

    "add a child grant to the given grant" in {
      val permission = WritePermission(Path("/user1"), None)
      addGrantChild(rootUID, "root_read", permission) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(jid), _) =>
          val id = jid.deserialize[WrappedGrantId]
          id.grantId.length must be_>(0)
      }}
    }
    
    "delete a grant" in {
      val permission = WritePermission(Path("/user1"), None)
      (for {
        HttpResponse(HttpStatus(OK, _), _, Some(jid), _)      <- addGrantChild(rootUID, "root_read", permission)
        WrappedGrantId(id) = jid.deserialize[WrappedGrantId]
        HttpResponse(HttpStatus(OK, _), _, Some(jgs), _)      <- getGrantChildren(rootUID, "root_read")
        GrantSet(beforeDelete) = jgs.deserialize[GrantSet]
        if beforeDelete.exists(_.gid == id)
        HttpResponse(HttpStatus(NoContent, _), _, None, _)    <- deleteGrant(rootUID, id)
        HttpResponse(HttpStatus(OK, _), _, Some(jgs), _)      <- getGrantChildren(rootUID, "root_read")
        GrantSet(afterDelete) = jgs.deserialize[GrantSet]
      } yield !afterDelete.exists(_.gid == id)) must whenDelivered { beTrue }
    }
  }
}
