package com.precog.auth

import org.joda.time.DateTime

import org.specs2.mutable._

import akka.dispatch.{ Await, ExecutionContext, Future } 
import akka.util.Duration

import org.streum.configrity.Configuration

import scalaz.{Validation, Success, NonEmptyList}

import blueeyes.akka_testing._

import blueeyes.core.data._
import blueeyes.bkka._
import blueeyes.core.service._
import blueeyes.core.service.test.BlueEyesServiceSpecification
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.http.MimeTypes
import blueeyes.core.http.MimeTypes._

import blueeyes.json._
import blueeyes.json.serialization.{ ValidatedExtraction, Extractor, Decomposer }
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.json.serialization.Extractor._

import scalaz._

import com.precog.common.Path
import com.precog.common.security._
import com.precog.common.accounts._

import APIKeyRecord.SafeSerialization._
import Grant.SafeSerialization._


trait TestAPIKeyService extends BlueEyesServiceSpecification
  with SecurityService
  with AkkaDefaults { self =>

  import DefaultBijections._

  val asyncContext = defaultFutureDispatch
  implicit val executionContext = defaultFutureDispatch
  implicit val M = new FutureMonad(defaultFutureDispatch)

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
  
  val apiKeyManager = new InMemoryAPIKeyManager[Future]

  override def APIKeyManager(config: Configuration) = apiKeyManager 

  val mimeType = application/(MimeTypes.json)

  lazy val authService: HttpClient[JValue] =
    client.contentType[JValue](application/(MimeTypes.json))

  override implicit val defaultFutureTimeouts: FutureTimeouts = FutureTimeouts(0, Duration(1, "second"))

  val shortFutureTimeouts = FutureTimeouts(5, Duration(50, "millis"))
}

class SecurityServiceSpec extends TestAPIKeyService with FutureMatchers with Tags {

  val f: JValue => JValue = v => v
  
  val tc = new AsyncHttpTranscoder[JValue, JValue] {
    def apply(a: HttpRequest[JValue]): HttpRequest[JValue] = a
    def unapply(fb: Future[HttpResponse[JValue]]): Future[HttpResponse[JValue]] = fb
  }
  
  def getAPIKeys(authAPIKey: String) = 
    authService.query("apiKey", authAPIKey).get("/apikeys/")
    
  def createAPIKey(authAPIKey: String, request: NewAPIKeyRequest) =
    createAPIKeyRaw(authAPIKey, request.serialize)

  def createAPIKeyRaw(authAPIKey: String, request: JValue) = 
    authService.query("apiKey", authAPIKey).post("/apikeys/")(request)(f, tc)

  def getAPIKeyDetails(authAPIKey: String, queryKey: String) = 
    authService.query("apiKey", authAPIKey).get("/apikeys/"+queryKey)

  def getAPIKeyGrants(authAPIKey: String, queryKey: String) = 
    authService.query("apiKey", authAPIKey).get("/apikeys/"+queryKey+"/grants/")

  def addAPIKeyGrant(authAPIKey: String, updateKey: String, grantId: WrappedGrantId) = 
    addAPIKeyGrantRaw(authAPIKey, updateKey, grantId.serialize)

  def addAPIKeyGrantRaw(authAPIKey: String, updateKey: String, grantId: JValue) = 
    authService.query("apiKey", authAPIKey).
      post("/apikeys/"+updateKey+"/grants/")(grantId)(f, tc)

  def createAPIKeyGrant(authAPIKey: String, request: NewGrantRequest) = 
    createAPIKeyGrantRaw(authAPIKey, request.serialize)

  def createAPIKeyGrantRaw(authAPIKey: String, request: JValue) = 
    authService.query("apiKey", authAPIKey).
      post("/grants/")(request)(f, tc)

  def removeAPIKeyGrant(authAPIKey: String, updateKey: String, grantId: String) = 
    authService.query("apiKey", authAPIKey).delete("/apikeys/"+updateKey+"/grants/"+grantId)

  def getGrantDetails(authAPIKey: String, grantId: String) = 
    authService.query("apiKey", authAPIKey).get("/grants/"+grantId)

  def getGrantChildren(authAPIKey: String, grantId: String) = 
    authService.query("apiKey", authAPIKey).get("/grants/"+grantId+"/children/")

  def addGrantChild(authAPIKey: String, grantId: String, request: NewGrantRequest) =
    addGrantChildRaw(authAPIKey, grantId, request.serialize)
    
  def addGrantChildRaw(authAPIKey: String, grantId: String, request: JValue) = 
    authService.query("apiKey", authAPIKey).
      post("/grants/"+grantId+"/children/")(request)(f, tc)
    
  def deleteGrant(authAPIKey: String, grantId: String) =
    authService.query("apiKey", authAPIKey).delete("/grants/"+grantId)

  def equalGrant(g1: Grant, g2: Grant) = (g1.grantId == g2.grantId) && (g1.permissions == g2.permissions) && (g1.expirationDate == g2.expirationDate)
  
  def mkNewGrantRequest(grant: Grant) = grant match {
    case Grant(_, name, description, _, parentIds, permissions, expirationDate) =>
      NewGrantRequest(name, description, parentIds, permissions, expirationDate)
  }

  val to = Duration(30, "seconds")
  val rootAPIKey = Await.result(apiKeyManager.rootAPIKey, to)
  val rootGrantId = Await.result(apiKeyManager.rootGrantId, to)
  
  def standardGrant(accountId: AccountId) = mkNewGrantRequest(Await.result(apiKeyManager.newStandardAccountGrant(accountId, Path(accountId)), to))
  def standardPermissions(accountId: AccountId) = standardGrant(accountId).permissions
  
  val user1 = Await.result(apiKeyManager.newStandardAPIKeyRecord("user1", Path("user1"), Some("user1-key"), None), to)
  val user1Grant = Await.result(apiKeyManager.findGrant(user1.grants.head), to).get
  
  val user2 = Await.result(apiKeyManager.newStandardAPIKeyRecord("user2", Path("user2"), Some("user2-key"), None), to)
  val user2Grant = Await.result(apiKeyManager.findGrant(user2.grants.head), to).get
  
  val user3 = Await.result(apiKeyManager.newStandardAPIKeyRecord("user3", Path("user3"), Some("user3-key"), None), to)
  val user3Grant = Await.result(apiKeyManager.findGrant(user3.grants.head), to).get
  
  val user4 = Await.result(apiKeyManager.newStandardAPIKeyRecord("user4", Path("user4"), Some("user4-key"), None), to)
  val user4Grant = Await.result(apiKeyManager.findGrant(user4.grants.head), to).get
  val user4DerivedGrant = Await.result(
    apiKeyManager.newGrant(None, None, user4.apiKey, Set(user4Grant.grantId), standardPermissions("user4"), None), to) 
  
  val expiredGrant = Await.result(
    apiKeyManager.newGrant(None, None, user1.apiKey, Set(user1Grant.grantId), standardPermissions("user1"), Some(new DateTime().minusYears(1000))), to) 
  val expired = Await.result(apiKeyManager.newAPIKey(None, None, user1.apiKey, Set(expiredGrant.grantId)), to) 

  val allAPIKeys = Await.result(apiKeyManager.listAPIKeys(), to)
  val allGrants = Await.result(apiKeyManager.listGrants(), to)
  
  val rootGrants = {
    def mkPerm(p: (Path, Set[AccountId]) => Permission) = p(Path("/"), Set())
    
    Set(Grant(
      rootGrantId, Some("root-grant"), Some("The root grant"), None, Set(),
      Set(mkPerm(ReadPermission), mkPerm(ReducePermission), mkPerm(WritePermission), mkPerm(DeletePermission)),
      None
    ))
  }
  
  val rootGrantRequests = rootGrants map mkNewGrantRequest
  
  "Security service" should {
    "get existing API key" in {
      Await.result(getAPIKeyDetails(rootAPIKey, rootAPIKey), to) must beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(jtd), _) =>
          (jtd \ "apiKey").deserialize[APIKey] must_== rootAPIKey
          ((jtd \ "grants") --> classOf[JArray]).elements.map(elem => (elem \ "grantId").deserialize[GrantId]).toSet must_== rootGrants.map(_.grantId)
      }
    }

    "return error on get and API key not found" in {
      getAPIKeyDetails(rootAPIKey, "not-gonna-find-it") must whenDelivered { beLike {
        case HttpResponse(HttpStatus(NotFound, _), _, Some(JString("Unable to find API key not-gonna-find-it")), _) => ok
      }}
    }
    
    "enumerate existing API keys" in {
      getAPIKeys(rootAPIKey) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(jts), _) =>
          val ks = jts.deserialize[Set[WrappedAPIKey]]
          ks must containTheSameElementsAs(allAPIKeys.filter(_.issuerKey == rootAPIKey).map(r => WrappedAPIKey(r.apiKey, r.name, r.description)).toSeq)
      }}
    }

    "create root-like API key with defaults" in {
      val request = NewAPIKeyRequest(Some("root-like"), None, rootGrantRequests)
      createAPIKey(rootAPIKey, request) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(jid), _) => 
          val id = jid.deserialize[WrappedAPIKey]
          id.apiKey.length must be_>(0)
      }}
    }

    "create non-root API key with defaults" in {
      val request = NewAPIKeyRequest(Some("non-root-1"), None, Set(standardGrant("non-root-1")))
      createAPIKey(rootAPIKey, request) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(jid), _) => 
          val id = jid.deserialize[WrappedAPIKey]
          id.apiKey.length must be_>(0)
      }}
    }

    "create derived non-root API key" in {
      val request = NewAPIKeyRequest(Some("non-root-2"), None, Set(mkNewGrantRequest(user1Grant)))
      val result = for {
        HttpResponse(HttpStatus(OK, _), _, Some(jid), _)    <- createAPIKey(user1.apiKey, request)
        WrappedAPIKey(apiKey, _, _) = jid.deserialize[WrappedAPIKey]
        HttpResponse(HttpStatus(OK, _), _, Some(jtd), _)    <- getAPIKeyDetails(rootAPIKey, apiKey)
      } yield {
        val perms = ((jtd \\ "permissions") --> classOf[JArray]).elements map { elem => 
          elem.deserialize[Set[Permission]]
        }

        (jtd \ "apiKey").deserialize[APIKey] must_== rootAPIKey
        perms.flatten.toSet must haveTheSameElementsAs(user1Grant.permissions)
      }

      Await.result(result, to)
    }

    "don't create when new API key is invalid" in {
      val request = JObject(List(JField("create", JString("invalid")))) 
      createAPIKeyRaw(rootAPIKey, request) must whenDelivered { beLike {
        case
          HttpResponse(HttpStatus(BadRequest, _), _,
            Some(JObject(elems)), _) if elems.contains("error") => 
              elems("error") must beLike {
                case JString(msg) => msg must startWith("Invalid new API key request body") 
              }
      }}
    }

    "don't create if API key is expired" in {
      val request = NewAPIKeyRequest(Some("expired-2"), None, Set(mkNewGrantRequest(expiredGrant)))
      createAPIKey(expired.apiKey, request) must whenDelivered { beLike {
        case
          HttpResponse(HttpStatus(BadRequest, _), _, Some(JObject(elems)), _) if elems.contains("error") =>
            elems("error")  must beLike {
              case JString("Unable to create API key with expired permission") => ok
            }
      }}
    }

    "don't create if API key cannot grant permissions" in {
      val request = NewAPIKeyRequest(Some("unauthorized"), None, Set(mkNewGrantRequest(user1Grant)))
      createAPIKey(user2.apiKey, request) must whenDelivered { beLike {
        case
          HttpResponse(HttpStatus(BadRequest, _), _, Some(JObject(elems)), _) if elems.contains("error") =>
            elems("error") must beLike {
              case JString(msg) => msg must startWith("Error creating new API key: Requestor lacks permissions to assign given grants to API key") 
            }
      }}
    }

    "retrieve the grants associated with a given API key" in {
      getAPIKeyGrants(rootAPIKey, rootAPIKey) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(jgs), _) =>
          val gs = jgs.deserialize[Set[WrappedGrantId]]
          gs.map(_.grantId) must containTheSameElementsAs(Seq(rootGrantId))
      }}
    }
    
    "add a specified grant to an API key" in {
      addAPIKeyGrant(user1.apiKey, user2.apiKey, WrappedGrantId(user1Grant.grantId, None, None)) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(Created, _), _, None, _) => ok
      }}
    }

    "get existing grant" in {
      getGrantDetails(user1.apiKey, user1Grant.grantId) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(jgd), _) =>
          import Grant.Serialization._
          val g = jgd.deserialize[Grant]
          g must_== user1Grant.copy(issuerKey = None, parentIds = Set.empty[GrantId])
      }}
    }

    "report an error on get and grant not found" in {
      getGrantDetails(user1.apiKey, "not-gonna-find-it") must whenDelivered { beLike {
        case HttpResponse(HttpStatus(NotFound, _), _, Some(JString("Unable to find grant not-gonna-find-it")), _) => ok
      }}
    }

    "create a new grant derived from the grants of the authorization API key" in {
      createAPIKeyGrant(user1.apiKey, NewGrantRequest(None, None, Set.empty[GrantId], Set(ReadPermission(Path("/user1/read-me"), Set("user1"))), None)) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(jid), _) =>
          val id = jid.deserialize[WrappedGrantId]
          id.grantId.length must be_>(0)
      }}
    }
    
    "don't create a new grant if it can't be derived from the authorization API keys grants" in {
      createAPIKeyGrant(user1.apiKey, NewGrantRequest(None, None, Set.empty[GrantId], Set(ReadPermission(Path("/user2/secret"), Set("user2"))), None)) must whenDelivered { beLike {
        case
          HttpResponse(HttpStatus(BadRequest, _), _, Some(JObject(elems)), _) if elems.contains("error") =>
            elems("error") must beLike {
              case JString(msg) => msg must startWith("Error creating new grant: Requestor lacks permissions to create grant") 
            }
      }}
    }

    "remove a specified grant from an API key" in {
      removeAPIKeyGrant(user3.apiKey, user3.apiKey, user3Grant.grantId) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(NoContent, _), _, None, _) => ok
      }}
    }

    "retrieve the child grants of the given grant" in {
      getGrantChildren(user4.apiKey, user4Grant.grantId) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(jgs), _) =>
          import Grant.Serialization._
          val gs = jgs.deserialize[Set[Grant]]
          gs must_== Set(user4DerivedGrant.copy(issuerKey = None, parentIds = Set.empty[GrantId]))
      }}
    }

    "add a child grant to the given grant" in {
      addGrantChild(user1.apiKey, user1Grant.grantId, NewGrantRequest(None, None, Set.empty[GrantId], Set(ReadPermission(Path("/user1/secret"), Set("user1"))), None)) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(jid), _) =>
          val id = jid.deserialize[WrappedGrantId]
          id.grantId.length must be_>(0)
      }}
    }

    "delete a grant" in {
      import Grant.Serialization._
      (for {
        HttpResponse(HttpStatus(OK, _), _, Some(jid), _)      <- addGrantChild(user1.apiKey, user1Grant.grantId, NewGrantRequest(None, None, Set.empty[GrantId], Set(ReadPermission(Path("/user1/secret"), Set("user1"))), None))
        WrappedGrantId(id, _, _) = jid.deserialize[WrappedGrantId]
        HttpResponse(HttpStatus(OK, _), _, Some(jgs), _)      <- getGrantChildren(user1.apiKey, user1Grant.grantId)
        beforeDelete = jgs.deserialize[Set[Grant]]
        if beforeDelete.exists(_.grantId == id)
        HttpResponse(HttpStatus(NoContent, _), _, None, _)    <- deleteGrant(user1.apiKey, id)
        HttpResponse(HttpStatus(OK, _), _, Some(jgs), _)      <- getGrantChildren(user1.apiKey, user1Grant.grantId)
        afterDelete = jgs.deserialize[Set[Grant]]
      } yield !afterDelete.exists(_.grantId == id)) must whenDelivered { beTrue }
    }
  }
}
