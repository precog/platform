package com.precog.auth

import org.joda.time.DateTime
import org.joda.time.Instant

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
import blueeyes.json.serialization.{ Extractor, Decomposer }
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.json.serialization.Extractor._

import scalaz._

import com.precog.common.Path
import com.precog.common.security._
import com.precog.common.security.service.v1
import com.precog.common.accounts._


import DefaultBijections._

trait TestAPIKeyService extends BlueEyesServiceSpecification
  with SecurityService
  with AkkaDefaults { self =>

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

  def apiKeyManager: APIKeyManager[Future]
  val clock = blueeyes.util.Clock.System

  override def APIKeyManager(config: Configuration) = (apiKeyManager, Stoppable.Noop)
}

class SecurityServiceSpec extends TestAPIKeyService with FutureMatchers with Tags {
  import Permission._

  val authService: HttpClient[JValue] = client.contentType[JValue](application/(MimeTypes.json)).path("/security/v1/")
  val apiKeyManager = new InMemoryAPIKeyManager[Future](blueeyes.util.Clock.System)

  val tc = new AsyncHttpTranscoder[JValue, JValue] {
    def apply(a: HttpRequest[JValue]): HttpRequest[JValue] = a
    def unapply(fb: Future[HttpResponse[JValue]]): Future[HttpResponse[JValue]] = fb
  }

  def getAPIKeys(authAPIKey: String) =
    authService.query("apiKey", authAPIKey).get("/apikeys/")

  def createAPIKey(authAPIKey: String, request: v1.NewAPIKeyRequest) =
    createAPIKeyRaw(authAPIKey, request.serialize)

  def createAPIKeyRaw(authAPIKey: String, request: JValue) =
    authService.query("apiKey", authAPIKey).post("/apikeys/")(request)(identity[JValue], tc)

  def getAPIKeyDetails(queryKey: String) =
    authService.get("/apikeys/"+queryKey)

  def getFullAPIKeyDetails(queryKey: String, rootKey: APIKey) =
    authService.query("authkey", rootKey).get("/apikeys/"+queryKey)

  def getAPIKeyGrants(queryKey: String) =
    authService.get("/apikeys/"+queryKey+"/grants/")

  def addAPIKeyGrant(authAPIKey: String, updateKey: String, grantId: GrantId) =
    addAPIKeyGrantRaw(authAPIKey, updateKey, JObject("grantId" -> JString(grantId)))

  def addAPIKeyGrantRaw(authAPIKey: String, updateKey: String, grantId: JValue) =
    authService.query("apiKey", authAPIKey).
      post("/apikeys/"+updateKey+"/grants/")(grantId)(identity[JValue], tc)

  def createAPIKeyGrant(authAPIKey: String, request: v1.NewGrantRequest) =
    createAPIKeyGrantRaw(authAPIKey, request.serialize)

  def createAPIKeyGrantRaw(authAPIKey: String, request: JValue) =
    authService.query("apiKey", authAPIKey).
      post("/grants/")(request)(identity[JValue], tc)

  def removeAPIKeyGrant(authAPIKey: String, updateKey: String, grantId: GrantId) =
    authService.delete("/apikeys/"+updateKey+"/grants/"+grantId)

  def getGrantDetails(authAPIKey: String, grantId: String) =
    authService.query("apiKey", authAPIKey).get("/grants/"+grantId)

  def getGrantChildren(authAPIKey: String, grantId: String) =
    authService.query("apiKey", authAPIKey).get("/grants/"+grantId+"/children/")

  def addGrantChild(authAPIKey: String, grantId: String, request: v1.NewGrantRequest) =
    addGrantChildRaw(authAPIKey, grantId, request.serialize)

  def addGrantChildRaw(authAPIKey: String, grantId: String, request: JValue) =
    authService.query("apiKey", authAPIKey).
      post("/grants/"+grantId+"/children/")(request)(identity[JValue], tc)

  def deleteGrant(authAPIKey: String, grantId: String) =
    authService.query("apiKey", authAPIKey).delete("/grants/"+grantId)

  def getPermissions(authAPIKey: String, path: Path) =
    authService.query("apiKey", authAPIKey).get("/permissions/fs/" + path)

  def equalGrant(g1: Grant, g2: Grant) = (g1.grantId == g2.grantId) && (g1.permissions == g2.permissions) && (g1.expirationDate == g2.expirationDate)

  def mkNewGrantRequest(grant: Grant) = grant match {
    case Grant(_, name, description, _, parentIds, permissions, _, expirationDate) =>
      v1.NewGrantRequest(name, description, parentIds, permissions, expirationDate)
  }

  def standardGrant(accountId: AccountId) = mkNewGrantRequest(Await.result(apiKeyManager.newStandardAccountGrant(accountId, Path(accountId)), to))
  def standardPermissions(accountId: AccountId) = standardGrant(accountId).permissions

  val to = Duration(3, "seconds")
  val rootAPIKey = Await.result(apiKeyManager.rootAPIKey, to)
  val rootGrantId = Await.result(apiKeyManager.rootGrantId, to)

  val user1 = Await.result(apiKeyManager.newStandardAPIKeyRecord("user1", Some("user1-key"), None), to)
  val user1Grant = Await.result(apiKeyManager.findGrant(user1.grants.head), to).get

  val user2 = Await.result(apiKeyManager.newStandardAPIKeyRecord("user2", Some("user2-key"), None), to)
  val user2Grant = Await.result(apiKeyManager.findGrant(user2.grants.head), to).get

  val user3 = Await.result(apiKeyManager.newStandardAPIKeyRecord("user3", Some("user3-key"), None), to)
  val user3Grant = Await.result(apiKeyManager.findGrant(user3.grants.head), to).get

  val user4 = Await.result(apiKeyManager.newStandardAPIKeyRecord("user4", Some("user4-key"), None), to)
  val user4Grant = Await.result(apiKeyManager.findGrant(user4.grants.head), to).get
  val user4DerivedGrant = Await.result(
    apiKeyManager.createGrant(None, None, user4.apiKey, Set(user4Grant.grantId), standardPermissions("user4"), None), to)

  val user5 = Await.result(apiKeyManager.createAPIKey(Some("user5-key"), None, user1.apiKey, Set.empty), to)
  val user6 = Await.result(apiKeyManager.createAPIKey(Some("user6-key"), None, user1.apiKey, Set.empty), to)

  val expiredGrant = Await.result(
    apiKeyManager.createGrant(None, None, user1.apiKey, Set(user1Grant.grantId), standardPermissions("user1"), Some(new DateTime().minusYears(1000))), to)
  val expired = Await.result(apiKeyManager.createAPIKey(None, None, user1.apiKey, Set(expiredGrant.grantId)), to)

  val allAPIKeys = Await.result(apiKeyManager.listAPIKeys(), to)
  val allGrants = Await.result(apiKeyManager.listGrants(), to)

  val rootPermissions = Set[Permission](
    WritePermission(Path.Root, WriteAsAny), 
    ReadPermission(Path.Root, WrittenByAny),  
    DeletePermission(Path.Root, WrittenByAny)
  )

  val rootGrants = {
    Set(Grant(
      rootGrantId, Some("root-grant"), Some("The root grant"), rootAPIKey, Set(),
      rootPermissions,
      new Instant(0L),
      None
    ))
  }

  val rootGrantRequests = rootGrants map mkNewGrantRequest

  "Security service" should {
    "get existing API key" in {
      getAPIKeyDetails(user1.apiKey) must awaited(to) {
        beLike {
          case HttpResponse(HttpStatus(OK, _), _, Some(jtd), _) =>
            jtd.validated[v1.APIKeyDetails] must beLike {
              case Success(details) =>
                details.apiKey must_== user1.apiKey
                details.name must_== user1.name
                details.description must_== user1.description
                details.grants.map(_.grantId) must_== user1.grants
            }
        }
      }
    }

    "get existing API key without issuer" in {
      getAPIKeyDetails(user5.apiKey) must awaited(to) {
        beLike {
          case HttpResponse(HttpStatus(OK, _), _, Some(jtd), _) =>
            jtd.validated[v1.APIKeyDetails] must beLike {
              case Success(details) =>
                details.apiKey must_== user5.apiKey
                details.name must_== user5.name
                details.description must_== user5.description
                details.grants.map(_.grantId) must_== user5.grants
                details.issuerChain must beEmpty
            }
        }
      }
    }
    "get existing API key with issuer" in {
      getFullAPIKeyDetails(user5.apiKey, rootAPIKey) must awaited(to) {
        beLike {
          case HttpResponse(HttpStatus(OK, _), _, Some(jtd), _) =>
            jtd.validated[v1.APIKeyDetails] must beLike {
              case Success(details) =>
                details.apiKey must_== user5.apiKey
                details.name must_== user5.name
                details.description must_== user5.description
                details.grants.map(_.grantId) must_== user5.grants
                details.issuerChain mustEqual List(user1.apiKey, rootAPIKey)
            }
        }
      }
    }

    "return error on get and API key not found" in {
      getAPIKeyDetails("not-gonna-find-it") must awaited(to) { beLike {
        case HttpResponse(HttpStatus(NotFound, _), _, Some(JString("Unable to find API key not-gonna-find-it")), _) => ok
      }}
    }

    "enumerate existing API keys" in {
      getAPIKeys(user1.apiKey) must awaited(to) { beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(jts), _) =>
          val ks = jts.deserialize[Set[v1.APIKeyDetails]]
          ks must haveSize(3)
          ks.map(_.apiKey) must containAllOf(List(user5.apiKey, user6.apiKey))
          // ks.map(_.apiKey) must haveTheSameElementsAs(allAPIKeys.filter(_.issuerKey.exists(_ == user1.apiKey)).map(_.apiKey))
      }}
    }

    "create root-like API key with defaults" in {
      val request = v1.NewAPIKeyRequest(Some("root-like"), None, rootGrantRequests)
      createAPIKey(rootAPIKey, request) must awaited(to) { beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(jid), _) =>
          val id = jid.deserialize[v1.APIKeyDetails]
          id.apiKey.length must be_>(0)
      }}
    }

    "create non-root API key with defaults" in {
      val request = v1.NewAPIKeyRequest(Some("non-root-1"), None, Set(standardGrant("non-root-1")))
      createAPIKey(rootAPIKey, request) must awaited(to) { beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(jid), _) =>
          val id = jid.deserialize[v1.APIKeyDetails]
          id.apiKey.length must be_>(0)
      }}
    }

    "create derived non-root API key" in {
      val request = v1.NewAPIKeyRequest(Some("non-root-2"), None, Set(mkNewGrantRequest(user1Grant)))
      val result = for {
        HttpResponse(HttpStatus(OK, _), _, Some(jid), _)    <- createAPIKey(user1.apiKey, request)
        apiKey = jid.deserialize[v1.APIKeyDetails].apiKey
        HttpResponse(HttpStatus(OK, _), _, Some(jtd), _)    <- getAPIKeyDetails(apiKey)
      } yield jtd.deserialize[v1.APIKeyDetails]

      result must awaited(to) {
        beLike {
          case v1.APIKeyDetails(apiKey, name, description, grants, _) =>
            grants.flatMap(_.permissions) must haveTheSameElementsAs(user1Grant.permissions)
        }
      }
    }

    "don't create when new API key is invalid" in {
      val request = JObject(List(JField("create", JString("invalid"))))
      createAPIKeyRaw(rootAPIKey, request) must awaited(to) { beLike {
        case HttpResponse(HttpStatus(BadRequest, msg), _, Some(JObject(elems)), _) if elems.contains("error") =>
          msg must startWith("Invalid new API key request body")
      }}
    }

    "don't create if API key is expired" in {
      val request = v1.NewAPIKeyRequest(Some("expired-2"), None, Set(mkNewGrantRequest(expiredGrant)))
      createAPIKey(expired.apiKey, request) must awaited(to) { beLike {
        case HttpResponse(HttpStatus(BadRequest, _), _, Some(JObject(elems)), _) if elems.contains("error") =>
          elems("error")  must beLike {
            case JString("Unable to create API key with expired permission") => ok
          }
      }}
    }

    "don't create if API key cannot grant permissions" in {
      val request = v1.NewAPIKeyRequest(Some("unauthorized"), None, Set(mkNewGrantRequest(user1Grant)))
      createAPIKey(user2.apiKey, request) must awaited(to) { beLike {
        case HttpResponse(HttpStatus(BadRequest, _), _, Some(JObject(elems)), _) if elems.contains("error") =>
          elems("error") must beLike {
            case JString(msg) => msg must startWith("Requestor lacks permission to assign given grants to API key")
          }
      }}
    }

    "retrieve the grants associated with a given API key" in {
      getAPIKeyGrants(user1.apiKey) must awaited(to) { beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(jgs), _) =>
          val gs = jgs.deserialize[Set[v1.GrantDetails]]
          gs.map(_.grantId) must haveTheSameElementsAs(Seq(user1Grant.grantId))
      }}
    }

    "add a specified grant to an API key" in {
      addAPIKeyGrant(user1.apiKey, user2.apiKey, user1Grant.grantId) must awaited(to) { beLike {
        case HttpResponse(HttpStatus(Created, _), _, None, _) => ok
      }}
    }

    "get existing grant" in {
      getGrantDetails(user1.apiKey, user1Grant.grantId) must awaited(to) { beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(jgd), _) =>
          jgd.deserialize[v1.GrantDetails] must beLike {
            case v1.GrantDetails(gid, gname, gdesc, perms, _, exp) =>
              gid must_== user1Grant.grantId
              gname must_== user1Grant.name
              gdesc must_== user1Grant.description
              perms must_== user1Grant.permissions
              exp must_== user1Grant.expirationDate
          }
      }}
    }

    "report an error on get and grant not found" in {
      getGrantDetails(user1.apiKey, "not-gonna-find-it") must awaited(to) { beLike {
        case HttpResponse(HttpStatus(NotFound, _), _, Some(JString("Unable to find grant not-gonna-find-it")), _) => ok
      }}
    }

    "create a new grant derived from the grants of the authorization API key" in {
      createAPIKeyGrant(user1.apiKey, v1.NewGrantRequest(None, None, Set.empty[GrantId], Set(ReadPermission(Path("/user1/read-me"), WrittenByAccount("user1"))), None)) must awaited(to) { beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(jid), _) =>
          val id = jid.deserialize[v1.GrantDetails]
          id.grantId.length must be_>(0)
      }}
    }

    "don't create a new grant if it can't be derived from the authorization API keys grants" in {
      createAPIKeyGrant(user1.apiKey, v1.NewGrantRequest(None, None, Set.empty[GrantId], Set(ReadPermission(Path("/user2/secret"), WrittenByAccount("user2"))), None)) must awaited(to) { beLike {
        case
          HttpResponse(HttpStatus(BadRequest, _), _, Some(JObject(elems)), _) if elems.contains("error") =>
            elems("error") must beLike {
              case JString(msg) => msg must startWith("Requestor lacks permissions to create grant")
            }
      }}
    }

    "remove a specified grant from an API key" in {
      removeAPIKeyGrant(user3.apiKey, user3.apiKey, user3Grant.grantId) must awaited(to) { beLike {
        case HttpResponse(HttpStatus(NoContent, _), _, None, _) => ok
      }}
    }

    "retrieve the child grants of the given grant" in {
      getGrantChildren(user4.apiKey, user4Grant.grantId) must awaited(to) { beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(jgs), _) =>
          val gs = jgs.deserialize[Set[v1.GrantDetails]]
          gs.map(_.grantId) must_== Set(user4DerivedGrant.grantId)
      }}
    }

    "add a child grant to the given grant" in {
      addGrantChild(user1.apiKey, user1Grant.grantId, v1.NewGrantRequest(None, None, Set.empty[GrantId], Set(ReadPermission(Path("/user1/secret"), WrittenByAccount("user1"))), None)) must awaited(to) { beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(jid), _) =>
          val id = jid.deserialize[v1.GrantDetails]
          id.grantId.length must be_>(0)
      }}
    }

    "delete a grant" in {
      (for {
        HttpResponse(HttpStatus(OK, _), _, Some(jid), _)      <- addGrantChild(user1.apiKey, user1Grant.grantId, v1.NewGrantRequest(None, None, Set.empty[GrantId], Set(ReadPermission(Path("/user1/secret"), WrittenByAccount("user1"))), None))
        details                                               = jid.deserialize[v1.GrantDetails]
        HttpResponse(HttpStatus(OK, _), _, Some(jgs), _)      <- getGrantChildren(user1.apiKey, user1Grant.grantId)
        beforeDelete                                          = jgs.deserialize[Set[v1.GrantDetails]]
        if beforeDelete.exists(_.grantId == details.grantId)
        HttpResponse(HttpStatus(NoContent, _), _, None, _)    <- deleteGrant(user1.apiKey, details.grantId)
        HttpResponse(HttpStatus(OK, _), _, Some(jgs), _)      <- getGrantChildren(user1.apiKey, user1Grant.grantId)
        afterDelete = jgs.deserialize[Set[v1.GrantDetails]]
      } yield !afterDelete.exists(_.grantId == details.grantId)) must awaited(to) { beTrue }
    }

    "retrieve permissions for a given path owned by user" in {
      getPermissions(user1.apiKey, Path("/user1")) must awaited(to) { beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(jperms), _) =>
          val perms = jperms.deserialize[Set[Permission]] map Permission.accessType
          val types = Set("read", "write", "delete")
          perms must_== types
      }}
    }

    "retrieve permissions for a path we don't own" in {
      val permsM = for {
        HttpResponse(HttpStatus(OK, _), _, Some(jid), _) <- createAPIKeyGrant(user1.apiKey, v1.NewGrantRequest(None, None, Set.empty, Set(ReadPermission(Path("/user1/public"), WrittenByAccount("user1"))), None))
        _ <- addAPIKeyGrant(user1.apiKey, user4.apiKey, jid.deserialize[v1.GrantDetails].grantId)
        HttpResponse(HttpStatus(OK, _), _, Some(jperms), _) <- getPermissions(user4.apiKey, Path("/user1/public"))
      } yield jperms.deserialize[Set[Permission]]

      permsM must awaited(to) { haveOneElementLike {
        case ReadPermission(_, _) => ok
      }}
    }
  }
}
