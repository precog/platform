package com.precog.auth

import org.joda.time.DateTime

import com.weiglewilczek.slf4s.Logging

import akka.dispatch.{ ExecutionContext, Future }

import blueeyes.bkka.AkkaTypeClasses._
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._
import blueeyes.json._
import blueeyes.json.serialization.{ Extractor, Decomposer, IsoSerialization }
import blueeyes.json.serialization.IsoSerialization._
import blueeyes.json.serialization.DefaultSerialization.{ DateTimeDecomposer => _, DateTimeExtractor => _, _ }
import blueeyes.json.serialization.Extractor._

import scalaz.{ Validation, Success, Failure }
import scalaz.Scalaz._
import scalaz.Validation._

import shapeless._

import com.precog.common.json._
import com.precog.common.security._

import APIKeyRecord.SafeSerialization._
import Grant.SafeSerialization._

class SecurityServiceHandlers(val apiKeyManager: APIKeyManager[Future])(implicit executor: ExecutionContext) {
  object ReadAPIKeysHandler extends CustomHttpService[Future[JValue], APIKey => Future[HttpResponse[JValue]]] with Logging {
    val service = (request: HttpRequest[Future[JValue]]) => {
      Success { (authAPIKey: APIKey) => 
        apiKeyManagement.apiKeys(authAPIKey).map { apiKeys =>
          HttpResponse[JValue](OK, content = Some(apiKeys.map(r => WrappedAPIKey(r.apiKey, r.name, r.description)).serialize))
        }
      }
    }
    val metadata = None
  }

  object CreateAPIKeyHandler extends CustomHttpService[Future[JValue], APIKey => Future[HttpResponse[JValue]]] with Logging {
    val service: HttpRequest[Future[JValue]] => Validation[NotServed, APIKey => Future[HttpResponse[JValue]]] = (request: HttpRequest[Future[JValue]]) => {
      Success { (authAPIKey: APIKey) => 
        request.content.map { _.flatMap { jvalue =>
          logger.debug("Creating API key in response to request with auth API key " + authAPIKey + ":\n" + jvalue)
          jvalue.validated[NewAPIKeyRequest] match {
            case Success(request) =>
              if (request.grants.exists(_.isExpired(some(new DateTime()))))
                Future(HttpResponse[JValue](HttpStatus(BadRequest, "Error creating new API key."), content = Some(jobject(
                  jfield("error", "Unable to create API key with expired permission")
                ))))
              else
                apiKeyManagement.createAPIKey(authAPIKey, request).map { 
                  case Success(apiKey) => 
                    HttpResponse[JValue](OK, content = Some(WrappedAPIKey(apiKey, request.name, request.description).serialize))
                  case Failure(e) => 
                    logger.warn("Failed to create API key: " + e)
                    HttpResponse[JValue](HttpStatus(BadRequest, "Error creating new API key."), content = Some(jobject(
                      jfield("error", "Error creating new API key: " + e)
                    )))
                }
            case Failure(e) =>
              logger.warn("The API key request body \n" + jvalue + "\n was invalid: " + e)
              Future(HttpResponse[JValue](HttpStatus(BadRequest, "Invalid new API key request body."), content = Some(jobject(
                jfield("error", "Invalid new API key request body: " + e)
              ))))
            }
          }}.getOrElse {
          Future(HttpResponse[JValue](HttpStatus(BadRequest, "Missing new API key request body."), content = Some(JString("Missing new API key request body."))))
        }
      }
    }
    val metadata = None
  }

  object ReadAPIKeyDetailsHandler extends CustomHttpService[Future[JValue], APIKey => Future[HttpResponse[JValue]]] with Logging {
    val service = (request: HttpRequest[Future[JValue]]) => {
      Success { (authAPIKey: APIKey) => 
        request.parameters.get('apikey).map { apiKey =>
          apiKeyManagement.apiKeyDetails(apiKey).map { 
            case Some((apiKey, grants)) =>
              HttpResponse[JValue](OK, content = Some(v1.APIKeyDetails(apiKey.apiKey, apiKey.name, apiKey.description, grants).serialize))
            case None =>
              HttpResponse[JValue](HttpStatus(NotFound), content = Some(JString("Unable to find API key "+apiKey)))
          }
        }.getOrElse {
          Future(HttpResponse[JValue](HttpStatus(BadRequest, "Missing API key in request URI."),
                                      content = Some(JString("Missing API key in request URI."))))
        }
      }
    }

    val metadata = None
  }

  object CheckPermissionsHandler extends CustomHttpService[Future[JValue], APIKey => Future[HttpResponse[JValue]]] with Logging {
    val service = (request: HttpRequest[Future[JValue]]) => {
      Success { (authAPIKey: APIKey) => 
        request.content.map { apiKey =>
          apiKeyManagement.apiKeyDetails(apiKey).map { 
            case Some((apiKey, grants)) =>
              HttpResponse[JValue](OK, content = Some(v1.APIKeyDetails(apiKey.apiKey, apiKey.name, apiKey.description, grants).serialize))
            case None =>
              HttpResponse[JValue](HttpStatus(NotFound), content = Some(JString("Unable to find API key "+apiKey)))
          }
        }.getOrElse {
          Future(HttpResponse[JValue](HttpStatus(BadRequest, "Missing API key in request URI."),
                                      content = Some(JString("Missing API key in request URI."))))
        }
      }
    }
    val metadata = None
  }

  object ReadAPIKeyGrantsHandler extends CustomHttpService[Future[JValue], APIKey => Future[HttpResponse[JValue]]] with Logging {
    val service = (request: HttpRequest[Future[JValue]]) => {
      Success { (authAPIKey: APIKey) =>
        request.parameters.get('apikey).map { apiKey =>
          apiKeyManagement.apiKeyGrants(apiKey).map {
            case Some(grants) =>
              HttpResponse[JValue](OK, content = Some(grants.serialize))
            case None =>
              HttpResponse[JValue](HttpStatus(NotFound), content = Some(JString("The specified API key does not exist")))
          }
        }.getOrElse {
          Future(HttpResponse[JValue](HttpStatus(BadRequest, "Missing API key in request URI."), 
                                      content = Some(JString("Missing API key in request URI."))))
        }
      }
    }
    val metadata = None
  }

  object CreateAPIKeyGrantHandler extends CustomHttpService[Future[JValue], APIKey => Future[HttpResponse[JValue]]] with Logging {
    val service = (request: HttpRequest[Future[JValue]]) => {
      Success { (authAPIKey: APIKey) => 
        (for {
          apiKey <- request.parameters.get('apikey) 
          contentFuture <- request.content 
        } yield {
          contentFuture flatMap { jv =>
            jv.validated[v1.WrappedGrantId] match {
              case Success(v1.WrappedGrantId(grantId, _, _)) =>
                // TODO: Shouldn't this be using the auth API key somehow???
                apiKeyManagement.addAPIKeyGrant(apiKey, grantId).map {
                  case Success(_)   => HttpResponse[JValue](Created)
                  case Failure(msg) => HttpResponse[JValue](HttpStatus(BadRequest), content = Some(JString(msg)))
                }
              case Failure(e) =>
                logger.warn("Unable to parse grant ID from " + jv + ": " + e)
                Future(HttpResponse[JValue](HttpStatus(BadRequest, "Invalid add grant request body."), 
                                            content = Some(jobject(jfield("error", "Invalid add grant request body: " + e)))
                ))
            }
          }
        }).getOrElse {
          Future(HttpResponse[JValue](HttpStatus(BadRequest, "Missing API key in request URI."), 
                                      content = Some(JString("Missing API key in request URI."))))
        }
      }
    }

    val metadata = None
  }

  object DeleteAPIKeyGrantHandler extends CustomHttpService[Future[JValue], APIKey => Future[HttpResponse[JValue]]] with Logging {
    val service = (request: HttpRequest[Future[JValue]]) => {
      Success { (authAPIKey: APIKey) =>
        (for {
          apiKey <- request.parameters.get('apikey) 
          grantId <- request.parameters.get('grantId)
        } yield apiKeyManagement.removeAPIKeyGrant(apiKey, grantId).map {
          case Success(_) => HttpResponse[JValue](NoContent)
          case Failure(e) => 
            HttpResponse[JValue](HttpStatus(BadRequest, "Invalid remove grant request."), content = Some(jobject(
              jfield("error", "Invalid remove grant request: " + e)
            )))
        }).getOrElse {
          Future(HttpResponse[JValue](HttpStatus(BadRequest, "Missing API key in request URI."), content = Some(JString("Missing API key in request URI."))))
        }
      }
    }
    val metadata = None
  }

  object DeleteAPIKeyHandler extends CustomHttpService[Future[JValue], APIKey => Future[HttpResponse[JValue]]] with Logging {
    val service = (request: HttpRequest[Future[JValue]]) => {
      Success { (authAPIKey: APIKey) => 
        request.parameters.get('apikey).map { apiKey =>
          apiKeyManagement.deleteAPIKey(apiKey).map { 
            if(_) HttpResponse[JValue](HttpStatus(NoContent))
            else  HttpResponse[JValue](HttpStatus(NotFound), content = Some(JString("Unable to find API key "+apiKey)))
          }
        }.getOrElse {
          Future(HttpResponse[JValue](HttpStatus(BadRequest, "Missing API key in request URI."), content = Some(JString("Missing API key in request URI."))))
        }
      }
    }
    val metadata = None
  }

  object ReadGrantsHandler extends CustomHttpService[Future[JValue], APIKey => Future[HttpResponse[JValue]]] with Logging {
    val service = (request: HttpRequest[Future[JValue]]) => {
      Success { (authAPIKey: APIKey) =>
        apiKeyManagement.apiKeyGrants(authAPIKey).map {
          case Some(grants) =>
            HttpResponse[JValue](OK, content = Some(grants.serialize))
          case None =>
            HttpResponse[JValue](HttpStatus(NotFound), content = Some(JString("The specified API key does not exist")))
        }
      }
    }
    val metadata = None
  }

  object CreateGrantHandler extends CustomHttpService[Future[JValue], APIKey => Future[HttpResponse[JValue]]] with Logging {
    val service = (request: HttpRequest[Future[JValue]]) => {
      Success { (authAPIKey: APIKey) =>
        (for {
          content <- request.content 
        } yield {
          content.flatMap { _.validated[v1.NewGrantRequest] match {
            case Success(request) => 
              apiKeyManagement.createGrant(authAPIKey, request) map {
                case Success(grantId) => 
                  HttpResponse[JValue](OK, content = Some(v1.WrappedGrantId(grantId, None, None).serialize))
                case Failure(e) =>
                  HttpResponse[JValue](HttpStatus(BadRequest, "Error creating new grant."), content = Some(jobject(
                    jfield("error", "Error creating new grant: " + e)
                  )))
              }

            case Failure(e) =>
              Future(HttpResponse[JValue](HttpStatus(BadRequest, "Invalid new grant request body."), content = Some(jobject(
                jfield("error", "Invalid new grant request body: " + e)
              ))))
          }}
        }).getOrElse {
          Future(HttpResponse[JValue](HttpStatus(BadRequest, "Missing API key in request URI."), content = Some(JString("Missing API key in request URI."))))
        }
      }
    }
    val metadata = None
  }

  object ReadGrantDetailsHandler extends CustomHttpService[Future[JValue], APIKey => Future[HttpResponse[JValue]]] with Logging {
    val service = (request: HttpRequest[Future[JValue]]) => {
      Success { (authAPIKey: APIKey) => 
        (for {
          grantId <- request.parameters.get('grantId)
        } yield apiKeyManagement.grantDetails(grantId).map {
          case Success(grant) => HttpResponse[JValue](OK, content = Some(grant.serialize))
          case Failure(e) => 
            HttpResponse[JValue](HttpStatus(NotFound), content = Some(JString("Unable to find grant "+grantId)))
        }).getOrElse {
          Future(HttpResponse[JValue](HttpStatus(BadRequest, "Missing API key in request URI."), content = Some(JString("Missing API key in request URI."))))
        }
      }
    }
    val metadata = None
  }

  object ReadGrantChildrenHandler extends CustomHttpService[Future[JValue], APIKey => Future[HttpResponse[JValue]]] with Logging {
    val service = (request: HttpRequest[Future[JValue]]) => {
      Success { (authAPIKey: APIKey) =>
        (for {
          grantId <- request.parameters.get('grantId)
        } yield apiKeyManagement.grantChildren(grantId).map { grants =>
          HttpResponse[JValue](OK, content = Some(grants.serialize))
        }).getOrElse {
          Future(HttpResponse[JValue](HttpStatus(BadRequest, "Missing API key in request URI."), content = Some(JString("Missing API key in request URI."))))
        }
      }
    }
    val metadata = None
  }

  object CreateGrantChildHandler extends CustomHttpService[Future[JValue], APIKey => Future[HttpResponse[JValue]]] with Logging {
    val service = (request: HttpRequest[Future[JValue]]) => {
      Success { (authAPIKey: APIKey) =>
        (for {
          parentId <- request.parameters.get('grantId) 
          content <- request.content 
        } yield {
          content.flatMap { _.validated[v1.NewGrantRequest] match {
            case Success(request) => apiKeyManagement.addGrantChild(authAPIKey, parentId, request) map {
              case Success(grantId) => 
                HttpResponse[JValue](OK, content = Some(v1.WrappedGrantId(grantId, None, None).serialize))
              case Failure(e) => 
                HttpResponse[JValue](HttpStatus(BadRequest, "Error creating new child grant."), content = Some(jobject(
                  jfield("error", "Error creating new child grant: " + e)
                )))
            }
            case Failure(e) =>
              Future(HttpResponse[JValue](HttpStatus(BadRequest, "Invalid new child grant request body."), content = Some(jobject(
                jfield("error", "Invalid new child grant request body: " + e)
              ))))
          }}
        }).getOrElse {
          Future(HttpResponse[JValue](HttpStatus(BadRequest, "Missing API key in request URI."), content = Some(JString("Missing API key in request URI."))))
        }
      }
    }
    val metadata = None
  }

  object DeleteGrantHandler extends CustomHttpService[Future[JValue], APIKey => Future[HttpResponse[JValue]]] with Logging {
    val service = (request: HttpRequest[Future[JValue]]) => {
      Success { (authAPIKey: APIKey) =>
        request.parameters.get('grantId).map { grantId =>
          apiKeyManagement.deleteGrant(grantId).map {
            if(_) HttpResponse[JValue](HttpStatus(NoContent))
            else  HttpResponse[JValue](HttpStatus(NotFound), content = Some(JString("Unable to find grant "+grantId)))
        }}.getOrElse {
          Future(HttpResponse[JValue](HttpStatus(BadRequest, "Missing API key in request URI."), content = Some(JString("Missing API key in request URI."))))
        }
      }
    }
    val metadata = None
  }

  /*
  class APIKeyManagement(val apiKeyManager: APIKeyManager[Future])(implicit val execContext: ExecutionContext) {
    def apiKeys(apiKey: APIKey) : Future[Set[APIKeyRecord]] = {
      apiKeyManager.listAPIKeys.map(_.filter(_.issuerKey == apiKey).toSet)
    }

    def createAPIKey(creator: APIKey, request: NewAPIKeyRequest): Future[Validation[String, APIKey]] = {
      apiKeyManager.newAPIKeyWithGrants(request.name, request.description, creator, request.grants.toSet).map {
        case Some(t) => Success(t)
        case None => failure("Requestor lacks permissions to assign given grants to API key")
      }
    }

    def apiKeyDetails(apiKey: APIKey): Future[Option[(APIKeyRecord, Set[Grant])]] = {
      apiKeyManager.findAPIKey(apiKey).flatMap {
        case Some(apiKey) =>
          val grants = Future.sequence { apiKey.grants.map(grantId => apiKeyManager.findGrant(grantId)) }
          grants.map(_.flatten).map(Option(apiKey, _))
        case None => Future(None)
      }
    }

    def apiKeyGrants(apiKey: APIKey): Future[Option[Set[Grant]]] = {
      apiKeyManager.findAPIKey(apiKey).flatMap {
        case Some(apiKey) =>
          val grants = Future.sequence { apiKey.grants.map(grantId => apiKeyManager.findGrant(grantId)) }
          grants.map(grants => some(grants.flatten))
        case None => Future(some(Set.empty))
      }
    }
    
    def addAPIKeyGrant(apiKey: APIKey, grantId: GrantId): Future[Validation[String, Unit]] = {
      apiKeyManager.addGrants(apiKey, Set(grantId)).map(_.map(_ => ()).toSuccess("Unable to add grant "+grantId+" to API key "+apiKey))
    }
    
    def removeAPIKeyGrant(apiKey: APIKey, grantId: GrantId): Future[Validation[String, Unit]] = {
      apiKeyManager.removeGrants(apiKey, Set(grantId)).map(_.map(_ => ()).toSuccess("Unable to remove grant "+grantId+" from API key "+apiKey))
    } 

    def deleteAPIKey(apiKey: APIKey): Future[Boolean] = {
      apiKeyManager.deleteAPIKey(apiKey).map(_.isDefined)
    } 

    def createGrant(apiKey: APIKey, request: NewGrantRequest): Future[Validation[String, GrantId]] = {
      apiKeyManager.deriveGrant(request.name, request.description, apiKey, request.permissions, request.expirationDate).map(
        _.toSuccess("Requestor lacks permissions to create grant")
      )
    }
    
    def grantDetails(grantId: GrantId): Future[Validation[String, Grant]] = {
      apiKeyManager.findGrant(grantId).map(_.toSuccess("Unable to find grant "+grantId))
    }

    def grantChildren(grantId: GrantId): Future[Set[Grant]] = {
      apiKeyManager.findGrantChildren(grantId)
    }
    
    def addGrantChild(issuerKey: APIKey, parentId: GrantId, request: NewGrantRequest): Future[Validation[String, GrantId]] = {
      apiKeyManager.deriveSingleParentGrant(None, None, issuerKey, parentId, request.permissions, request.expirationDate).map(
        _.toSuccess("Requestor lacks permissions to create grant")
      )
    }

    def deleteGrant(grantId: GrantId): Future[Boolean] = {
      apiKeyManager.deleteGrant(grantId).map(!_.isEmpty)
    } 
  }
  */
  }

// type SecurityServiceHandlers
