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

import org.joda.time.DateTime

import com.weiglewilczek.slf4s.Logging

import akka.dispatch.{ ExecutionContext, Future, MessageDispatcher }

import blueeyes.bkka.AkkaTypeClasses._
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._
import blueeyes.json.{ JField, JObject, JString, JValue }
import blueeyes.json.serialization.{ ValidatedExtraction, Extractor, Decomposer }
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

case class APIKeyDetails(apiKey: APIKey, name: Option[String], description: Option[String], grants: Set[Grant])

object APIKeyDetails {
  implicit val apiKeyDetailsIso = Iso.hlist(APIKeyDetails.apply _, APIKeyDetails.unapply _)
  
  val schema = "apiKey" :: "name" :: "description" :: "grants" :: HNil
  
  implicit val (apiKeyDetailsDecomposer, apiKeyDetailsExtractor) = serialization[APIKeyDetails](schema)
}

case class WrappedGrantId(grantId: String, name: Option[String], description: Option[String])

object WrappedGrantId {
  implicit val wrappedGrantIdIso = Iso.hlist(WrappedGrantId.apply _, WrappedGrantId.unapply _)
  
  val schema = "grantId" :: "name" :: "description" :: HNil

  implicit val (wrappedGrantIdDecomposer, wrappedGrantIdExtractor) = serialization[WrappedGrantId](schema)
}

case class WrappedAPIKey(apiKey: APIKey, name: Option[String], description: Option[String])

object WrappedAPIKey {
  implicit val wrappedAPIKeyIso = Iso.hlist(WrappedAPIKey.apply _, WrappedAPIKey.unapply _)
  
  val schema = "apiKey" :: "name" :: "description" :: HNil

  implicit val (wrappedAPIKeyDecomposer, wrappedAPIKeyExtractor) = serialization[WrappedAPIKey](schema)
}

class GetAPIKeysHandler(apiKeyManagement: APIKeyManagement)(implicit dispatcher: MessageDispatcher) extends CustomHttpService[Future[JValue], APIKeyRecord => Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[Future[JValue]]) => {
    Success { (authAPIKey: APIKeyRecord) => 
      apiKeyManagement.apiKeys(authAPIKey.apiKey).map { apiKeys =>
        HttpResponse[JValue](OK, content = Some(apiKeys.map(r => WrappedAPIKey(r.apiKey, r.name, r.description)).serialize))
      }
    }
  }
  val metadata = None
}

class CreateAPIKeyHandler(apiKeyManagement: APIKeyManagement)(implicit dispatcher: MessageDispatcher) extends CustomHttpService[Future[JValue], APIKeyRecord => Future[HttpResponse[JValue]]] with Logging {
  val service: HttpRequest[Future[JValue]] => Validation[NotServed, APIKeyRecord => Future[HttpResponse[JValue]]] = (request: HttpRequest[Future[JValue]]) => {
    Success { (authAPIKey: APIKeyRecord) => 
      request.content.map { _.flatMap { jvalue =>
        logger.debug("Creating API key in response to request with auth API key " + authAPIKey + ":\n" + jvalue)
        jvalue.validated[NewAPIKeyRequest] match {
          case Success(request) =>
            if (request.grants.exists(_.isExpired(some(new DateTime()))))
              Future(HttpResponse[JValue](HttpStatus(BadRequest, "Error creating new API key."), content = Some(JObject(List(
                JField("error", "Unable to create API key with expired permission")
              )))))
            else
              apiKeyManagement.createAPIKey(authAPIKey, request).map { 
                case Success(apiKey) => 
                  HttpResponse[JValue](OK, content = Some(WrappedAPIKey(apiKey, request.name, request.description).serialize))
                case Failure(e) => 
                  logger.warn("Failed to create API key: " + e)
                  HttpResponse[JValue](HttpStatus(BadRequest, "Error creating new API key."), content = Some(JObject(List(
                    JField("error", "Error creating new API key: " + e)
                  ))))
              }
          case Failure(e) =>
            logger.warn("The API key request body \n" + jvalue + "\n was invalid: " + e)
            Future(HttpResponse[JValue](HttpStatus(BadRequest, "Invalid new API key request body."), content = Some(JObject(List(
              JField("error", "Invalid new API key request body: " + e)
            )))))
          }
        }}.getOrElse {
        Future(HttpResponse[JValue](HttpStatus(BadRequest, "Missing new API key request body."), content = Some(JString("Missing new API key request body."))))
      }
    }
  }
  val metadata = None
}

class GetAPIKeyDetailsHandler(apiKeyManagement: APIKeyManagement)(implicit dispatcher: MessageDispatcher) extends CustomHttpService[Future[JValue], APIKeyRecord => Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[Future[JValue]]) => {
    Success { (authAPIKey: APIKeyRecord) => 
      request.parameters.get('apikey).map { apiKey =>
        apiKeyManagement.apiKeyDetails(apiKey).map { 
          case Some((apiKey, grants)) =>
            HttpResponse[JValue](OK, content = Some(APIKeyDetails(apiKey.apiKey, apiKey.name, apiKey.description, grants).serialize))
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

class GetAPIKeyGrantsHandler(apiKeyManagement: APIKeyManagement)(implicit dispatcher: MessageDispatcher) extends CustomHttpService[Future[JValue], APIKeyRecord => Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[Future[JValue]]) => {
    Success { (authAPIKey: APIKeyRecord) => 
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

class AddAPIKeyGrantHandler(apiKeyManagement: APIKeyManagement)(implicit dispatcher: MessageDispatcher) extends CustomHttpService[Future[JValue], APIKeyRecord => Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[Future[JValue]]) => {
    Success { (authAPIKey: APIKeyRecord) => 
      (for {
        apiKey <- request.parameters.get('apikey) 
        contentFuture <- request.content 
      } yield {
        contentFuture flatMap { jv =>
          jv.validated[WrappedGrantId] match {
            case Success(WrappedGrantId(grantId, _, _)) =>
              // TODO: Shouldn't this be using the auth API key somehow???
              apiKeyManagement.addAPIKeyGrant(apiKey, grantId).map {
                case Success(_)   => HttpResponse[JValue](Created)
                case Failure(msg) => HttpResponse[JValue](HttpStatus(BadRequest), content = Some(JString(msg)))
              }
            case Failure(e) =>
              logger.warn("Unable to parse grant ID from " + jv + ": " + e)
              Future(HttpResponse[JValue](HttpStatus(BadRequest, "Invalid add grant request body."), 
                                          content = Some(JObject(List(JField("error", "Invalid add grant request body: " + e)
              )))))
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

class RemoveAPIKeyGrantHandler(apiKeyManagement: APIKeyManagement)(implicit dispatcher: MessageDispatcher) extends CustomHttpService[Future[JValue], APIKeyRecord => Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[Future[JValue]]) => {
    Success { (authAPIKey: APIKeyRecord) =>
      (for {
        apiKey <- request.parameters.get('apikey) 
        grantId <- request.parameters.get('grantId)
      } yield apiKeyManagement.removeAPIKeyGrant(apiKey, grantId).map {
        case Success(_) => HttpResponse[JValue](NoContent)
        case Failure(e) => 
          HttpResponse[JValue](HttpStatus(BadRequest, "Invalid remove grant request."), content = Some(JObject(List(
            JField("error", "Invalid remove grant request: " + e)
          ))))
      }).getOrElse {
        Future(HttpResponse[JValue](HttpStatus(BadRequest, "Missing API key in request URI."), content = Some(JString("Missing API key in request URI."))))
      }
    }
  }
  val metadata = None
}

class DeleteAPIKeyHandler(apiKeyManagement: APIKeyManagement)(implicit dispatcher: MessageDispatcher) extends CustomHttpService[Future[JValue], APIKeyRecord => Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[Future[JValue]]) => {
    Success { (authAPIKey: APIKeyRecord) => 
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

class CreateGrantHandler(apiKeyManagement: APIKeyManagement)(implicit dispatcher: MessageDispatcher) extends CustomHttpService[Future[JValue], APIKeyRecord => Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[Future[JValue]]) => {
    Success { (authAPIKey: APIKeyRecord) =>
      (for {
        content <- request.content 
      } yield {
        content.flatMap { _.validated[NewGrantRequest] match {
          case Success(request) => apiKeyManagement.createGrant(authAPIKey.apiKey, request) map {
            case Success(grantId) => 
              HttpResponse[JValue](OK, content = Some(WrappedGrantId(grantId, None, None).serialize))
            case Failure(e) =>
              HttpResponse[JValue](HttpStatus(BadRequest, "Error creating new grant."), content = Some(JObject(List(
                JField("error", "Error creating new grant: " + e)
              ))))
          }
          case Failure(e) =>
            Future(HttpResponse[JValue](HttpStatus(BadRequest, "Invalid new grant request body."), content = Some(JObject(List(
              JField("error", "Invalid new grant request body: " + e)
            )))))
        }}
      }).getOrElse {
        Future(HttpResponse[JValue](HttpStatus(BadRequest, "Missing API key in request URI."), content = Some(JString("Missing API key in request URI."))))
      }
    }
  }
  val metadata = None
}

class GetGrantDetailsHandler(apiKeyManagement: APIKeyManagement)(implicit dispatcher: MessageDispatcher) extends CustomHttpService[Future[JValue], APIKeyRecord => Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[Future[JValue]]) => {
    Success { (t: APIKeyRecord) => 
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

class GetGrantChildrenHandler(apiKeyManagement: APIKeyManagement)(implicit dispatcher: MessageDispatcher) extends CustomHttpService[Future[JValue], APIKeyRecord => Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[Future[JValue]]) => {
    Success { (authAPIKey: APIKeyRecord) =>
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

class AddGrantChildHandler(apiKeyManagement: APIKeyManagement)(implicit dispatcher: MessageDispatcher) extends CustomHttpService[Future[JValue], APIKeyRecord => Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[Future[JValue]]) => {
    Success { (authAPIKey: APIKeyRecord) =>
      (for {
        parentId <- request.parameters.get('grantId) 
        content <- request.content 
      } yield {
        content.flatMap { _.validated[NewGrantRequest] match {
          case Success(request) => apiKeyManagement.addGrantChild(authAPIKey.apiKey, parentId, request) map {
            case Success(grantId) => 
              HttpResponse[JValue](OK, content = Some(WrappedGrantId(grantId, None, None).serialize))
            case Failure(e) => 
              HttpResponse[JValue](HttpStatus(BadRequest, "Error creating new child grant."), content = Some(JObject(List(
                JField("error", "Error creating new child grant: " + e)
              ))))
          }
          case Failure(e) =>
            Future(HttpResponse[JValue](HttpStatus(BadRequest, "Invalid new child grant request body."), content = Some(JObject(List(
              JField("error", "Invalid new child grant request body: " + e)
            )))))
        }}
      }).getOrElse {
        Future(HttpResponse[JValue](HttpStatus(BadRequest, "Missing API key in request URI."), content = Some(JString("Missing API key in request URI."))))
      }
    }
  }
  val metadata = None
}

class DeleteGrantHandler(apiKeyManagement: APIKeyManagement)(implicit dispatcher: MessageDispatcher) extends CustomHttpService[Future[JValue], APIKeyRecord => Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[Future[JValue]]) => {
    Success { (authAPIKey: APIKeyRecord) =>
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

class APIKeyManagement(val apiKeyManager: APIKeyManager[Future])(implicit val execContext: ExecutionContext) {
  
  def apiKeys(apiKey: APIKey) : Future[Set[APIKeyRecord]] = {
    apiKeyManager.listAPIKeys.map(_.filter(_.issuerKey == apiKey).toSet)
  }

  def createAPIKey(requestor: APIKeyRecord, request: NewAPIKeyRequest): Future[Validation[String, APIKey]] = {
    apiKeyManager.newAPIKeyWithGrants(request.name, request.description, requestor.apiKey, request.grants.toSet).map {
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
  
  def addAPIKeyGrant(apiKey: APIKey, grantId: GrantID): Future[Validation[String, Unit]] = {
    apiKeyManager.addGrants(apiKey, Set(grantId)).map(_.map(_ => ()).toSuccess("Unable to add grant "+grantId+" to API key "+apiKey))
  }
  
  def removeAPIKeyGrant(apiKey: APIKey, grantId: GrantID): Future[Validation[String, Unit]] = {
    apiKeyManager.removeGrants(apiKey, Set(grantId)).map(_.map(_ => ()).toSuccess("Unable to remove grant "+grantId+" from API key "+apiKey))
  } 

  def deleteAPIKey(apiKey: APIKey): Future[Boolean] = {
    apiKeyManager.deleteAPIKey(apiKey).map(_.isDefined)
  } 

  def createGrant(apiKey: APIKey, request: NewGrantRequest): Future[Validation[String, GrantID]] = {
    apiKeyManager.deriveGrant(request.name, request.description, apiKey, request.permissions, request.expirationDate).map(
      _.toSuccess("Requestor lacks permissions to create grant")
    )
  }
  
  def grantDetails(grantId: GrantID): Future[Validation[String, Grant]] = {
    apiKeyManager.findGrant(grantId).map(_.toSuccess("Unable to find grant "+grantId))
  }

  def grantChildren(grantId: GrantID): Future[Set[Grant]] = {
    apiKeyManager.findGrantChildren(grantId)
  }
  
  def addGrantChild(issuerKey: APIKey, parentId: GrantID, request: NewGrantRequest): Future[Validation[String, GrantID]] = {
    apiKeyManager.deriveSingleParentGrant(None, None, issuerKey, parentId, request.permissions, request.expirationDate).map(
      _.toSuccess("Requestor lacks permissions to create grant")
    )
  }

  def deleteGrant(grantId: GrantID): Future[Boolean] = {
    apiKeyManager.deleteGrant(grantId).map(!_.isEmpty)
  } 

  def close() = apiKeyManager.close()
}
