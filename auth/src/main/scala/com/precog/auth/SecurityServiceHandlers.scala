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

import akka.dispatch.{ ExecutionContext, Future, MessageDispatcher }
import akka.util.Timeout

import blueeyes.bkka.AkkaTypeClasses._
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._
import blueeyes.json.JsonAST._
import blueeyes.json.serialization.{ ValidatedExtraction, Extractor, Decomposer }
import blueeyes.json.serialization.DefaultSerialization.{ DateTimeDecomposer => _, DateTimeExtractor => _, _ }
import blueeyes.json.serialization.Extractor._

import com.weiglewilczek.slf4s.Logging

import scalaz.{ Applicative, Validation, Success, Failure }
import scalaz.Scalaz._
import scalaz.Validation._

import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

class GetAPIKeysHandler(apiKeyManagement: APIKeyManagement)(implicit dispatcher: MessageDispatcher) extends CustomHttpService[Future[JValue], APIKeyRecord => Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[Future[JValue]]) => {
    Success { (authAPIKey: APIKeyRecord) => 
      apiKeyManagement.apiKeys(authAPIKey.tid).map { apiKeys =>
        HttpResponse[JValue](OK, content = Some(APIKeySet(apiKeys.map(t => WrappedAPIKey(t.name, t.tid))).serialize))
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
          case Success(r) =>
            if (r.grants.exists(_.isExpired(new DateTime())))
              Future(HttpResponse[JValue](HttpStatus(BadRequest, "Error creating new API key."), content = Some(JObject(List(
                JField("error", "Unable to create API key with expired permission")
              )))))
            else
              apiKeyManagement.createAPIKey(authAPIKey, r).map { 
                case Success(apiKeyRecord) => 
                  HttpResponse[JValue](OK, content = Some(WrappedAPIKey(apiKeyRecord.name, apiKeyRecord.tid).serialize))
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
      request.parameters.get('apikey).map { tid =>
        apiKeyManagement.apiKeyDetails(tid).map { 
          case Some((apiKey, grants)) =>
            HttpResponse[JValue](OK, content = Some(APIKeyDetails(apiKey, grants).serialize))
          case None =>
            HttpResponse[JValue](HttpStatus(NotFound), content = Some(JString("Unable to find API key "+tid)))
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
      request.parameters.get('apikey).map { tid =>
        apiKeyManagement.apiKeyGrants(tid).map {
          case Some(grants) =>
            HttpResponse[JValue](OK, content = Some(GrantSet(grants).serialize))
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
        tid <- request.parameters.get('apikey) 
        contentFuture <- request.content 
      } yield {
        contentFuture flatMap { jv =>
          jv.validated[WrappedGrantId] match {
            case Success(WrappedGrantId(gid)) =>
              // TODO: Shouldn't this be using the auth API key somehow???
              apiKeyManagement.addAPIKeyGrant(tid, gid).map {
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
        tid <- request.parameters.get('apikey) 
        gid <- request.parameters.get('grantId)
      } yield apiKeyManagement.removeAPIKeyGrant(tid, gid).map {
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
      request.parameters.get('apikey).map { tid =>
        apiKeyManagement.deleteAPIKey(tid).map { 
          if(_) HttpResponse[JValue](HttpStatus(NoContent))
          else  HttpResponse[JValue](HttpStatus(NotFound), content = Some(JString("Unable to find API key "+tid)))
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
        content.flatMap { _.validated[Permission] match {
          case Success(permission) => apiKeyManagement.createGrant(authAPIKey.tid, permission) map {
            case Success(grant) => 
              HttpResponse[JValue](OK, content = Some(WrappedGrantId(grant.gid).serialize))
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
        gid <- request.parameters.get('grantId)
      } yield apiKeyManagement.grantDetails(gid).map {
        case Success(grant) => HttpResponse[JValue](OK, content = Some(GrantDetails(grant).serialize))
        case Failure(e) => 
          HttpResponse[JValue](HttpStatus(NotFound), content = Some(JString("Unable to find grant "+gid)))
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
        gid <- request.parameters.get('grantId)
      } yield apiKeyManagement.grantChildren(gid).map { grants =>
        HttpResponse[JValue](OK, content = Some(GrantSet(grants).serialize))
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
        gid <- request.parameters.get('grantId) 
        content <- request.content 
      } yield {
        content.flatMap { _.validated[Permission] match {
          case Success(permission) => apiKeyManagement.addGrantChild(gid, permission) map {
            case Success(grant) => 
              HttpResponse[JValue](OK, content = Some(WrappedGrantId(grant.gid).serialize))
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
      request.parameters.get('grantId).map { gid =>
        apiKeyManagement.deleteGrant(gid).map {
          if(_) HttpResponse[JValue](HttpStatus(NoContent))
          else  HttpResponse[JValue](HttpStatus(NotFound), content = Some(JString("Unable to find grant "+gid)))
      }}.getOrElse {
        Future(HttpResponse[JValue](HttpStatus(BadRequest, "Missing API key in request URI."), content = Some(JString("Missing API key in request URI."))))
      }
    }
  }
  val metadata = None
}

class APIKeyManagement(val apiKeyManager: APIKeyManager[Future])(implicit val execContext: ExecutionContext)
  extends APIKeyManagerAccessControl[Future](apiKeyManager) {
  
  def apiKeys(tid: APIKey) : Future[Set[APIKeyRecord]] = {
    apiKeyManager.listAPIKeys.map(_.filter(_.cid == tid).toSet)
  }

  def createAPIKey(requestor: APIKeyRecord, request: NewAPIKeyRequest): Future[Validation[String, APIKeyRecord]] = {
    apiKeyManager.newAPIKey(request.apiKeyName, requestor.tid, Set.empty).flatMap { apiKeyRecord =>
      val tid = apiKeyRecord.tid
      
      mayGrant(requestor.tid, request.grants.toSet).flatMap { mayGrant =>
        if (mayGrant) {
          val grantIds = Future.sequence(request.grants.map(apiKeyManager.newGrant(None, _))).map(_.map(_.gid).toSet)
          val foAPIKey = grantIds.flatMap(apiKeyManager.addGrants(tid, _))
          
          foAPIKey.map {
            case Some(t) => Success(t)
            case None => failure("Unable to assign given grants to API key "+tid)
          }
        } else {
          Future(failure("Requestor lacks permissions to give grants to API key "+tid))
        }
      }
    }
  }

  def apiKeyDetails(tid: APIKey): Future[Option[(APIKeyRecord, Set[Grant])]] = {
    apiKeyManager.findAPIKey(tid).flatMap {
      case Some(apiKey) =>
        val grants = Future.sequence { apiKey.grants.map(grantId => apiKeyManager.findGrant(grantId)) }
        grants.map(_.flatten).map(Option(apiKey, _))
      case None => Future(None)
    }
  }

  def apiKeyGrants(tid: APIKey): Future[Option[Set[Grant]]] = {
    apiKeyManager.findAPIKey(tid).flatMap {
      case Some(apiKey) =>
        val grants = Future.sequence { apiKey.grants.map(grantId => apiKeyManager.findGrant(grantId)) }
        grants.map(grants => some(grants.flatten))
      case None => Future(some(Set.empty))
    }
  }
  
  def addAPIKeyGrant(tid: APIKey, gid: GrantID): Future[Validation[String, Unit]] = {
    apiKeyManager.addGrants(tid, Set(gid)).map(_.map(_ => ()).toSuccess("Unable to add grant "+gid+" to API key "+tid))
  }
  
  def removeAPIKeyGrant(tid: APIKey, gid: GrantID): Future[Validation[String, Unit]] = {
    apiKeyManager.removeGrants(tid, Set(gid)).map(_.map(_ => ()).toSuccess("Unable to remove grant "+gid+" from API key "+tid))
  } 

  def deleteAPIKey(tid: APIKey): Future[Boolean] = {
    apiKeyManager.deleteAPIKey(tid).map(_.isDefined)
  } 

  def createGrant(tid: APIKey, request: Permission): Future[Validation[String, Grant]] = {
    mayGrant(tid, Set(request)).flatMap { mayGrant =>
      if (mayGrant) apiKeyManager.newGrant(None, request).map(success(_))
      else Future(failure("Requestor lacks permissions to give grants to API key "+tid))
    }
  }
  
  def grantDetails(gid: GrantID): Future[Validation[String, Grant]] = {
    apiKeyManager.findGrant(gid).map(_.toSuccess("Unable to find grant "+gid))
  }

  def grantChildren(gid: GrantID): Future[Set[Grant]] = {
    apiKeyManager.findGrantChildren(gid)
  }
  
  def addGrantChild(gid: GrantID, request: Permission): Future[Validation[String, Grant]] = {
    apiKeyManager.findGrant(gid).flatMap {
      case Some(grant) => apiKeyManager.newGrant(Option(gid), request).map(Success(_))
      case None => Future(Failure("Unable to find grant "+gid))
    }
  }
  
  def deleteGrant(gid: GrantID): Future[Boolean] = {
    apiKeyManager.deleteGrant(gid).map(!_.isEmpty)
  } 

  def close() = apiKeyManager.close()
}

case class NewAPIKeyRequest(apiKeyName: String, grants: List[Permission])

trait NewAPIKeyRequestSerialization {
  implicit val newAPIKeyRequestExtractor: Extractor[NewAPIKeyRequest] = new Extractor[NewAPIKeyRequest] with ValidatedExtraction[NewAPIKeyRequest] {    
    override def validated(obj: JValue): Validation[Error, NewAPIKeyRequest] = 
      (((obj \ "name").validated[String] <+> Success("(unnamed)")) |@|
       (obj \ "grants").validated[List[Permission]]).apply(NewAPIKeyRequest.apply _)
  }
  
  implicit val newAPIKeyRequestDecomposer: Decomposer[NewAPIKeyRequest] = new Decomposer[NewAPIKeyRequest] {
    override def decompose(request: NewAPIKeyRequest): JValue = JObject(List(
      JField("name", request.apiKeyName),
      JField("grants", JArray(request.grants.map(_.serialize)))
    )) 
  }
}

object NewAPIKeyRequest extends NewAPIKeyRequestSerialization

case class APIKeyDetails(apiKeyRecord: APIKeyRecord, grants: Set[Grant])

trait APIKeyDetailsSerialization {
  implicit val apiKeyDetailsExtractor: Extractor[APIKeyDetails] = new Extractor[APIKeyDetails] with ValidatedExtraction[APIKeyDetails] {
    override def validated(obj: JValue): Validation[Error, APIKeyDetails] =
      ((obj \ "name").validated[String] |@| (obj \ "apiKey").validated[String] |@| (obj \ "grants").validated[GrantSet]) { (name, apiKey, grantSet) =>
        APIKeyDetails(APIKeyRecord(name, apiKey, "(redacted)", grantSet.grants.map(_.gid)), grantSet.grants)
      }
  }
  
  implicit val apiKeyDetailsDecomposer: Decomposer[APIKeyDetails] = new Decomposer[APIKeyDetails] {
    override def decompose(details: APIKeyDetails): JValue = JObject(List(
      JField("name", details.apiKeyRecord.name),
      JField("apiKey", details.apiKeyRecord.tid),
      JField("grants", GrantSet(details.grants).serialize)
    ))
  }
}

object APIKeyDetails extends APIKeyDetailsSerialization

case class WrappedAPIKey(name: String, apiKey: APIKey)

trait WrappedAPIKeySerialization {
  implicit val wrappedAPIKeyExtractor: Extractor[WrappedAPIKey] = new Extractor[WrappedAPIKey] with ValidatedExtraction[WrappedAPIKey] {
    override def validated(obj: JValue): Validation[Error, WrappedAPIKey] =
      ((obj \ "name").validated[String] |@| (obj \ "apiKey").validated[String]) { WrappedAPIKey.apply _ }
  }
  
  implicit val wrappedAPIKeyDecomposer: Decomposer[WrappedAPIKey] = new Decomposer[WrappedAPIKey] {
    override def decompose(wrappedAPIKey: WrappedAPIKey): JValue = JObject(List(
      JField("name", wrappedAPIKey.name),
      JField("apiKey", wrappedAPIKey.apiKey)
    ))
  }
}

object WrappedAPIKey extends WrappedAPIKeySerialization 

case class APIKeySet(apiKeys: Set[WrappedAPIKey])

trait APIKeySetSerialization {
  implicit val apiKeySetExtractor: Extractor[APIKeySet] = new Extractor[APIKeySet] with ValidatedExtraction[APIKeySet] {
    override def validated(obj: JValue): Validation[Error, APIKeySet] =
      obj.validated[Set[WrappedAPIKey]] map { APIKeySet.apply _ }
  }

  implicit val apiKeySetDecomposer: Decomposer[APIKeySet] = new Decomposer[APIKeySet] {
    override def decompose(apiKeySet: APIKeySet): JValue = apiKeySet.apiKeys.serialize
  }
}

object APIKeySet extends APIKeySetSerialization

case class WrappedGrantId(grantId: String)

trait WrappedGrantIdSerialization {
  implicit val wrappedGrantIdExtractor: Extractor[WrappedGrantId] = new Extractor[WrappedGrantId] with ValidatedExtraction[WrappedGrantId] {
    override def validated(obj: JValue): Validation[Error, WrappedGrantId] =
      (obj \ "grantId").validated[String].map(WrappedGrantId(_))
  }
  
  implicit val wrappedGrantIdDecomposer: Decomposer[WrappedGrantId] = new Decomposer[WrappedGrantId] {
    override def decompose(wrappedGrantId: WrappedGrantId): JValue = JObject(List(
      JField("grantId", wrappedGrantId.grantId)
    ))
  }
}

object WrappedGrantId extends WrappedGrantIdSerialization 

case class GrantSet(grants: Set[Grant])

trait GrantSetSerialization {
  implicit val grantSetExtractor: Extractor[GrantSet] = new Extractor[GrantSet] with ValidatedExtraction[GrantSet] {
    override def validated(obj: JValue): Validation[Error, GrantSet] =
      obj.validated[Set[GrantDetails]].map(grants => GrantSet(grants.map(_.grant)))
  }
  
  implicit val grantSetDecomposer: Decomposer[GrantSet] = new Decomposer[GrantSet] {
    override def decompose(details: GrantSet): JValue = JArray(details.grants.map(GrantDetails(_).serialize).toList)
  }
}

object GrantSet extends GrantSetSerialization

case class GrantDetails(grant: Grant)

trait GrantDetailsSerialization {
  implicit val grantDetailsExtractor: Extractor[GrantDetails] = new Extractor[GrantDetails] with ValidatedExtraction[GrantDetails] {
    override def validated(obj: JValue): Validation[Error, GrantDetails] =
      ((obj \ "grantId").validated[String] |@|
       obj.validated[Permission]).apply((id, permission) => GrantDetails(Grant(id, None, permission)))
  }

  implicit val grantDetailsDecomposer: Decomposer[GrantDetails] = new Decomposer[GrantDetails] {
    override def decompose(details: GrantDetails): JValue =
      JObject(List(
        JField("grantId", details.grant.gid)
      )) merge details.grant.permission.serialize
  }
}

object GrantDetails extends GrantDetailsSerialization

// type TokenServiceHandlers // for ctags
