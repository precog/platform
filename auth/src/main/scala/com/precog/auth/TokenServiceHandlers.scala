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
import blueeyes.json.xschema.{ ValidatedExtraction, Extractor, Decomposer }
import blueeyes.json.xschema.DefaultSerialization.{ DateTimeDecomposer => _, DateTimeExtractor => _, _ }
import blueeyes.json.xschema.Extractor._

import com.weiglewilczek.slf4s.Logging

import scalaz.{ Applicative, Validation, Success, Failure }
import scalaz.Scalaz._
import scalaz.Validation._

import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

class GetTokensHandler(tokenManagement: TokenManagement)(implicit dispatcher: MessageDispatcher) extends CustomHttpService[Future[JValue], Token => Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[Future[JValue]]) => {
    Success { (authToken: Token) => 
      tokenManagement.tokens(authToken.tid).map { tokens =>
        HttpResponse[JValue](OK, content = Some(APIKeySet(tokens.map(_.tid)).serialize))
      }
    }
  }
  val metadata = None
}

class CreateTokenHandler(tokenManagement: TokenManagement)(implicit dispatcher: MessageDispatcher) extends CustomHttpService[Future[JValue], Token => Future[HttpResponse[JValue]]] with Logging {
  val service: HttpRequest[Future[JValue]] => Validation[NotServed, Token => Future[HttpResponse[JValue]]] = (request: HttpRequest[Future[JValue]]) => {
    Success { (authToken: Token) => 
      request.content.map { _.flatMap { jvalue =>
        logger.debug("Creating token in response to request with auth token " + authToken + ":\n" + jvalue)
        jvalue.validated[NewTokenRequest] match {
          case Success(r) =>
            if (r.grants.exists(_.isExpired(new DateTime())))
              Future(HttpResponse[JValue](HttpStatus(BadRequest, "Error creating new token."), content = Some(JObject(List(
                JField("error", "Unable to create token with expired permission")
              )))))
            else
              tokenManagement.createToken(authToken, r).map { 
                case Success(token) => 
                  HttpResponse[JValue](OK, content = Some(token.tid.serialize))
                case Failure(e) => 
                  logger.warn("Failed to create token: " + e)
                  HttpResponse[JValue](HttpStatus(BadRequest, "Error creating new token."), content = Some(JObject(List(
                    JField("error", "Error creating new token: " + e)
                  ))))
              }
          case Failure(e) =>
            logger.warn("The token request body \n" + jvalue + "\n was invalid: " + e)
            Future(HttpResponse[JValue](HttpStatus(BadRequest, "Invalid new token request body."), content = Some(JObject(List(
              JField("error", "Invalid new token request body: " + e)
            )))))
          }
        }}.getOrElse {
        Future(HttpResponse[JValue](HttpStatus(BadRequest, "Missing new token request body."), content = Some(JString("Missing new token request body."))))
      }
    }
  }
  val metadata = None
}

class GetTokenDetailsHandler(tokenManagement: TokenManagement)(implicit dispatcher: MessageDispatcher) extends CustomHttpService[Future[JValue], Token => Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[Future[JValue]]) => {
    Success { (authToken: Token) => 
      request.parameters.get('apikey).map { tid =>
        tokenManagement.tokenDetails(tid).map { 
          case Some((token, grants)) =>
            HttpResponse[JValue](OK, content = Some(TokenDetails(token, grants).serialize))
          case None =>
            HttpResponse[JValue](HttpStatus(NotFound), content = Some(JString("Unable to find token "+tid)))
        }
      }.getOrElse {
        Future(HttpResponse[JValue](HttpStatus(BadRequest, "Missing token id in request URI."),
                                    content = Some(JString("Missing token id in request URI."))))
      }
    }
  }
  val metadata = None
}

class GetTokenGrantsHandler(tokenManagement: TokenManagement)(implicit dispatcher: MessageDispatcher) extends CustomHttpService[Future[JValue], Token => Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[Future[JValue]]) => {
    Success { (authToken: Token) => 
      request.parameters.get('apikey).map { tid =>
        tokenManagement.tokenGrants(tid).map {
          case Some(grants) =>
            HttpResponse[JValue](OK, content = Some(GrantSet(grants).serialize))
          case None =>
            HttpResponse[JValue](HttpStatus(NotFound), content = Some(JString("The specified token does not exist")))
        }
      }.getOrElse {
        Future(HttpResponse[JValue](HttpStatus(BadRequest, "Missing token id in request URI."), 
                                    content = Some(JString("Missing token id in request URI."))))
      }
    }
  }
  val metadata = None
}

class AddTokenGrantHandler(tokenManagement: TokenManagement)(implicit dispatcher: MessageDispatcher) extends CustomHttpService[Future[JValue], Token => Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[Future[JValue]]) => {
    Success { (authToken: Token) => 
      (for {
        tid <- request.parameters.get('apikey) 
        contentFuture <- request.content 
      } yield {
        contentFuture flatMap { jv =>
          (jv \ "grantId").validated[String] match {
            case Success(gid) =>
              // TODO: Shouldn't this be using the auth token somehow???
              tokenManagement.addTokenGrant(tid, gid).map {
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
        Future(HttpResponse[JValue](HttpStatus(BadRequest, "Missing token id in request URI."), 
                                    content = Some(JString("Missing token id in request URI."))))
      }
    }
  }
  val metadata = None
}

class RemoveTokenGrantHandler(tokenManagement: TokenManagement)(implicit dispatcher: MessageDispatcher) extends CustomHttpService[Future[JValue], Token => Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[Future[JValue]]) => {
    Success { (authToken: Token) =>
      (for {
        tid <- request.parameters.get('apikey) 
        gid <- request.parameters.get('grantId)
      } yield tokenManagement.removeTokenGrant(tid, gid).map {
        case Success(_) => HttpResponse[JValue](NoContent)
        case Failure(e) => 
          HttpResponse[JValue](HttpStatus(BadRequest, "Invalid remove grant request."), content = Some(JObject(List(
            JField("error", "Invalid remove grant request: " + e)
          ))))
      }).getOrElse {
        Future(HttpResponse[JValue](HttpStatus(BadRequest, "Missing token id in request URI."), content = Some(JString("Missing token id in request URI."))))
      }
    }
  }
  val metadata = None
}

class DeleteTokenHandler(tokenManagement: TokenManagement)(implicit dispatcher: MessageDispatcher) extends CustomHttpService[Future[JValue], Token => Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[Future[JValue]]) => {
    Success { (authToken: Token) => 
      request.parameters.get('apikey).map { tid =>
        tokenManagement.deleteToken(tid).map { 
          if(_) HttpResponse[JValue](HttpStatus(NoContent))
          else  HttpResponse[JValue](HttpStatus(NotFound), content = Some(JString("Unable to find token "+tid)))
        }
      }.getOrElse {
        Future(HttpResponse[JValue](HttpStatus(BadRequest, "Missing token id in request URI."), content = Some(JString("Missing token id in request URI."))))
      }
    }
  }
  val metadata = None
}

class CreateGrantHandler(tokenManagement: TokenManagement)(implicit dispatcher: MessageDispatcher) extends CustomHttpService[Future[JValue], Token => Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[Future[JValue]]) => {
    Success { (authToken: Token) =>
      (for {
        content <- request.content 
      } yield {
        content.flatMap { _.validated[Permission] match {
          case Success(permission) => tokenManagement.createGrant(permission) map {
            case Success(grant) => 
              HttpResponse[JValue](OK, content = Some(grant.gid.serialize))
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
        Future(HttpResponse[JValue](HttpStatus(BadRequest, "Missing token id in request URI."), content = Some(JString("Missing token id in request URI."))))
      }
    }
  }
  val metadata = None
}

class GetGrantDetailsHandler(tokenManagement: TokenManagement)(implicit dispatcher: MessageDispatcher) extends CustomHttpService[Future[JValue], Token => Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[Future[JValue]]) => {
    Success { (t: Token) => 
      (for {
        gid <- request.parameters.get('grantId)
      } yield tokenManagement.grantDetails(gid).map {
        case Success(grant) => HttpResponse[JValue](OK, content = Some(GrantDetails(grant).serialize))
        case Failure(e) => 
          HttpResponse[JValue](HttpStatus(NotFound), content = Some(JString("Unable to find grant "+gid)))
      }).getOrElse {
        Future(HttpResponse[JValue](HttpStatus(BadRequest, "Missing token id in request URI."), content = Some(JString("Missing token id in request URI."))))
      }
    }
  }
  val metadata = None
}

class GetGrantChildrenHandler(tokenManagement: TokenManagement)(implicit dispatcher: MessageDispatcher) extends CustomHttpService[Future[JValue], Token => Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[Future[JValue]]) => {
    Success { (authToken: Token) =>
      (for {
        gid <- request.parameters.get('grantId)
      } yield tokenManagement.grantChildren(gid).map { grants =>
        HttpResponse[JValue](OK, content = Some(GrantSet(grants).serialize))
      }).getOrElse {
        Future(HttpResponse[JValue](HttpStatus(BadRequest, "Missing token id in request URI."), content = Some(JString("Missing token id in request URI."))))
      }
    }
  }
  val metadata = None
}

class AddGrantChildHandler(tokenManagement: TokenManagement)(implicit dispatcher: MessageDispatcher) extends CustomHttpService[Future[JValue], Token => Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[Future[JValue]]) => {
    Success { (authToken: Token) =>
      (for {
        gid <- request.parameters.get('grantId) 
        content <- request.content 
      } yield {
        content.flatMap { _.validated[Permission] match {
          case Success(permission) => tokenManagement.addGrantChild(gid, permission) map {
            case Success(grant) => 
              HttpResponse[JValue](OK, content = Some(grant.gid.serialize))
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
        Future(HttpResponse[JValue](HttpStatus(BadRequest, "Missing token id in request URI."), content = Some(JString("Missing token id in request URI."))))
      }
    }
  }
  val metadata = None
}

class DeleteGrantHandler(tokenManagement: TokenManagement)(implicit dispatcher: MessageDispatcher) extends CustomHttpService[Future[JValue], Token => Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[Future[JValue]]) => {
    Success { (authToken: Token) =>
      request.parameters.get('grantId).map { gid =>
        tokenManagement.deleteGrant(gid).map {
          if(_) HttpResponse[JValue](HttpStatus(NoContent))
          else  HttpResponse[JValue](HttpStatus(NotFound), content = Some(JString("Unable to find grant "+gid)))
      }}.getOrElse {
        Future(HttpResponse[JValue](HttpStatus(BadRequest, "Missing token id in request URI."), content = Some(JString("Missing token id in request URI."))))
      }
    }
  }
  val metadata = None
}

class TokenManagement(val tokenManager: TokenManager[Future])(implicit val execContext: ExecutionContext)
  extends TokenManagerAccessControl[Future](tokenManager) {
  
  def tokens(tid: TokenID) : Future[Set[Token]] = {
    tokenManager.listTokens.map(_.filter(_.cid == tid).toSet)
  }

  def createToken(requestor: Token, request: NewTokenRequest): Future[Validation[String, Token]] = {
    tokenManager.newToken("Anonymous", Set.empty).flatMap { token =>
      val tid = token.tid
      
      mayGrant(requestor.tid, request.grants.toSet).flatMap { mayGrant =>
        if (mayGrant) {
          val grantIds = Future.sequence(request.grants.map(tokenManager.newGrant(None, _))).map(_.map(_.gid).toSet)
          val foToken = grantIds.flatMap(tokenManager.addGrants(tid, _))
          
          foToken.map {
            case Some(t) => Success(t)
            case None => failure("Unable to assign given grants to token "+tid)
          }
        } else {
          Future(failure("Requestor lacks permissions to give grants to token "+tid))
        }
      }
    }
  }

  def tokenDetails(tid: TokenID): Future[Option[(Token, Set[Grant])]] = {
    tokenManager.findToken(tid).flatMap {
      case Some(token) =>
        val grants = Future.sequence { token.grants.map(grantId => tokenManager.findGrant(grantId)) }
        grants.map(_.flatten).map(Option(token, _))
      case None => Future(None)
    }
  }

  def tokenGrants(tid: TokenID): Future[Option[Set[Grant]]] = {
    tokenManager.findToken(tid).flatMap {
      case Some(token) =>
        val grants = Future.sequence { token.grants.map(grantId => tokenManager.findGrant(grantId)) }
        grants.map(grants => some(grants.flatten))
      case None => Future(some(Set.empty))
    }
  }
  
  def addTokenGrant(tid: TokenID, gid: GrantID): Future[Validation[String, Unit]] = {
    tokenManager.addGrants(tid, Set(gid)).map(_.map(_ => ()).toSuccess("Unable to add grant "+gid+" to token "+tid))
  }
  
  def removeTokenGrant(tid: TokenID, gid: GrantID): Future[Validation[String, Unit]] = {
    tokenManager.removeGrants(tid, Set(gid)).map(_.map(_ => ()).toSuccess("Unable to remove grant "+gid+" from token "+tid))
  } 

  def deleteToken(tid: TokenID): Future[Boolean] = {
    tokenManager.deleteToken(tid).map(_.isDefined)
  } 

  def createGrant(request: Permission): Future[Validation[String, Grant]] = {
    tokenManager.newGrant(None, request).map(Success(_))
  }
  
  def grantDetails(gid: GrantID): Future[Validation[String, Grant]] = {
    tokenManager.findGrant(gid).map(_.toSuccess("Unable to find grant "+gid))
  }

  def grantChildren(gid: GrantID): Future[Set[Grant]] = {
    tokenManager.findGrantChildren(gid)
  }
  
  def addGrantChild(gid: GrantID, request: Permission): Future[Validation[String, Grant]] = {
    tokenManager.findGrant(gid).flatMap {
      case Some(grant) => tokenManager.newGrant(Option(gid), request).map(Success(_))
      case None => Future(Failure("Unable to find grant "+gid))
    }
  }
  
  def deleteGrant(gid: GrantID): Future[Boolean] = {
    tokenManager.deleteGrant(gid).map(!_.isEmpty)
  } 

  def close() = tokenManager.close()
}

case class NewTokenRequest(grants: List[Permission])

trait NewTokenRequestSerialization {
  implicit val newTokenRequestExtractor: Extractor[NewTokenRequest] = new Extractor[NewTokenRequest] with ValidatedExtraction[NewTokenRequest] {    
    override def validated(obj: JValue): Validation[Error, NewTokenRequest] = 
      (obj \ "grants").validated[List[Permission]].map(NewTokenRequest(_))
  }
  
  implicit val newTokenRequestDecomposer: Decomposer[NewTokenRequest] = new Decomposer[NewTokenRequest] {
    override def decompose(request: NewTokenRequest): JValue = JObject(List(
      JField("grants", JArray(request.grants.map(_.serialize)))
    )) 
  }
}

object NewTokenRequest extends NewTokenRequestSerialization

case class TokenDetails(token: Token, grants: Set[Grant])

trait TokenDetailsSerialization {
  implicit val tokenDetailsExtractor: Extractor[TokenDetails] = new Extractor[TokenDetails] with ValidatedExtraction[TokenDetails] {
    override def validated(obj: JValue): Validation[Error, TokenDetails] =
      ((obj \ "apiKey").validated[String] |@|
       (obj \ "grants").validated[GrantSet]).apply((id, grantSet) => TokenDetails(Token(id, "Unknown", grantSet.grants.map(_.gid)), grantSet.grants))
  }
  
  implicit val tokenDetailsDecomposer: Decomposer[TokenDetails] = new Decomposer[TokenDetails] {
    override def decompose(details: TokenDetails): JValue = JObject(List(
      JField("apiKey", details.token.tid),
      JField("grants", GrantSet(details.grants).serialize)
    ))
  }
}

object TokenDetails extends TokenDetailsSerialization

case class WrappedAPIKey(apiKey: String)

trait WrappedAPIKeySerialization {
  implicit val wrappedAPIKeyExtractor: Extractor[WrappedAPIKey] = new Extractor[WrappedAPIKey] with ValidatedExtraction[WrappedAPIKey] {
    override def validated(obj: JValue): Validation[Error, WrappedAPIKey] =
      (obj \ "apiKey").validated[String].map(WrappedAPIKey(_))
  }
  
  implicit val wrappedAPIKeyDecomposer: Decomposer[WrappedAPIKey] = new Decomposer[WrappedAPIKey] {
    override def decompose(wrappedAPIKey: WrappedAPIKey): JValue = JObject(List(
      JField("apiKey", wrappedAPIKey.apiKey)
    ))
  }
}

object WrappedAPIKey extends WrappedAPIKeySerialization 

case class APIKeySet(apiKeys: Set[TokenID])

trait APIKeySetSerialization {
  implicit val tokenSetExtractor: Extractor[APIKeySet] = new Extractor[APIKeySet] with ValidatedExtraction[APIKeySet] {
    override def validated(obj: JValue): Validation[Error, APIKeySet] =
      obj.validated[Set[WrappedAPIKey]].map(wrappedKeys => APIKeySet(wrappedKeys.map(_.apiKey)))
  }

  implicit val tokenSetDecomposer: Decomposer[APIKeySet] = new Decomposer[APIKeySet] {
    override def decompose(apiKeySet: APIKeySet): JValue = JArray(apiKeySet.apiKeys.map(apiKey => WrappedAPIKey(apiKey).serialize).toList)
  }
}

object APIKeySet extends APIKeySetSerialization

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
