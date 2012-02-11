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
package ingest
package service

import blueeyes._
import blueeyes.core.service._
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service.RestPathPattern._
import blueeyes.json.JsonAST._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.util.Clock

import akka.dispatch.Future
import akka.dispatch.MessageDispatcher

import org.joda.time.DateTime
import com.weiglewilczek.slf4s.Logger

import scalaz.Scalaz._
import scalaz.{Validation, Success, Failure}

import com.precog.analytics._

object TokenService extends HttpRequestHandlerCombinators {

  def apply(tokenManager: TokenStorage, clock: Clock, logger: Logger)(implicit dispatcher: MessageDispatcher): HttpService[Future[JValue], Token => Future[HttpResponse[JValue]]] = {
    path(/?) {
      get { 
        (request: HttpRequest[Future[JValue]]) => (token: Token) => {
          tokenManager.listDescendants(token) map { 
            _.map(_.tokenId).serialize.ok
          }
        }
      } ~
      post { 
        (request: HttpRequest[Future[JValue]]) => (parent: Token) => {
          request.content map { 
            _.flatMap { content => 
              val path        = (content \ "path").deserialize[Option[String]].getOrElse("/")
              val permissions = (content \ "permissions").deserialize(Permissions.permissionsExtractor(parent.permissions))
              val expires     = (content \ "expires").deserialize[Option[DateTime]].getOrElse(parent.expires)
              val limits      = (content \ "limits").deserialize(Limits.limitsExtractor(parent.limits))

              if (expires < clock.now()) {
                Future(HttpResponse[JValue](BadRequest, content = Some("Your are attempting to create an expired token. Such a token will not be usable.")))
              } else tokenManager.issueNew(parent, Path(path), permissions, expires, limits) map {
                case Success(newToken) => HttpResponse[JValue](content = Some(newToken.tokenId.serialize))
                case Failure(message) => throw new HttpException(BadRequest, message)
              }
            }
          } getOrElse {
            Future(HttpResponse[JValue](BadRequest, content = Some("New token must be contained in POST content")))
          }
        }
      }
    } ~
    path("/") {
      path("children") {
        get { 
          (request: HttpRequest[Future[JValue]]) => (token: Token) => {
            tokenManager.listChildren(token).map(children => HttpResponse[JValue](content = Some(children.map(_.tokenId).serialize)))
          }
        }
      } ~ 
      path('descendantTokenId) {
        get { 
          (request: HttpRequest[Future[JValue]]) => (token: Token) => {
            if (token.tokenId == request.parameters('descendantTokenId)) {
              token.parentTokenId.map { parTokenId =>
                tokenManager.lookup(parTokenId).map { parent => 
                  val sanitized = parent.map(token.relativeTo).map(_.copy(parentTokenId = None, accountTokenId = ""))
                  HttpResponse[JValue](content = sanitized.map(_.serialize))
                }
              } getOrElse {
                Future(HttpResponse[JValue](Forbidden))
              }
            } else {
              tokenManager.getDescendant(token, request.parameters('descendantTokenId)).map { 
                _.map(_.relativeTo(token).copy(accountTokenId = "").serialize)
              } map { descendantToken =>
                HttpResponse[JValue](content = descendantToken)
              }
            }
          }
        } ~
        delete { 
          (request: HttpRequest[Future[JValue]]) => (token: Token) => {
            tokenManager.lookup(request.parameters('descendantTokenId)) flatMap { 
              _ map { descendant =>
                tokenManager.deleteDescendant(token, descendant.tokenId) map { _ =>
                  HttpResponse[JValue](content = None)
                } onFailure { 
                  case ex => logger.warn("An error occurred deleting the token: " + request.parameters('descendantTokenId), ex)
                } 
              } getOrElse {
                Future {
                  HttpResponse[JValue](
                    HttpStatus(BadRequest, "No token with id " + request.parameters('descendantTokenId) + " could be found."), 
                    content = None)
                }
              } 
            } 
          }
        } 
      }
    }
  }
}

class TokenRequiredService[A, B](tokenManager: TokenManager, val delegate: HttpService[A, Token => Future[B]])(implicit err: (HttpFailure, String) => B, dispatcher: MessageDispatcher) 
extends DelegatingService[A, Future[B], A, Token => Future[B]] {
  val service = (request: HttpRequest[A]) => {
    request.parameters.get('tokenId) match {
      case None => DispatchError(BadRequest, "A tokenId query parameter is required to access this URL").fail

      case Some(tokenId) =>
        delegate.service(request) map { (f: Token => Future[B]) =>
          tokenManager.lookup(tokenId) flatMap { 
            case None =>                           Future(err(BadRequest,   "The specified token does not exist"))
            case Some(token) if (token.expired) => Future(err(Unauthorized, "The specified token has expired"))

            case Some(token) => f(token)
          }
        }
    }
  }

  val metadata = Some(AboutMetadata(ParameterMetadata('tokenId, None), DescriptionMetadata("A ReportGrid account token is required for the use of this service.")))
}

// vim: set ts=4 sw=4 et:
