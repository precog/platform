package com.precog.common
package security 

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
import com.weiglewilczek.slf4s.Logging

import scalaz.Scalaz._
import scalaz.{Validation, Success, Failure}

class TokenRequiredService[A, B](tokenManager: TokenManager, val delegate: HttpService[A, Token => Future[B]])(implicit err: (HttpFailure, String) => B, dispatcher: MessageDispatcher) 
extends DelegatingService[A, Future[B], A, Token => Future[B]] with Logging {
  val service = (request: HttpRequest[A]) => {
    request.parameters.get('tokenId) match {
      case None => DispatchError(BadRequest, "A tokenId query parameter is required to access this URL").fail

      case Some(tokenId) =>
        delegate.service(request) map { (f: Token => Future[B]) =>
          logger.debug("Locating token: " + tokenId)
          tokenManager.findToken(tokenId) flatMap {  
            case None                           => logger.warn("Could not locate token " + tokenId); Future(err(BadRequest,   "The specified token does not exist"))
            case Some(token)                    => logger.debug("Found token " + tokenId); f(token)
          }
        }
    }
  }

  val metadata = Some(AboutMetadata(ParameterMetadata('tokenId, None), DescriptionMetadata("A Precog account token is required for the use of this service.")))
}
