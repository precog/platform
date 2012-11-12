package com.precog.common
package security 

import blueeyes._
import blueeyes.core.service._
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service.RestPathPattern._
import blueeyes.json._
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.util.Clock

import akka.dispatch.Future
import akka.dispatch.MessageDispatcher

import org.joda.time.DateTime
import com.weiglewilczek.slf4s.Logging

import scalaz._
import scalaz.syntax.std.option._

class APIKeyRequiredService[A, B](apiKeyManager: APIKeyManager[Future], val delegate: HttpService[A, APIKeyRecord => Future[B]])
  (implicit err: (HttpFailure, String) => B, dispatcher: MessageDispatcher) 
  extends DelegatingService[A, Future[B], A, APIKeyRecord => Future[B]] with Logging {
  val service = (request: HttpRequest[A]) => {
    request.parameters.get('apiKey).
      toSuccess[NotServed](DispatchError(BadRequest, "An apiKey query parameter is required to access this URL")) flatMap { apiKey =>
      delegate.service(request) map { (f: APIKeyRecord => Future[B]) =>
        logger.debug("Locating API key: " + apiKey)
        apiKeyManager.findAPIKey(apiKey) flatMap {  
          case None =>
            logger.warn("Could not locate API key " + apiKey)
            Future(err(BadRequest, "The specified API key does not exist"))
            
          case Some(apiKey) =>
            logger.debug("Found API key " + apiKey)
            f(apiKey)
        }
      }
    }
  }

  val metadata =
    Some(AboutMetadata(
      ParameterMetadata('apiKey, None),
      DescriptionMetadata("A Precog account API key is required for the use of this service.")))
}
