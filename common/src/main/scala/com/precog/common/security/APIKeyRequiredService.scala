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
import akka.dispatch.ExecutionContext

import org.joda.time.DateTime
import com.weiglewilczek.slf4s.Logging

import scalaz._
import scalaz.syntax.std.option._

class APIKeyRequiredService[A, B](keyFinder: APIKeyFinder[Future], val delegate: HttpService[A, APIKey => Future[B]], err: (HttpFailure, String) => B, executor: ExecutionContext) 
extends DelegatingService[A, Future[B], A, APIKey => Future[B]] with Logging {
  implicit val executor0 = executor
  val service = (request: HttpRequest[A]) => {
    request.parameters.get('apiKey).
      toSuccess[NotServed](DispatchError(BadRequest, "An apiKey query parameter is required to access this URL")) flatMap { apiKey =>
      delegate.service(request) map { (f: APIKey => Future[B]) =>
        logger.trace("Locating API key: " + apiKey)
        keyFinder.findAPIKey(apiKey) flatMap {  
          case None =>
            logger.warn("Could not locate API key " + apiKey)
            Future(err(BadRequest, "The specified API key does not exist: "+apiKey))
            
          case Some(apiKeyRecord) =>
            logger.trace("Found API key " + apiKeyRecord)
            f(apiKeyRecord.apiKey)
        }
      }
    }
  }

  val metadata =
    Some(AboutMetadata(
      ParameterMetadata('apiKey, None),
      DescriptionMetadata("A Precog API key is required for the use of this service.")))
}
