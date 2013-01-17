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
package com.precog.common
package security 
package service

import blueeyes.core.service._
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.json._
import blueeyes.json.serialization.DefaultSerialization._

import akka.dispatch.Future
import akka.dispatch.ExecutionContext

import com.weiglewilczek.slf4s.Logging

import scalaz._
import scalaz.syntax.std.option._

trait APIKeyServiceCombinators extends HttpRequestHandlerCombinators {
  implicit val jsonErrorTransform = (failure: HttpFailure, s: String) => HttpResponse(failure, content = Some(s.serialize))

  def apiKey[A, B](keyFinder: APIKey => Future[Option[APIKey]])(service: HttpService[A, APIKey => Future[B]])(implicit err: (HttpFailure, String) => B, M: Monad[Future]) = {
    new APIKeyRequiredService[A, B](keyFinder, service, err, M)
  }
}

class APIKeyRequiredService[A, B](keyFinder: APIKey => Future[Option[APIKey]], val delegate: HttpService[A, APIKey => Future[B]], err: (HttpFailure, String) => B, M: Monad[Future]) 
extends DelegatingService[A, Future[B], A, APIKey => Future[B]] with Logging {
  val service = (request: HttpRequest[A]) => {
    request.parameters.get('apiKey).toSuccess[NotServed] {
      DispatchError(BadRequest, "An apiKey query parameter is required to access this URL")
    } flatMap { apiKey =>
      delegate.service(request) map { (f: APIKey => Future[B]) =>
        keyFinder(apiKey) flatMap {  
          case None =>
            logger.warn("Could not locate API key " + apiKey)
            M.point(err(BadRequest, "The specified API key does not exist: "+apiKey))
            
          case Some(apiKey) =>
            logger.trace("Found API key " + apiKey)
            f(apiKey)
        }
      }
    }
  }

  val metadata =
    Some(AboutMetadata(
      ParameterMetadata('apiKey, None),
      DescriptionMetadata("A Precog API key is required for the use of this service.")))
}
