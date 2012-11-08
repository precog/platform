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
