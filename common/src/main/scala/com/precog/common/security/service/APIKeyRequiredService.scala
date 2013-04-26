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

import com.precog.common.services.ServiceHandlerUtil._

import blueeyes.core.service._
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.http.HttpHeaders._
import blueeyes.core.http.MimeTypes._
import blueeyes.json._
import blueeyes.json.serialization.DefaultSerialization._

import akka.dispatch.Future
import akka.dispatch.ExecutionContext

import com.weiglewilczek.slf4s.Logging

import scalaz._
import scalaz.syntax.std.option._

trait APIKeyServiceCombinators extends HttpRequestHandlerCombinators {
  def apiKeyIsValid[A, B](error: String => Future[B])(service: HttpService[A, APIKey => Future[B]]): HttpService[A, Validation[String, APIKey] => Future[B]] = {
    new APIKeyValidService(service, error)
  }

  def apiKeyRequired[A, B](keyFinder: APIKey => Future[Option[APIKey]])(service: HttpService[A, Validation[String, APIKey] => Future[B]]): HttpService[A, Future[B]] = {
    new APIKeyRequiredService[A, B](keyFinder, service)
  }

  def invalidAPIKey[A](implicit convert: JValue => A, M: Monad[Future]): String => Future[HttpResponse[A]] = {
    (msg: String) => M.point((forbidden(msg) map convert).copy(headers = HttpHeaders(`Content-Type`(application/json))))
  }

  // Convenience combinator for when we know our result is an `HttpResponse` and that
  // we are returning JSON.
  def jsonAPIKey[A, B](apiKeyFinder: APIKeyFinder[Future])(
      service: HttpService[A, APIKey => Future[HttpResponse[B]]])(implicit
      inj: JValue => B, M: Monad[Future]): HttpService[A, Future[HttpResponse[B]]] = {
    jsonAPIKey(k => apiKeyFinder.findAPIKey(k, None).map(_.map(_.apiKey)))(service)
  }

  def jsonAPIKey[A, B](keyFinder: APIKey => Future[Option[APIKey]])(
      service: HttpService[A, APIKey => Future[HttpResponse[B]]])(implicit
      inj: JValue => B, M: Monad[Future]): HttpService[A, Future[HttpResponse[B]]] = {
    apiKeyRequired(keyFinder) {
      apiKeyIsValid(invalidAPIKey[B])(service)
    }
  }
}

class APIKeyValidService[A, B](val delegate: HttpService[A, APIKey => Future[B]], error: String => Future[B])
extends DelegatingService[A, Validation[String, APIKey] => Future[B], A, APIKey => Future[B]] {
  val service = { (request: HttpRequest[A]) =>
    delegate.service(request) map { (f: APIKey => Future[B]) =>
      { (apiKeyV: Validation[String, APIKey]) => apiKeyV.fold(error, f) }
    }
  }

  val metadata = AboutMetadata(
    ParameterMetadata('apiKey, None),
    DescriptionMetadata("A valid Precog API key is required for the use of this service.")
  )
}

class APIKeyRequiredService[A, B](keyFinder: APIKey => Future[Option[APIKey]], val delegate: HttpService[A, Validation[String, APIKey] => Future[B]])
extends DelegatingService[A, Future[B], A, Validation[String, APIKey] => Future[B]] with Logging {
  val service = (request: HttpRequest[A]) => {
    request.parameters.get('apiKey).toSuccess[NotServed] {
      DispatchError(BadRequest, "An apiKey query parameter is required to access this URL")
    } flatMap { apiKey =>
      delegate.service(request) map { (f: Validation[String, APIKey] => Future[B]) =>
        keyFinder(apiKey) flatMap { maybeApiKey =>
          logger.info("Found API key: " + maybeApiKey)
          f(maybeApiKey.toSuccess[String] {
            logger.warn("Could not locate API key " + apiKey)
            "The specified API key does not exist: " + apiKey
          })
        }
      }
    }
  }

  val metadata = AboutMetadata(
    ParameterMetadata('apiKey, None),
    DescriptionMetadata("A Precog API key is required for the use of this service.")
  )
}
