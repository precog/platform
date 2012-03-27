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

import com.precog.common.Path
import com.precog.common.security._

class TokenRequiredService[A, B](tokenManager: TokenManager, val delegate: HttpService[A, Token => Future[B]])(implicit err: (HttpFailure, String) => B, dispatcher: MessageDispatcher) 
extends DelegatingService[A, Future[B], A, Token => Future[B]] {
  val service = (request: HttpRequest[A]) => {
    request.parameters.get('tokenId) match {
      case None => DispatchError(BadRequest, "A tokenId query parameter is required to access this URL").fail

      case Some(tokenId) =>
        delegate.service(request) map { (f: Token => Future[B]) =>
          tokenManager.lookup(tokenId) flatMap {  
            case None                           => Future(err(BadRequest,   "The specified token does not exist"))
            case Some(token) if (token.expired) => Future(err(Unauthorized, "The specified token has expired"))
            case Some(token)                    => f(token)
          }
        }
    }
  }

  val metadata = Some(AboutMetadata(ParameterMetadata('tokenId, None), DescriptionMetadata("A ReportGrid account token is required for the use of this service.")))
}
