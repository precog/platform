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
package com.precog.dvergr

import com.precog.common.jobs._
import com.precog.common.security._
import com.precog.common.client._
import com.precog.common.JValueByteChunkTranscoders._

import blueeyes._
import blueeyes.bkka._
import blueeyes.core.data._
import blueeyes.core.http._
import blueeyes.core.http.MimeTypes._
import blueeyes.core.http.HttpStatusCodes.{ Response => _, _ }
import blueeyes.core.service._
import blueeyes.core.service.engines.HttpClientXLightWeb
import blueeyes.json._
import blueeyes.json.serialization.DefaultSerialization._

import akka.dispatch.Future
import akka.dispatch.ExecutionContext

import scalaz._

import WebJobManager._

trait AuthService[M[+_]] { self =>
  def isValid(apiKey: APIKey): M[Boolean]

  def withM[N[+_]](implicit t: M ~> N) = new AuthService[N] {
    def isValid(apiKey: APIKey): N[Boolean] = t(self.isValid(apiKey))
  }
}

case class WebAuthService(protocol: String, host: String, port: Int, path: String)(implicit executor: ExecutionContext)
    extends WebClient(protocol, host, port, path) with AuthService[Response] {
  import scalaz.syntax.monad._
  import scalaz.EitherT.eitherT
  implicit val M: Monad[Future] = new FutureMonad(executor)

  final def isValid(apiKey: APIKey): Response[Boolean] = withJsonClient { client =>
    eitherT(client.query("apiKey", apiKey).get[JValue]("apikeys/" + apiKey) map {
      case HttpResponse(HttpStatus(OK, _), _, _, _) => \/.right(true)
      case HttpResponse(HttpStatus(NotFound, _), _, _, _) => \/.right(false)
      case res => \/.left("Unexpected response from auth service:\n" + res)
    })
  }
}

case class TestAuthService[M[+_]](validAPIKeys: Set[APIKey])(implicit M: Applicative[M]) extends AuthService[M] {
  final def isValid(apiKey: APIKey): M[Boolean] = M.point(validAPIKeys contains apiKey)
}

