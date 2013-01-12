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

import client._

import akka.dispatch.Future
import akka.dispatch.ExecutionContext

import blueeyes.bkka._
import blueeyes.core.data._
import blueeyes.core.data.DefaultBijections._
import blueeyes.core.http._
import blueeyes.core.http.MimeTypes._
import blueeyes.core.http.HttpStatusCodes.{ Response => _, _ }
import blueeyes.core.service._
import blueeyes.json._
import blueeyes.json.serialization.SerializationImplicits._

import org.joda.time.DateTime
import org.streum.configrity.Configuration

import scalaz._
import scalaz.EitherT.eitherT
import scalaz.syntax.monad._

object WebAPIKeyFinder {
  def apply(config: Configuration)(implicit M: Monad[Future]): APIKeyFinder[Future] = {
    sys.error("todo")
  }
}

class WebAPIKeyFinder(protocol: String, host: String, port: Int, path: String)(implicit executor: ExecutionContext) 
    extends WebClient(protocol, host, port, path) with APIKeyFinder[Future] {

  implicit val M = new FutureMonad(executor)

  def findAPIKey(apiKey: APIKey): Future[Option[v1.APIKeyDetails]] = {
    withJsonClient { client =>
      client.query("apiKey", apiKey).get[JValue]("apikeys/" + apiKey) map {
        case HttpResponse(HttpStatus(OK, _), _, Some(jvalue), _) => 
          jvalue.validated[v1.APIKeyDetails].toOption

        case res => 
          logger.warn("Unexpected response from auth service for apiKey " + apiKey + ":\n" + res)
          None
      }
    }
  }

  def findAllAPIKeys(fromRoot: APIKey): Future[Set[v1.APIKeyDetails]] = {
    sys.error("todo")
  }

  def hasCapability(apiKey: APIKey, perms: Set[Permission], at: Option[DateTime]): Future[Boolean] = {
    sys.error("todo")
  }
}



// vim: set ts=4 sw=4 et:
