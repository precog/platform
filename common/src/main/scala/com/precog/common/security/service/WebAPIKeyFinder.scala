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
import accounts.AccountId

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
  def apply(config: Configuration)(implicit executor: ExecutionContext): APIKeyFinder[Future] = {
    new WebAPIKeyFinder(
      config[String]("protocol", "http"),
      config[String]("host", "localhost"),
      config[Int]("port", 80),
      config[String]("path", "/security/v1/"),
      config[String]("rootKey")
    )
  }
}

class WebAPIKeyFinder(protocol: String, host: String, port: Int, path: String, rootAPIKey: APIKey)(implicit executor: ExecutionContext) 
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

  def newAPIKey(accountId: AccountId, path: Path, keyName: Option[String] = None, keyDesc: Option[String] = None): Future[v1.APIKeyDetails] = {
    val keyRequest = v1.NewAPIKeyRequest.newAccount(accountId, path, None, None)

    withJsonClient { client => 
      client.query("apiKey", rootAPIKey).post[JValue]("apiKeys/")(keyRequest.serialize) map {
        case HttpResponse(HttpStatus(OK, _), _, Some(wrappedKey), _) =>
          wrappedKey.validated[v1.APIKeyDetails] valueOr { error =>
            logger.error("Unable to deserialize response from auth service: " + error.message)
            throw HttpException(BadGateway, "Unexpected response to API key creation request: " + error.message)
          }

        case HttpResponse(HttpStatus(failure: HttpFailure, reason), _, content, _) => 
          logger.error("Fatal error attempting to create api key: " + failure + ": " + content)
          throw HttpException(failure, reason)

        case x => 
          logger.error("Unexpected response from api provisioning service: " + x)
          throw HttpException(BadGateway, "Unexpected response from the api provisioning service: " + x)
      }
    }
  }

  def addGrant(authKey: APIKey, accountKey: APIKey, grantId: GrantId): Future[Boolean] = {
    val requestBody = jobject(JField("grantId", JString(grantId)))

    withJsonClient { client =>
      client.query("apiKey", authKey).post[JValue]("apikeys/" + accountKey + "/grants/")(requestBody) map {
        case HttpResponse(HttpStatus(Created, _), _, None, _) => 
          true
        
        case _ =>
          false
      }
    }
  }
}



// vim: set ts=4 sw=4 et:
