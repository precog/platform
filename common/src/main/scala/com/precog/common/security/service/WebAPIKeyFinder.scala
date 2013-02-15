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
import blueeyes.json.serialization.DefaultSerialization.{ DateTimeDecomposer => _, DateTimeExtractor => _, _ }

import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import org.streum.configrity.Configuration

import scalaz._
import scalaz.Validation._
import scalaz.EitherT.eitherT
import scalaz.std.set._
import scalaz.std.option._
import scalaz.syntax.monad._
import scalaz.syntax.traverse._
import scalaz.syntax.std.option._

object WebAPIKeyFinder {
  def apply(config: Configuration)(implicit executor: ExecutionContext): ValidationNEL[String, APIKeyFinder[Future]] = {
    config.get[String]("rootKey").toSuccess("rootKey configuration parameter is required").toValidationNEL map { rootKey: APIKey =>
      new RealWebAPIKeyFinder(
        config[String]("protocol", "http"),
        config[String]("host", "localhost"),
        config[Int]("port", 80),
        config[String]("path", "/security/v1/"),
        rootKey
      )
    }
  }
}

class RealWebAPIKeyFinder(protocol: String, host: String, port: Int, path: String, val rootAPIKey: APIKey)(implicit val executor: ExecutionContext) 
    extends WebClient(protocol, host, port, path) with WebAPIKeyFinder {
  implicit val M = new FutureMonad(executor)
}

trait WebAPIKeyFinder extends BaseClient with APIKeyFinder[Future] {
  implicit def executor: ExecutionContext
  def rootAPIKey: APIKey

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
    withJsonClient { client =>
      client.query("apiKey", fromRoot).get[JValue]("apikeys/") map {
        case HttpResponse(HttpStatus(OK, _), _, Some(jvalue), _) =>
          jvalue.validated[Set[v1.APIKeyDetails]] getOrElse Set.empty
        case res =>
          logger.warn("Unexpected response from auth service for apiKey " + fromRoot + ":\n" + res)
          Set.empty
      }
    }
  }

  private val fmt = ISODateTimeFormat.dateTime()

  private def findPermissions(apiKey: APIKey, path: Path, at: Option[DateTime]): Future[Set[Permission]] = {
    withJsonClient { client0 =>
      val client = at map (fmt.print(_)) map (client0.query("at", _)) getOrElse client0
      client.query("apiKey", apiKey).get[JValue]("permissions/fs" + path) map {
        case HttpResponse(HttpStatus(OK, _), _, Some(jvalue), _) =>
          jvalue.validated[Set[Permission]] getOrElse Set.empty
        case res =>
          logger.warn("Unexpected response from auth service for apiKey " + apiKey + ":\n" + res)
          Set.empty
      }
    }
  }

  def hasCapability(apiKey: APIKey, perms: Set[Permission], at: Option[DateTime]): Future[Boolean] = {

    // We group permissions by path, then find the permissions for each path individually.
    // This means a call to this will do 1 HTTP request per path, which isn't efficient.

    val results: Set[Future[Boolean]] = perms.groupBy(_.path).map({ case (path, requiredPathPerms) =>
      findPermissions(apiKey, path, at) map { actualPathPerms =>
        requiredPathPerms forall { perm => actualPathPerms exists (_ implies perm) }
      }
    })(collection.breakOut)
    results.sequence.map(_.foldLeft(true)(_ && _))
  }

  def newAPIKey(accountId: AccountId, path: Path, keyName: Option[String] = None, keyDesc: Option[String] = None): Future[v1.APIKeyDetails] = {
    val keyRequest = v1.NewAPIKeyRequest.newAccount(accountId, path, keyName, keyDesc, Set())

    withJsonClient { client => 
      client.query("apiKey", rootAPIKey).post[JValue]("apikeys/")(keyRequest.serialize) map {
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
