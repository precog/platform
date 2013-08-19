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
import blueeyes.json.serialization._
import blueeyes.json.serialization.DefaultSerialization.{ DateTimeDecomposer => _, DateTimeExtractor => _, _ }

import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import org.streum.configrity.Configuration
import com.weiglewilczek.slf4s.Logging

import java.net.URLEncoder

import scalaz._
import scalaz.Validation._
import scalaz.{NonEmptyList => NEL}
import scalaz.EitherT.eitherT
import scalaz.std.option._
import scalaz.std.stream._
import scalaz.syntax.bifunctor._
import scalaz.syntax.monad._
import scalaz.syntax.traverse._
import scalaz.syntax.std.option._

object WebAPIKeyFinder {
  def apply(config: Configuration)(implicit executor: ExecutionContext): ValidationNel[String, APIKeyFinder[Response]] = {
    val serviceConfig = config.detach("service")
    serviceConfig.get[String]("hardcoded_rootKey") map { apiKey =>
      implicit val M = ResponseMonad(new FutureMonad(executor))
      success(new StaticAPIKeyFinder[Response](apiKey))
    } getOrElse {
      (config.get[String]("rootKey").toSuccess("Configuration property \"rootKey\" is required").toValidationNel |@|
       serviceConfig.get[String]("protocol").toSuccess(NEL("Configuration property \"service.protocol\" is required")) |@|
       serviceConfig.get[String]("host").toSuccess(NEL("Configuration property \"service.host\" is required")) |@|
       serviceConfig.get[Int]("port").toSuccess(NEL("Configuration property \"service.port\" is required")) |@|
       serviceConfig.get[String]("path").toSuccess(NEL("Configuration property \"service.path\" is required"))) { (rootKey, protocol, host, port, path) =>
        new RealWebAPIKeyFinder(protocol, host, port, path, rootKey)
      }
    }
  }
}

class RealWebAPIKeyFinder(protocol: String, host: String, port: Int, path: String, val rootAPIKey: APIKey)(implicit val executor: ExecutionContext)
    extends WebClient(protocol, host, port, path) with WebAPIKeyFinder {
  implicit val M = new FutureMonad(executor)
}

trait WebAPIKeyFinder extends BaseClient with APIKeyFinder[Response] {
  import scalaz.syntax.monad._
  import EitherT.{ left => leftT, right => rightT, _ }
  import \/.{ left, right }
  import DefaultBijections._
  import blueeyes.json.serialization.DefaultSerialization._

  implicit def executor: ExecutionContext

  def rootAPIKey: APIKey

  def findAPIKey(apiKey: APIKey, rootKey: Option[APIKey]): Response[Option[v1.APIKeyDetails]] = {
    withJsonClient { client0 =>
      val client = rootKey.map(client0.query("authkey", _)).getOrElse(client0)
      eitherT(client.get[JValue]("apikeys/" + apiKey) map {
        case HttpResponse(HttpStatus(OK, _), _, Some(jvalue), _) =>
          (((_: Extractor.Error).message) <-: jvalue.validated[v1.APIKeyDetails] :-> { details => Some(details) }).disjunction

        case res @ HttpResponse(HttpStatus(NotFound, _), _, _, _) =>
          logger.warn("apiKey " + apiKey + " not found:\n" + res)
          right(None)

        case res =>
          logger.error("Unexpected response from auth service retrieving details for apiKey " + apiKey + ":\n" + res)
          left("Unexpected response from auth service retrieving details for apiKey " + apiKey + ":\n" + res)
      })
    }
  }

  def findAllAPIKeys(fromRoot: APIKey): Response[Set[v1.APIKeyDetails]] = {
    withJsonClient { client =>
      eitherT(client.query("apiKey", fromRoot).get[JValue]("apikeys/") map {
        case HttpResponse(HttpStatus(OK, _), _, Some(jvalue), _) =>
          (((_:Extractor.Error).message) <-: jvalue.validated[Set[v1.APIKeyDetails]]).disjunction
        case res =>
          logger.error("Unexpected response from auth service finding child API keys from root " + fromRoot + ":\n" + res)
          left("Unexpected response from auth service finding child API keys from root " + fromRoot + ":\n" + res)
      })
    }
  }

  private val fmt = ISODateTimeFormat.dateTime()

  private def findPermissions(apiKey: APIKey, path: Path, at: Option[DateTime]): Response[Set[Permission]] = {
    withJsonClient { client0 =>
      val client = at map (fmt.print(_)) map (client0.query("at", _)) getOrElse client0
      eitherT(client.query("apiKey", apiKey).get[JValue]("permissions/fs" + path.urlEncode.path) map {
        case HttpResponse(HttpStatus(OK, _), _, Some(jvalue), _) =>
          (((_:Extractor.Error).message) <-: jvalue.validated[Set[Permission]]).disjunction
        case res =>
          logger.error("Unexpected response from auth service retrieving permissions associated with apiKey " + apiKey + ":\n" + res)
          left("Unexpected response from auth service retrieving permissions associated with apiKey " + apiKey + ":\n" + res)
      })
    }
  }

  def hasCapability(apiKey: APIKey, perms: Set[Permission], at: Option[DateTime]): Response[Boolean] = {
    // We group permissions by path, then find the permissions for each path individually.
    // This means a call to this will do 1 HTTP request per path, which isn't efficient.

    val results: Iterable[Response[Boolean]] = perms.groupBy(_.path) map { case (path, requiredPathPerms) =>
      findPermissions(apiKey, path, at) map { actualPathPerms =>
        requiredPathPerms forall { perm => actualPathPerms exists (_ implies perm) }
      }
    }

    results.toStream.sequence.map(_.foldLeft(true)(_ && _))
  }

  def newAPIKey(accountId: AccountId, path: Path, keyName: Option[String] = None, keyDesc: Option[String] = None): Response[v1.APIKeyDetails] = {
    val keyRequest = v1.NewAPIKeyRequest.newAccount(accountId, path, keyName, keyDesc, Set())

    withJsonClient { client =>
      eitherT(client.query("apiKey", rootAPIKey).post[JValue]("apikeys/")(keyRequest.serialize) map {
        case HttpResponse(HttpStatus(OK, _), _, Some(wrappedKey), _) =>
          (((_:Extractor.Error).message) <-: wrappedKey.validated[v1.APIKeyDetails]).disjunction

        case res =>
          logger.error("Unexpected response from api provisioning service: " + res)
          left("Unexpected response from api key provisioning service; unable to proceed." + res)
      })
    }
  }

  def addGrant(accountKey: APIKey, grantId: GrantId): Response[Boolean] = {
    val requestBody = jobject(JField("grantId", JString(grantId)))

    withJsonClient { client =>
      eitherT(client.post[JValue]("apikeys/" + accountKey + "/grants/")(requestBody) map {
        case HttpResponse(HttpStatus(Created, _), _, None, _) => right(true)
        case _ => right(false)
      })
    }
  }
}



// vim: set ts=4 sw=4 et:
