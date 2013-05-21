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
package accounts

import com.precog.common.Path
import com.precog.common.client._
import com.precog.common.security._
import com.precog.util._

import akka.dispatch.{ ExecutionContext, Future, Promise }

import blueeyes.bkka._
import blueeyes.core.data.DefaultBijections._
import blueeyes.core.data.ByteChunk
import blueeyes.core.http._
import blueeyes.core.http.MimeTypes._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._
import blueeyes.core.service.engines.HttpClientXLightWeb
import blueeyes.json._
import blueeyes.json.serialization.{ Extractor, Decomposer }
import blueeyes.json.serialization.DefaultSerialization.{ DateTimeDecomposer => _, DateTimeExtractor => _, _ }
import blueeyes.json.serialization.Extractor._

import org.apache.commons.codec.binary.Base64
import org.joda.time.DateTime
import org.streum.configrity.Configuration
import com.weiglewilczek.slf4s.Logging

import scalaz._
import scalaz.NonEmptyList._
import scalaz.Validation._
import scalaz.syntax.bifunctor._
import scalaz.syntax.monad._
import scalaz.syntax.validation._
import scalaz.syntax.std.option._

object WebAccountFinder extends Logging {
  def apply(config: Configuration)(implicit executor: ExecutionContext): Validation[NonEmptyList[String], AccountFinder[Response]] = {
    val serviceConfig = config.detach("service")
    serviceConfig.get[String]("hardcoded_account") map { accountId =>
      implicit val M = ResponseMonad(new FutureMonad(executor))
      success(new StaticAccountFinder[Response](accountId, serviceConfig[String]("hardcoded_rootKey", ""), serviceConfig.get[String]("hardcoded_rootPath")))
    } getOrElse {
      (serviceConfig.get[String]("protocol").toSuccess(nels("Configuration property service.protocol is required")) |@|
       serviceConfig.get[String]("host").toSuccess(nels("Configuration property service.host is required")) |@|
       serviceConfig.get[Int]("port").toSuccess(nels("Configuration property service.port is required")) |@|
       serviceConfig.get[String]("path").toSuccess(nels("Configuration property service.path is required")) |@|
       serviceConfig.get[String]("user").toSuccess(nels("Configuration property service.user is required")) |@|
       serviceConfig.get[String]("password").toSuccess(nels("Configuration property service.password is required"))) {
        (protocol, host, port, path, user, password) =>
          logger.info("Creating new WebAccountFinder with properties %s://%s:%s/%s %s:%s".format(protocol, host, port.toString, path, user, password))
          new WebAccountFinder(protocol, host, port, path, user, password)
      }
    }
  }
}

class WebAccountFinder(protocol: String, host: String, port: Int, path: String, user: String, password: String)(implicit executor: ExecutionContext) extends WebClient(protocol, host, port, path) with AccountFinder[Response] with Logging {
  import scalaz.syntax.monad._
  import EitherT.{ left => leftT, right => rightT, _ }
  import \/.{ left, right }
  import blueeyes.core.data.DefaultBijections._
  import blueeyes.json.serialization.DefaultSerialization._

  implicit val M: Monad[Future] = new FutureMonad(executor)

  def findAccountByAPIKey(apiKey: APIKey) : Response[Option[AccountId]] = {
    logger.debug("Finding account for API key " + apiKey + " with " + (protocol, host, port, path, user, password).toString)
    invoke { client =>
      logger.info("Querying accounts service for API key %s".format(apiKey))
      eitherT(client.query("apiKey", apiKey).get[JValue]("/accounts/") map {
        case HttpResponse(HttpStatus(OK, _), _, Some(jaccountId), _) =>
          logger.info("Got response for apiKey " + apiKey)
            (((_:Extractor.Error).message) <-: jaccountId.validated[WrappedAccountId] :-> { wid =>
              Some(wid.accountId)
            }).disjunction

        case HttpResponse(HttpStatus(OK, _), _, None, _) =>
          logger.warn("No account found for apiKey: " + apiKey)
          right(None)

        case res =>
          logger.error("Unexpected response from accounts service for findAccountByAPIKey: " + res)
          left("Unexpected response from accounts service; unable to proceed: " + res)
      } recoverWith {
        case ex =>
          logger.error("findAccountByAPIKey for " + apiKey + "failed.", ex)
          Promise.successful(left("Client error accessing accounts service; unable to proceed: " + ex.getMessage))
      })
    }
  }

  def findAccountDetailsById(accountId: AccountId): Response[Option[AccountDetails]] = {
    logger.debug("Finding accoung for id: " + accountId)
    invoke { client =>
      eitherT(client.get[JValue]("/accounts/" + accountId) map {
        case HttpResponse(HttpStatus(OK, _), _, Some(jaccount), _) =>
          logger.info("Got response for AccountId " + accountId)
          (((_:Extractor.Error).message) <-: jaccount.validated[Option[AccountDetails]]).disjunction

        case res =>
          logger.error("Unexpected response from accounts serviceon findAccountDetailsById: " + res)
          left("Unexpected response from accounts service; unable to proceed: " + res)
      } recoverWith {
        case ex =>
          logger.error("findAccountById for " + accountId + "failed.", ex)
          Promise.successful(left("Client error accessing accounts service; unable to proceed: " + ex.getMessage))
      })
    }
  }

  def invoke[A](f: HttpClient[ByteChunk] => A): A = {
    val auth = HttpHeaders.Authorization("Basic "+new String(Base64.encodeBase64((user+":"+password).getBytes("UTF-8")), "UTF-8"))
    withJsonClient { client =>
      f(client.header(auth))
    }
  }
}

