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
package com.precog.accounts

import com.precog.util._
import blueeyes.BlueEyesServer

import blueeyes._
import blueeyes.core.data.{BijectionsChunkJson, BijectionsChunkFutureJson, BijectionsChunkString, ByteChunk}
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._
import blueeyes.core.service.engines.HttpClientXLightWeb
import blueeyes.json._
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.bkka.AkkaDefaults
import blueeyes.bkka.Stoppable
import blueeyes.health.metrics.{eternity}
import blueeyes.util.Clock

import HttpHeaders.Authorization

import akka.dispatch.Future
import akka.dispatch.ExecutionContext

import org.joda.time.DateTime
import org.I0Itec.zkclient.ZkClient 
import org.I0Itec.zkclient.DataUpdater
import org.streum.configrity.Configuration

import com.weiglewilczek.slf4s.Logging

import scalaz._
import scalaz.syntax.std.option._


case class SecurityService(protocol: String, host: String, port: Int, path: String, rootKey: String) {
  def withClient[A](f: HttpClient[ByteChunk] => A): A = {
    val client = new HttpClientXLightWeb 
    f(client.protocol(protocol).host(host).port(port).path(path))
  }
  
  def withRootClient[A](f: HttpClient[ByteChunk] => A): A = {
    val client = new HttpClientXLightWeb 
    f(client.protocol(protocol).host(host).port(port).path(path).query("apiKey", rootKey))
  }
}

case class AccountServiceState(accountManagement: AccountManager[Future], clock: Clock, securityService: SecurityService, rootAccountId: String)


trait AccountServiceCombinators extends HttpRequestHandlerCombinators {
  def auth[A](accountManager: AccountManager[Future])(service: HttpService[A, Account => Future[HttpResponse[JValue]]])(implicit ctx: ExecutionContext) = {
    new AuthenticationService[A, HttpResponse[JValue]](accountManager, service)({
      case NotProvided => HttpResponse(Unauthorized, headers = HttpHeaders(List(("WWW-Authenticate","Basic"))))
      case AuthMismatch(message) => HttpResponse(Unauthorized, content = Some(message.serialize))
    })
  }
}


trait AccountService extends BlueEyesServiceBuilder with AkkaDefaults with AccountServiceCombinators {
  import BijectionsChunkJson._
  import BijectionsChunkString._
  import BijectionsChunkFutureJson._

  implicit val timeout = akka.util.Timeout(120000) //for now

  def accountManager(config: Configuration): AccountManager[Future]

  def clock: Clock

  val AccountService = service("accounts", "1.0") {
    requestLogging(timeout) {
      healthMonitor(timeout, List(eternity)) { monitor => context =>
        startup {
          import context._

          Future {
            logger.debug("Building account service state...")
            val accountManagement = accountManager(config)
            val securityService = SecurityService(
               config[String]("security.service.protocol", "http"),
               config[String]("security.service.host", "localhost"),
               config[Int]("security.service.port", 80),
               config[String]("security.service.path", "/security/v1/"),
               config[String]("security.rootKey")
             )

            val rootAccountId = config[String]("accounts.rootAccountId", "INVALID")

            AccountServiceState(accountManagement, clock, securityService, rootAccountId)
          }
        } ->
        request { (state: AccountServiceState) =>
          jsonp[ByteChunk] {
            path("/accounts/") {
              post(new PostAccountHandler(state.accountManagement, state.clock, state.securityService, state.rootAccountId)) ~
              auth(state.accountManagement) {
                get(new ListAccountsHandler(state.accountManagement)) ~ 
                path("'accountId") {
                  get(new GetAccountDetailsHandler(state.accountManagement)) ~ 
                  delete(new DeleteAccountHandler(state.accountManagement)) ~
                  path("/password") {
                    put(new PutAccountPasswordHandler(state.accountManagement))
                  } ~ 
                  path("/grants/") {
                    post(new CreateAccountGrantHandler(state.accountManagement, state.securityService))
                  } ~
                  path("/plan") {
                    get(new GetAccountPlanHandler(state.accountManagement)) ~
                    put(new PutAccountPlanHandler(state.accountManagement)) ~ 
                    delete(new DeleteAccountPlanHandler(state.accountManagement))
                  }
                } 
              }
            }
          }
        } ->
        shutdown { state => 
          for {
            _ <- state.accountManagement.close()
          } yield Option.empty[Stoppable]
        }
      }
    }
  }
}
