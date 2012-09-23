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

import blueeyes.BlueEyesServer

import blueeyes._
import blueeyes.core.service._
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.json.JsonAST._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.bkka.AkkaDefaults
import blueeyes.bkka.Stoppable
import blueeyes.core.data.{BijectionsChunkJson, BijectionsChunkFutureJson, BijectionsChunkString, ByteChunk}
import blueeyes.health.metrics.{eternity}
import blueeyes.util.Clock

import HttpHeaders.Authorization

import akka.dispatch.Future
import akka.dispatch.MessageDispatcher

import org.joda.time.DateTime
import org.I0Itec.zkclient.ZkClient 
import org.I0Itec.zkclient.DataUpdater
import org.streum.configrity.Configuration

import com.weiglewilczek.slf4s.Logging

import scalaz._
import scalaz.syntax.std.option._


case class AccountServiceState(accountManagement: AccountManagement, clock: Clock, securityServiceRoot: String, rootKey: String)


trait AccountServiceCombinators extends HttpRequestHandlerCombinators {
  implicit val jsonErrorTransform = (failure: HttpFailure, s: String) => HttpResponse(failure, content = Some(s.serialize))
  
  def auth[A, B](accountManager: AccountManager[Future])(service: HttpService[A, Account => Future[B]])(implicit err: (HttpFailure, String) => B, dispatcher: MessageDispatcher) = {
    new AuthenticationService[A, B](accountManager, service)
  }
}


trait AccountService extends BlueEyesServiceBuilder with AkkaDefaults with AccountServiceCombinators {
  import BijectionsChunkJson._
  import BijectionsChunkString._
  import BijectionsChunkFutureJson._

  implicit val timeout = akka.util.Timeout(120000) //for now

  def AccountManagerFactory(config: Configuration): AccountManager[Future]

  def clock: Clock

  val AccountService = service("accounts", "1.0") {
    requestLogging(timeout) {
      healthMonitor(timeout, List(eternity)) { monitor => context =>
        startup {
          import context._
          val accountConfig = config.detach("accountConfig")
          Future(AccountServiceState(new AccountManagement(AccountManagerFactory(accountConfig)), 
                                     clock,
                                     accountConfig[String]("security.service"),
                                     accountConfig[String]("security.rootKey"))) 
        } ->
        request { (state: AccountServiceState) =>
          jsonp[ByteChunk] {
            post(new PostAccountHandler(state.accountManagement, state.clock, state.securityServiceRoot, state.rootKey)) ~
            auth(state.accountManagement.accountManager) {
              get(new ListAccountsHandler(state.accountManagement)) ~ 
              path("'accountId") {
                get(new GetAccountDetailsHandler(state.accountManagement)) ~ 
                delete(new DeleteAccountHandler(state.accountManagement)) ~
                path("/grants") {
                  post(new CreateAccountGrantHandler(state.accountManagement))
                } ~
                path("/plan") {
                  get(new GetAccountPlanHandler(state.accountManagement)) ~
                  put(new PutAccountPlanHandler(state.accountManagement)) ~ 
                  delete(new DeleteAccountPlanHandler(state.accountManagement))
                }
              } 
            }
          }
        } ->
        shutdown { state => Future[Option[Stoppable]]( None ) }
      }
    }
  }
}
