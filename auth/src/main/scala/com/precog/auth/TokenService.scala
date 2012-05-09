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
package com.precog
package auth

import common.security._

import blueeyes.BlueEyesServer

import akka.dispatch.Future

import blueeyes.BlueEyesServiceBuilder
import blueeyes.bkka.AkkaDefaults
import blueeyes.bkka.Stoppable
import blueeyes.core.data.{BijectionsChunkJson, BijectionsChunkFutureJson, BijectionsChunkString, ByteChunk}
import blueeyes.health.metrics.{eternity}

import org.streum.configrity.Configuration

case class TokenServiceState(tokenManager: TokenManager, accessControl: AccessControl)

trait TokenService extends BlueEyesServiceBuilder with AkkaDefaults with TokenServiceCombinators {
  import BijectionsChunkJson._
  import BijectionsChunkString._
  import BijectionsChunkFutureJson._

  val insertTimeout = akka.util.Timeout(10000)
  implicit val timeout = akka.util.Timeout(120000) //for now

  def tokenManagerFactory(config: Configuration): TokenManager

  val tokenService = service("auth", "1.0") {
    requestLogging(timeout) {
      healthMonitor(timeout, List(eternity)) { monitor => context =>
        startup {
          import context._
          val tokenManager = tokenManagerFactory(config.detach("security"))

          val accessControl = new TokenManagerAccessControl(tokenManager)

          Future(TokenServiceState(tokenManager, accessControl))
        } ->
        request { (state: TokenServiceState) =>
          jsonp[ByteChunk] {
            token(state.tokenManager) {
              path("/token") {
                get(new GetTokenHandler(state.tokenManager)) ~
                post(new AddTokenHandler(state.tokenManager)) ~
                path("/grants") {
                  get(new GetGrantsHandler(state.tokenManager)) ~
                  post(new AddGrantHandler(state.tokenManager)) ~
                  path("/'grantId") {
                    delete(new RemoveGrantHandler(state.tokenManager)) ~
                    path("/children") {
                      get(new GetGrantChildrenHandler(state.tokenManager)) ~
                      post(new AddGrantChildrenHandler(state.tokenManager)) ~
                      path("/'childGrantId") {
                        get(new GetGrantChildHandler(state.tokenManager)) ~
                        delete(new RemoveGrantChildHandler(state.tokenManager))
                      }
                    }
                  }
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
