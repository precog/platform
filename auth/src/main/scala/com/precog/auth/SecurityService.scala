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

import akka.dispatch.Future
import akka.dispatch.ExecutionContext

import blueeyes.BlueEyesServiceBuilder
import blueeyes.bkka.Stoppable
import blueeyes.core.data.{DefaultBijections, ByteChunk}
import blueeyes.health.metrics.eternity
import ByteChunk._

import org.streum.configrity.Configuration
import scalaz._

case class SecurityServiceState(apiKeyManagement: APIKeyManagement)

trait SecurityService extends BlueEyesServiceBuilder with APIKeyServiceCombinators {
  import DefaultBijections._

  val insertTimeout = akka.util.Timeout(10000)
  implicit val timeout = akka.util.Timeout(120000) //for now

  implicit def executionContext: ExecutionContext
  implicit val M: Monad[Future]

  def APIKeyManager(config: Configuration): APIKeyManager[Future]

  val securityService = service("security", "1.0") {
    requestLogging(timeout) {
      healthMonitor(timeout, List(eternity)) { monitor => context =>
        startup {
          import context._
          val securityConfig = config.detach("security")
          val apiKeyManager = APIKeyManager(securityConfig)
          M.point(SecurityServiceState(new APIKeyManagement(apiKeyManager)))
        } ->
        request { (state: SecurityServiceState) =>
          jsonp[ByteChunk] {
            transcode {
              apiKey(state.apiKeyManagement.apiKeyManager) {
                path("/apikeys/") {
                  get(new GetAPIKeysHandler(state.apiKeyManagement)) ~
                  post(new CreateAPIKeyHandler(state.apiKeyManagement)) ~
                  path("'apikey") {
                    get(new GetAPIKeyDetailsHandler(state.apiKeyManagement)) ~
                    delete(new DeleteAPIKeyHandler(state.apiKeyManagement)) ~
                    path("/grants/") {
                      get(new GetAPIKeyGrantsHandler(state.apiKeyManagement)) ~
                      post(new AddAPIKeyGrantHandler(state.apiKeyManagement)) ~
                      path("'grantId") {
                        delete(new RemoveAPIKeyGrantHandler(state.apiKeyManagement))
                      }
                    }
                  }
                } ~
                path("/grants/") {
                  post(new CreateGrantHandler(state.apiKeyManagement)) ~
                  path("'grantId") {
                    get(new GetGrantDetailsHandler(state.apiKeyManagement)) ~
                    delete(new DeleteGrantHandler(state.apiKeyManagement)) ~
                    path("/children/") {
                      get(new GetGrantChildrenHandler(state.apiKeyManagement)) ~
                      post(new AddGrantChildHandler(state.apiKeyManagement))
                    }
                  }
                }
              }
            }
          }
        } ->
        stop { state => 
          Stoppable.fromFuture(state.apiKeyManagement.close())
        }
      }
    }
  }
}
