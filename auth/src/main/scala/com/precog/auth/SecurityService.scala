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
package com.precog.auth

import com.precog.common.security._
import com.precog.common.security.service._
import com.precog.common.services.{CORSHeaderHandler, PathServiceCombinators}

import akka.dispatch.Future
import akka.dispatch.ExecutionContext

import blueeyes.BlueEyesServiceBuilder
import blueeyes.bkka.Stoppable
import blueeyes.core.data.{DefaultBijections, ByteChunk}
import blueeyes.core.http._
import blueeyes.json._
import blueeyes.health.metrics.eternity
import ByteChunk._
import DefaultBijections._

import com.weiglewilczek.slf4s.Logging
import com.weiglewilczek.slf4s.Logger
import org.streum.configrity.Configuration
import scalaz._

trait SecurityService extends BlueEyesServiceBuilder with APIKeyServiceCombinators with PathServiceCombinators with Logging {
  case class State(handlers: SecurityServiceHandlers, stoppable: Stoppable)

  val timeout = akka.util.Timeout(120000) //for now

  implicit def executionContext: ExecutionContext
  implicit def M: Monad[Future]

  def APIKeyManager(config: Configuration): (APIKeyManager[Future], Stoppable)
  def clock: blueeyes.util.Clock

  def securityService = service("security", "1.0") {
    requestLogging(timeout) { help("/docs/api") {
      healthMonitor("/health", timeout, List(eternity)) { monitor => context =>
        startup {
          import context._
          val securityConfig = config.detach("security")
          val (apiKeyManager, stoppable) = APIKeyManager(securityConfig)
          M.point(State(new SecurityServiceHandlers(apiKeyManager, clock), stoppable))
        } ->
        request { case State(handlers, stoppable) =>
          import CORSHeaderHandler.allowOrigin
          import handlers._
          allowOrigin("*", executionContext) {
            jsonp[ByteChunk] {
              produce(MimeTypes.application / MimeTypes.json) {
                transcode {
                  path("/apikeys/'apikey") {
                    get(ReadAPIKeyDetailsHandler) ~
                    delete(DeleteAPIKeyHandler) ~
                    path("/grants/") {
                      get(ReadAPIKeyGrantsHandler) ~
                      post(CreateAPIKeyGrantHandler) ~
                      path("'grantId") {
                        delete(DeleteAPIKeyGrantHandler)
                      }
                    }
                  } ~
                  path("/grants/'grantId") {
                    get(ReadGrantDetailsHandler) ~
                    path("/children/") {
                      get(ReadGrantChildrenHandler)
                    }
                  } ~
                  jsonAPIKey(k => handlers.apiKeyManager.findAPIKey(k).map(_.map(_.apiKey))) {
                    path("/apikeys/") {
                      get(ReadAPIKeysHandler) ~
                      post(CreateAPIKeyHandler)
                    } ~
                    path("/grants/") {
                      get(ReadGrantsHandler) ~
                      post(CreateGrantHandler) ~
                      path("'grantId") {
                        delete(DeleteGrantHandler) ~
                        path("/children/") {
                          post(CreateGrantChildHandler)
                        }
                      }
                    } ~
                    dataPath("/permissions/fs") {
                      get(ReadPermissionsHandler)
                    }
                  }
                }
              }
            } ~ 
            debug[ByteChunk, Future[HttpResponse[ByteChunk]]](logger) {
              import HttpStatusCodes.NotFound
              orFail {
                (r: HttpRequest[ByteChunk]) => (NotFound, "No handler was found for request " + r)
              }
            }
          } 
        } ->
        stop[State] { case State(_, stoppable) =>
          stoppable
        }
      }
    }}
  }
}
