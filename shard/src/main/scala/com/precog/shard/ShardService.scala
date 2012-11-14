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
package shard

import service._
import ingest.service._
import common.Path
import common.security._
import daze._

import akka.dispatch.Future

import blueeyes.bkka.AkkaDefaults
import blueeyes.bkka.Stoppable
import blueeyes.BlueEyesServiceBuilder
import blueeyes.core.data.{ BijectionsChunkJson, BijectionsChunkFutureJson, BijectionsChunkString, ByteChunk }
import blueeyes.core.http.{HttpHeaders, HttpRequest, HttpResponse}
import blueeyes.core.service.CustomHttpService
import blueeyes.health.metrics.{eternity}
import blueeyes.json.JValue

import org.streum.configrity.Configuration

import com.weiglewilczek.slf4s.Logging
import scalaz._

case class ShardState(queryExecutor: QueryExecutor[Future], apiKeyManager: APIKeyManager[Future], accessControl: AccessControl[Future])

trait ShardService extends 
    BlueEyesServiceBuilder with 
    ShardServiceCombinators with 
    AkkaDefaults with 
    Logging {
  import BijectionsChunkJson._
  import BijectionsChunkString._
  import BijectionsChunkFutureJson._
  import BijectionsChunkQueryResult._

  implicit val timeout = akka.util.Timeout(120000) //for now

  implicit def M: Monad[Future]

  def queryExecutorFactory(config: Configuration, accessControl: AccessControl[Future]): QueryExecutor[Future]

  def apiKeyManagerFactory(config: Configuration): APIKeyManager[Future]

  val analyticsService = this.service("quirrel", "1.0") {
    requestLogging(timeout) {
      healthMonitor(timeout, List(eternity)) { monitor => context =>
        startup {
          import context._

          
          logger.info("Using config: " + config)
          logger.info("Security config = " + config.detach("security"))

          val apiKeyManager = apiKeyManagerFactory(config.detach("security"))

          logger.trace("apiKeyManager loaded")

          val accessControl = new APIKeyManagerAccessControl(apiKeyManager)

          logger.trace("accessControl loaded")
          
          val queryExecutor = queryExecutorFactory(config.detach("queryExecutor"), accessControl)

          logger.trace("queryExecutor loaded")

          queryExecutor.startup.map { _ =>
            ShardState(
              queryExecutor,
              apiKeyManager,
              accessControl
            )
          }
        } ->
        request { (state: ShardState) =>
          jvalue {
            path("/actors/status") {
              get(new ActorStatusHandler(state.queryExecutor))
            }
          } ~ jsonpcb[QueryResult] {
            apiKey(state.apiKeyManager) {
              dataPath("analytics/fs") {
                query {
                  get(new QueryServiceHandler(state.queryExecutor)) ~
                  // Handle OPTIONS requests internally to simplify the standalone service
                  options {
                    (request: HttpRequest[ByteChunk]) => {
                      (a: APIKeyRecord, p: Path, s: String, o: QueryOptions) => Future {
                        HttpResponse[QueryResult](headers = HttpHeaders(Seq("Allow" -> "GET,POST,OPTIONS", 
                                                                            "Access-Control-Allow-Origin" -> "*",
                                                                            "Access-Control-Allow-Methods" -> "GET, POST, OPTIONS, DELETE",
                                                                            "Access-Control-Allow-Headers" -> "Origin, X-Requested-With, Content-Type, X-File-Name, X-File-Size, X-File-Type, X-Precog-Path, X-Precog-Service, X-Precog-Token, X-Precog-Uuid, Accept")))
                      }
                    }
                  }
                }
              } ~
              dataPath("meta/fs") {
                get(new BrowseServiceHandler(state.queryExecutor, state.accessControl)) ~
                // Handle OPTIONS requests internally to simplify the standalone service
                options {
                  (request: HttpRequest[ByteChunk]) => {
                    (a: APIKeyRecord, p: Path) => Future {
                      HttpResponse[QueryResult](headers = HttpHeaders(Seq("Allow" -> "GET,POST,OPTIONS", 
                                                                          "Access-Control-Allow-Origin" -> "*",
                                                                          "Access-Control-Allow-Methods" -> "GET, POST, OPTIONS, DELETE",
                                                                          "Access-Control-Allow-Headers" -> "Origin, X-Requested-With, Content-Type, X-File-Name, X-File-Size, X-File-Type, X-Precog-Path, X-Precog-Service, X-Precog-Token, X-Precog-Uuid, Accept")))
                    }
                  }
                }
              }
            } ~ path("actors/status") {
              get(new ActorStatusHandler(state.queryExecutor))
            }
          }
        } ->
        shutdown { state =>
          for {
            shardShutdown <- state.queryExecutor.shutdown()
            _             <- state.apiKeyManager.close()
          } yield {
            logger.info("Shard system clean shutdown: " + shardShutdown)            
            Option.empty[Stoppable]
          }
        }
      }
    }
  }
}
