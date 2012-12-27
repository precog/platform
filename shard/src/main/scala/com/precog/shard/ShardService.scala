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
package com.precog.shard

import service._

import com.precog.common.Path
import com.precog.common.security._
import com.precog.common.accounts._
import com.precog.daze._

import akka.dispatch.{Future, ExecutionContext}

import blueeyes.json._
import blueeyes.bkka.Stoppable
import blueeyes.core.data._
import blueeyes.core.data.ByteChunk._
import blueeyes.core.http._
import blueeyes.core.service._
import blueeyes.BlueEyesServiceBuilder

import blueeyes.core.data.{ DefaultBijections, ByteChunk }
import blueeyes.core.http.{HttpHeaders, HttpRequest, HttpResponse}
import blueeyes.core.service.CustomHttpService

import blueeyes.health.metrics.{eternity}
import blueeyes.json.JValue

import DefaultBijections._

import org.streum.configrity.Configuration

import com.weiglewilczek.slf4s.Logging
import scalaz._

case class ShardState(queryExecutor: QueryExecutor[Future], apiKeyFinder: APIKeyFinder[Future])

trait ShardService extends 
    BlueEyesServiceBuilder with 
    ShardServiceCombinators with 
    Logging {

  implicit val timeout = akka.util.Timeout(120000) //for now

  implicit def executionContext: ExecutionContext
  implicit def M: Monad[Future]

  def QueryExecutor(config: Configuration, accessControl: AccessControl[Future], accountManager: AccountFinder[Future]): QueryExecutor[Future]
  def APIKeyFinder(config: Configuration): APIKeyFinder[Future]
  def AccountFinder(config: Configuration): AccountFinder[Future]

  implicit val failureToQueryResult: (HttpFailure, String) => HttpResponse[QueryResult] = 
    (fail: HttpFailure, msg: String) => HttpResponse[QueryResult](status = fail, content = Some(Left(JString(msg))))

  implicit val futureJValueToFutureQueryResult: Future[HttpResponse[JValue]] => Future[HttpResponse[QueryResult]] =
    (_: Future[HttpResponse[JValue]]) map { r => r.copy(content = r.content.map(Left(_))) }

  def optionsResponse = M.point(
    HttpResponse[QueryResult](headers = HttpHeaders(Seq("Allow" -> "GET,POST,OPTIONS",
      "Access-Control-Allow-Origin" -> "*",
      "Access-Control-Allow-Methods" -> "GET, POST, OPTIONS, DELETE",
      "Access-Control-Allow-Headers" -> "Origin, X-Requested-With, Content-Type, X-File-Name, X-File-Size, X-File-Type, X-Precog-Path, X-Precog-Service, X-Precog-Token, X-Precog-Uuid, Accept")))
  )

  import java.nio.ByteBuffer
  import java.nio.charset.Charset
  val utf8 = Charset.forName("UTF-8")

  val analyticsService = this.service("quirrel", "1.0") {
    requestLogging(timeout) {
      healthMonitor(timeout, List(eternity)) { monitor => context =>
        startup {
          import context._
          
          logger.info("Using config: " + config)
          logger.info("Security config = " + config.detach("security"))

          val apiKeyFinder = APIKeyFinder(config.detach("security"))

          logger.trace("apiKeyFinder loaded")

          val accountFinder = AccountFinder(config.detach("accounts"))

          logger.trace("accountManager loaded")

          val queryExecutor = QueryExecutor(config.detach("queryExecutor"), apiKeyFinder, accountFinder)

          logger.trace("queryExecutor loaded")

          queryExecutor.startup.map { _ =>
            ShardState(queryExecutor, apiKeyFinder)
          }
        } ->
        request { (state: ShardState) =>
          jsonp[ByteChunk] {
            {
              apiKey(state.apiKeyFinder) {
                dataPath("analytics/fs") {
                  query {
                    get(new QueryServiceHandler[ByteChunk](state.queryExecutor)) ~
                    // Handle OPTIONS requests internally to simplify the standalone service
                    options {
                      (request: HttpRequest[ByteChunk]) => {
                        (a: APIKey, p: Path, s: String, o: QueryOptions) => optionsResponse
                      }
                    }
                  }
                } ~
                dataPath("meta/fs") {
                  get(new BrowseServiceHandler[ByteChunk](state.queryExecutor, state.apiKeyFinder)) ~
                  // Handle OPTIONS requests internally to simplify the standalone service
                  options {
                    (request: HttpRequest[ByteChunk]) => {
                      (a: APIKey, p: Path) => optionsResponse
                    }
                  }
                }
              } ~ 
              path("actors/status") {
                get(new ActorStatusHandler[ByteChunk](state.queryExecutor))
              }
            } map {
              _ map { 
                _ map {
                  case Left(jv) => Left(ByteBuffer.wrap(jv.renderCompact.getBytes))
                  case Right(stream) => Right(stream.map(cb => utf8.encode(cb)))
                }
              }
            }
          }
        } ->
        shutdown { state =>
          for {
            shardShutdown <- state.queryExecutor.shutdown()
          } yield {
            logger.info("Shard system clean shutdown: " + shardShutdown)            
            Option.empty[Stoppable]
          }
        }
      }
    }
  }
}
