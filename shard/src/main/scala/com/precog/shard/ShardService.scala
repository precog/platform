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

import com.precog.common.jobs.JobManager
import com.precog.common.Path
import com.precog.common.security._
import com.precog.common.accounts._
import com.precog.daze._

import akka.dispatch.{Future, ExecutionContext}

import blueeyes.util.Clock
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

import java.nio.CharBuffer
import java.nio.ByteBuffer
import java.nio.charset.Charset

import org.streum.configrity.Configuration

import com.weiglewilczek.slf4s.Logging
import scalaz._
import scalaz.syntax.monad._
import scalaz.std.function._

sealed trait BaseShardService extends 
    BlueEyesServiceBuilder with 
    ShardServiceCombinators with 
    Logging {

  type SyncQueryExecutorFactory = QueryExecutorFactory[Future, StreamT[Future, CharBuffer]]

  implicit val timeout = akka.util.Timeout(120000) //for now

  implicit def executionContext: ExecutionContext
  implicit def M: Monad[Future]

  def APIKeyFinder(config: Configuration): APIKeyFinder[Future]
  def AccountFinder(config: Configuration): AccountFinder[Future]

  implicit val err: (HttpFailure, String) => HttpResponse[ByteChunk] = { (failure, s) =>
    HttpResponse(failure, content = Some(ByteChunk(s.getBytes("UTF-8"))))
  }

  private implicit val onError: (HttpFailure, String) => HttpResponse[QueryResult] = { (failure, msg) =>
    HttpResponse[QueryResult](status = failure, content = Some(Left(JString(msg))))
  }

  private val queryResultToByteChunk: QueryResult => ByteChunk = {
    (qr: QueryResult) => qr match {
      case Left(jv) => Left(ByteBuffer.wrap(jv.renderCompact.getBytes))
      case Right(stream) => Right(stream.map(cb => utf8.encode(cb)))
    }
  }

  def optionsResponse = M.point(
    HttpResponse[ByteChunk](headers = HttpHeaders(Seq("Allow" -> "GET,POST,OPTIONS",
      "Access-Control-Allow-Origin" -> "*",
      "Access-Control-Allow-Methods" -> "GET, POST, OPTIONS, DELETE",
      "Access-Control-Allow-Headers" -> "Origin, X-Requested-With, Content-Type, X-File-Name, X-File-Size, X-File-Type, X-Precog-Path, X-Precog-Service, X-Precog-Token, X-Precog-Uuid, Accept")))
  )

  val utf8 = Charset.forName("UTF-8")

  def asyncQueryService(queryExecutorFactory: AsyncQueryExecutorFactory, apiKeyFinder: APIKeyFinder[Future], jobManager: JobManager[Future], clock: Clock) = {
    apiKey(k => apiKeyFinder.findAPIKey(k).map(_.map(_.apiKey))) {
      path("/analytics/queries") {
        path("'jobId") {
          get(new AsyncQueryResultServiceHandler(jobManager)) ~ 
          delete(new QueryDeleteHandler[ByteChunk](jobManager, clock))
        } ~
        asyncQuery {
          post {
            new AsyncQueryServiceHandler(queryExecutorFactory.asynchronous) map { 
              //function4, future, httpreponse
              _ map { _ map { _ map queryResultToByteChunk } }
            }
          }
        }
      }
    }
  }

  def basicQueryService(queryExecutorFactory: SyncQueryExecutorFactory, apiKeyFinder: APIKeyFinder[Future]) = {
    jsonp[ByteChunk] {
      transcode[ByteChunk, JValue] {
        path("/actors/status") {
          get(new ActorStatusHandler(queryExecutorFactory))
        } 
      } ~ 
      apiKey(k => apiKeyFinder.findAPIKey(k).map(_.map(_.apiKey))) {
        dataPath("analytics/fs") {
          query {
            get { 
              new SyncQueryServiceHandler(queryExecutorFactory) map {
                //function4, future, httpreponse
                _ map { _ map { _ map queryResultToByteChunk } }
              }
            } ~
            options {
              (request: HttpRequest[ByteChunk]) => (a: APIKey, p: Path, s: String, o: QueryOptions) => optionsResponse 
            }
          }
        } ~
        dataPath("/meta/fs") {
          (get {
            new BrowseServiceHandler[ByteChunk](queryExecutorFactory, apiKeyFinder) map {
              //function2, future, httpreponse
              _ map { _ map { _ map queryResultToByteChunk } }
            }
          } ~
          options {
            (request: HttpRequest[ByteChunk]) => (a: APIKey, p: Path) => optionsResponse 
          }): HttpService[ByteChunk, (APIKey, Path) => Future[HttpResponse[ByteChunk]]]
        }
      }
    }
  }
}

trait ShardService extends BaseShardService {
  case class State(queryExecutorFactory: SyncQueryExecutorFactory, apiKeyFinder: APIKeyFinder[Future])

  def QueryExecutorFactory(config: Configuration,
                           apiKeyFinder: APIKeyFinder[Future],
                           accountFinder: AccountFinder[Future]): SyncQueryExecutorFactory

  val analyticsService = this.service("quirrel", "1.0") {
    requestLogging(timeout) {
      healthMonitor(timeout, List(eternity)) { monitor => context =>
        startup {
          import context._

          logger.info("Using config: " + config)

          val apiKeyFinder = APIKeyFinder(config.detach("security"))
          val accountFinder = AccountFinder(config.detach("accounts"))
          val queryExecutorFactory = QueryExecutorFactory(config.detach("queryExecutor"), apiKeyFinder, accountFinder)

          queryExecutorFactory.startup.map { _ =>
            State(queryExecutorFactory, apiKeyFinder)
          }
        } ->
        request { case State(queryExecutorFactory, apiKeyFinder) =>
          basicQueryService(queryExecutorFactory, apiKeyFinder)
        } ->
        stop { state: State =>
          Stoppable.fromFuture(state.queryExecutorFactory.shutdown())
        }
      }
    }
  }
}

trait AsyncShardService extends BaseShardService {
  case class State(
    queryExecutorFactory: AsyncQueryExecutorFactory,
    apiKeyFinder: APIKeyFinder[Future],
    jobManager: JobManager[Future],
    clock: Clock)

  def clock: Clock

  def JobManager(config: Configuration): JobManager[Future] 

  def QueryExecutorFactory(config: Configuration,
                           accessControl: AccessControl[Future],
                           accountFinder: AccountFinder[Future],
                           jobManager: JobManager[Future]): AsyncQueryExecutorFactory

  val analyticsService = this.service("quirrel", "1.0") {
    requestLogging(timeout) {
      healthMonitor(timeout, List(eternity)) { monitor => context =>
        startup {
          import context._

          val apiKeyFinder = APIKeyFinder(config.detach("security"))
          val accountFinder = AccountFinder(config.detach("accounts"))
          val jobManager = JobManager(config.detach("jobs"))
          val queryExecutorFactory = QueryExecutorFactory(config.detach("queryExecutor"), apiKeyFinder, accountFinder, jobManager)

          queryExecutorFactory.startup.map { _ =>
            State(queryExecutorFactory, apiKeyFinder, jobManager, clock)
          }
        } ->
        request { case State(queryExecutorFactory, apiKeyFinder, jobManager, clock) =>
          asyncQueryService(queryExecutorFactory, apiKeyFinder, jobManager, clock) ~
          basicQueryService(queryExecutorFactory, apiKeyFinder)
        } ->
        stop { state: State =>
          Stoppable.fromFuture(state.queryExecutorFactory.shutdown())
        }
      }
    }
  }
}

