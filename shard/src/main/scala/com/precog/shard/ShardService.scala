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

import com.precog.accounts.BasicAccountManager
import com.precog.common.jobs.JobManager

import service._
import ingest.service._
import common.Path
import common.security._
import daze._

import akka.dispatch.{Future, Promise}

import blueeyes.util.Clock
import blueeyes.json._
import blueeyes.bkka.{AkkaDefaults, Stoppable}
import blueeyes.core.data._
import blueeyes.core.http._
import blueeyes.core.service._
import blueeyes.BlueEyesServiceBuilder

import blueeyes.core.data.{ DefaultBijections, ByteChunk }
import blueeyes.core.http.{HttpHeaders, HttpRequest, HttpResponse}
import blueeyes.core.service.CustomHttpService

import blueeyes.health.metrics.{eternity}
import blueeyes.json.JValue

import DefaultBijections._

import java.nio.{ CharBuffer, ByteBuffer }
import java.nio.charset.Charset

import org.streum.configrity.Configuration

import com.weiglewilczek.slf4s.Logging
import scalaz._

sealed trait ShardState {
  def queryExecutorFactory: QueryExecutorFactory[Future, StreamT[Future, CharBuffer]]
  def apiKeyManager: APIKeyManager[Future]
}

case class ManagedQueryShardState(
  queryExecutorFactory: ManagedQueryExecutorFactory,
  apiKeyManager: APIKeyManager[Future],
  accountManager: BasicAccountManager[Future],
  jobManager: JobManager[Future],
  clock: Clock) extends ShardState

case class BasicShardState(
  queryExecutorFactory: QueryExecutorFactory[Future, StreamT[Future, CharBuffer]],
  apiKeyManager: APIKeyManager[Future]) extends ShardState


trait ShardService extends 
    BlueEyesServiceBuilder with 
    ShardServiceCombinators with 
    Logging {

  implicit val timeout = akka.util.Timeout(120000) //for now

  implicit def M: Monad[Future]

  /**
   * This provides the configuration for the service and expects a `ShardState`
   * in return. The exact `ShardState` can be either a `BasicShardState` or a
   * `ManagedQueryShardState`. If a `ManagedQueryShardState` is returned, then
   * all features, including async queries and detailed query output are
   * enabled. Otherwise, if only a `BasicShardState` is returned, then only
   * basic synchronous queries will be allowed.
   *
   * On service startup, the queryExecutorFactory's `startup` method will be
   * called.
   */
  def configureShardState(config: Configuration): ShardState


  def optionsResponse = Promise.successful(
    HttpResponse[QueryResult](headers = HttpHeaders(Seq("Allow" -> "GET,POST,OPTIONS",
      "Access-Control-Allow-Origin" -> "*",
      "Access-Control-Allow-Methods" -> "GET, POST, OPTIONS, DELETE",
      "Access-Control-Allow-Headers" -> "Origin, X-Requested-With, Content-Type, X-File-Name, X-File-Size, X-File-Type, X-Precog-Path, X-Precog-Service, X-Precog-Token, X-Precog-Uuid, Accept")))
  )

  // TODO: maybe some of these implicits should be moved, but for now i
  // don't have the patience to figure out where.

  implicit val err: (HttpFailure, String) => HttpResponse[ByteChunk] = { (failure, s) =>
    HttpResponse(failure, content = Some(ByteChunk(s.getBytes("UTF-8"))))
  }

  implicit val failureToQueryResult:
      (HttpFailure, String) => HttpResponse[QueryResult] =
    (fail: HttpFailure, msg: String) =>
  HttpResponse[QueryResult](status = fail, content = Some(Left(JString(msg))))

  implicit val futureJValueToFutureQueryResult:
      Future[HttpResponse[JValue]] => Future[HttpResponse[QueryResult]] =
    (fr: Future[HttpResponse[JValue]]) => fr.map { r => 
      r.copy(content = r.content.map(Left(_)))
    }

  val utf8 = Charset.forName("UTF-8")

  implicit val queryResultToFutureByteChunk: QueryResult => Future[ByteChunk] = {
    (qr: QueryResult) => qr match {
      case Left(jv) => Future(Left(ByteBuffer.wrap(jv.renderCompact.getBytes)))
      case Right(stream) => Future(Right(stream.map(cb => utf8.encode(cb))))
    }
  }

  implicit val futureByteChunk2byteChunk: Future[ByteChunk] => ByteChunk = { fb =>
    Right(StreamT.wrapEffect[Future, ByteBuffer] {
      fb map {
        case Left(buffer) => buffer :: StreamT.empty[Future, ByteBuffer]
        case Right(stream) => stream
      }
    })
  }

  implicit val queryResult2byteChunk = futureByteChunk2byteChunk compose queryResultToFutureByteChunk

  // I feel dirty inside.
  implicit def futureMapper[A, B](implicit a2b: A => B): Future[HttpResponse[A]] => Future[HttpResponse[B]] = { fa =>
    fa map { res => res.copy(content = res.content map a2b) }
  }

  private def asyncQueryService(state: ShardState) = state match {
    case BasicShardState(_, _) =>
      new QueryServiceNotAvailable
    case ManagedQueryShardState(queryExecutorFactory, _, _, _, _) =>
      new AsyncQueryServiceHandler(queryExecutorFactory.asynchronous)
  }

  private def syncQueryService(state: ShardState) = state match {
    case BasicShardState(queryExecutorFactory, _) =>
      new BasicQueryServiceHandler(queryExecutorFactory)
    case ManagedQueryShardState(queryExecutorFactory, _, _, jobManager, _) =>
      new SyncQueryServiceHandler(queryExecutorFactory.synchronous, jobManager, SyncResultFormat.Simple)
  }

  private def asyncHandler(state: ShardState) = {
    val queryHandler: HttpService[Future[JValue], Future[HttpResponse[QueryResult]]] = apiKey(state.apiKeyManager) {
      path("/analytics/queries") {
        asyncQuery(post(asyncQueryService(state)))
      }
    }

    // ByteChunk -> HttpResponse[ByteChunk]
    // Future[JValue] -> StreamT[Future, CharBuffer]
    state match {
      case ManagedQueryShardState(_, apiKeyManager, _, jobManager, clock) =>
        // [ByteChunk, Future[HttpResponse[ByteChunk]]]
        apiKey[ByteChunk, HttpResponse[ByteChunk]](apiKeyManager) {
          path("/analytics/queries") {
            path("'jobId") {
              get(new AsyncQueryResultServiceHandler(jobManager)) ~
              delete(new QueryDeleteHandler(jobManager, clock))
            }
          }
        } ~ queryHandler

      case _ =>
        val x: HttpService[Future[JValue], Future[HttpResponse[ByteChunk]]] = queryHandler map (_ map { response =>
          response.copy(content = response.content map queryResult2byteChunk)
        })
        val f: ByteChunk => Future[JValue] = implicitly
        x contramap f
    }
  }

  private def syncHandler(state: ShardState) = {
    jvalue[ByteChunk] {
      path("/actors/status") {
        get(new ActorStatusHandler(state.queryExecutorFactory))
      }
    } ~ jsonpcb[QueryResult] {
      apiKey(state.apiKeyManager) {
        dataPath("analytics/fs") {
          query {
            get(syncQueryService(state)) ~
            options {   // Handle OPTIONS requests internally to simplify the standalone service
              (request: HttpRequest[ByteChunk]) => {
                (a: APIKeyRecord, p: Path, s: String, o: QueryOptions) => optionsResponse
              }
            }
          }
        } ~
        dataPath("meta/fs") {
          get(new BrowseServiceHandler(state.queryExecutorFactory, state.apiKeyManager)) ~
          // Handle OPTIONS requests internally to simplify the standalone service
          options {
            (request: HttpRequest[ByteChunk]) => {
              (a: APIKeyRecord, p: Path) => optionsResponse
            }
          }
        }
      } ~ path("actors/status") {
        get(new ActorStatusHandler(state.queryExecutorFactory))
      }
    }
  }

  val analyticsService = this.service("quirrel", "1.0") {
    requestLogging(timeout) {
      healthMonitor(timeout, List(eternity)) { monitor => context =>
        startup {
          logger.info("Starting shard with config:\n" + context.config)
          val state = configureShardState(context.config)
          state.queryExecutorFactory.startup map { _ => state }
        } ->
        request { state =>
          asyncHandler(state) ~ syncHandler(state)
        } ->
        shutdown { state =>
          for {
            shutdownState <- state.queryExecutorFactory.shutdown()
            _ <- state.apiKeyManager.close()
          } yield {
            logger.info("Stopped shard: " + shutdownState)
            Option.empty[Stoppable]
          }
        }
      }
    }
  }
}
