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
import com.precog.util.PrecogUnit
import com.precog.muspelheim._

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

sealed trait ShardStateOptions
object ShardStateOptions {
  case object NoOptions extends ShardStateOptions
  case object DisableAsyncQueries extends ShardStateOptions
}

sealed trait ShardState {
  def platform: Platform[Future, StreamT[Future, CharBuffer]]
  def apiKeyManager: APIKeyManager[Future]
  def stoppable: Stoppable
}

case class ManagedQueryShardState(
  platform: ManagedPlatform,
  apiKeyManager: APIKeyManager[Future],
  accountManager: BasicAccountManager[Future],
  jobManager: JobManager[Future],
  clock: Clock,
  options: ShardStateOptions = ShardStateOptions.NoOptions,
  stoppable: Stoppable) extends ShardState

case class BasicShardState(
  platform: Platform[Future, StreamT[Future, CharBuffer]],
  apiKeyManager: APIKeyManager[Future],
  stoppable: Stoppable) extends ShardState

trait ShardService extends
    BlueEyesServiceBuilder with
    ShardServiceCombinators with
    Logging {

  import ShardStateOptions.DisableAsyncQueries

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
   * On service startup, the platform's `startup` method will be
   * called.
   */
  def configureShardState(config: Configuration): Future[ShardState]

  def optionsResponse[A] = Promise.successful(
    HttpResponse[A](
      headers = HttpHeaders(Seq(
        "Allow" -> "GET,POST,OPTIONS",
        "Access-Control-Allow-Origin" -> "*",
        "Access-Control-Allow-Methods" -> "GET, POST, OPTIONS, DELETE",
        "Access-Control-Allow-Headers" -> "Origin, X-Requested-With, Content-Type, X-File-Name, X-File-Size, X-File-Type, X-Precog-Path, X-Precog-Service, X-Precog-Token, X-Precog-Uuid, Accept")
      )
    )
  )

  // TODO: maybe some of these implicits should be moved, but for now i
  // don't have the patience to figure out where.

  implicit def err: (HttpFailure, String) => HttpResponse[ByteChunk] = { (failure, s) =>
    HttpResponse(failure, content = Some(ByteChunk(JString(s.getBytes("UTF-8")).renderCompact)))
  }

  implicit def failureToQueryResult: (HttpFailure, String) => HttpResponse[QueryResult] = { (fail: HttpFailure, msg: String) =>
    HttpResponse[QueryResult](status = fail, content = Some(Left(JString(msg))))
  }

  implicit def futureJValueToFutureQueryResult: Future[HttpResponse[JValue]] => Future[HttpResponse[QueryResult]] = {
    (fr: Future[HttpResponse[JValue]]) => fr.map { r =>
      r.copy(content = r.content.map(Left(_)))
    }
  }

  lazy val utf8 = Charset.forName("UTF-8")

  implicit def queryResultToFutureByteChunk: QueryResult => Future[ByteChunk] = {
    (qr: QueryResult) => qr match {
      case Left(jv) => Future(Left(ByteBuffer.wrap(jv.renderCompact.getBytes)))
      case Right(stream) => Future(Right(stream.map(cb => utf8.encode(cb))))
    }
  }

  implicit def futureByteChunk2byteChunk: Future[ByteChunk] => ByteChunk = { fb =>
    Right(StreamT.wrapEffect[Future, ByteBuffer] {
      fb map {
        case Left(buffer) => buffer :: StreamT.empty[Future, ByteBuffer]
        case Right(stream) => stream
      }
    })
  }

  implicit def queryResult2byteChunk = futureByteChunk2byteChunk compose queryResultToFutureByteChunk

  // I feel dirty inside.
  implicit def futureMapper[A, B](implicit a2b: A => B): Future[HttpResponse[A]] => Future[HttpResponse[B]] = { fa =>
    fa map { res => res.copy(content = res.content map a2b) }
  }

  private def asyncQueryService(state: ShardState) = state match {
    case BasicShardState(_, _, _) | ManagedQueryShardState(_, _, _, _, _, DisableAsyncQueries, _) =>
      new QueryServiceNotAvailable
    case ManagedQueryShardState(platform, _, _, _, _, _, _) =>
      new AsyncQueryServiceHandler(platform.asynchronous)
  }

  private def syncQueryService(state: ShardState) = state match {
    case BasicShardState(platform, _, _) =>
      new BasicQueryServiceHandler(platform)
    case ManagedQueryShardState(platform, _, _, jobManager, _, _, _) =>
      new SyncQueryServiceHandler(platform.synchronous, jobManager, SyncResultFormat.Simple)
  }

  private def asyncHandler(state: ShardState) = {
    val queryHandler: HttpService[Future[JValue], Future[HttpResponse[QueryResult]]] = 
      path("/analytics") {
        apiKey(state.apiKeyManager) {
          path("/queries") {
            asyncQuery(post(asyncQueryService(state)))
          }
        }
      }

    state match {
      case ManagedQueryShardState(_, apiKeyManager, _, jobManager, clock, _, _) =>
        path("/analytics") {
          apiKey[ByteChunk, HttpResponse[ByteChunk]](apiKeyManager) {
            path("/queries") {
              path("'jobId") {
                get(new AsyncQueryResultServiceHandler(jobManager)) ~
                delete(new QueryDeleteHandler(jobManager, clock))
              }
            }
          } 
        } ~ queryHandler

      case _ =>
        queryHandler map {
          _ map { response => response.copy(content = response.content map queryResult2byteChunk) }
        } contramap {
          implicitly[ByteChunk => Future[JValue]]
        }
    }
  }

  private def syncHandler(state: ShardState) = {
    implicit def lift[A](a: A): Future[A] = Promise.successful(a)

    jsonpcb[ByteChunk] {
      apiKey(state.apiKeyManager) {
        dataPath("analytics/fs") {
          query {
            get(syncQueryService(state)) ~
            options {   // Handle OPTIONS requests internally to simplify the standalone service
              (request: HttpRequest[ByteChunk]) => {
                (a: APIKeyRecord, p: Path, s: String, o: QueryOptions) => optionsResponse[QueryResult]
              }
            }
          } map { f => 
            (a: APIKeyRecord, p: Path) => f(a, p) map { r => r.copy(content = r.content map queryResult2byteChunk) } 
          }
        } ~
        dataPath("meta/fs") {
          {
            get(new BrowseServiceHandler(state.platform.metadataClient, state.apiKeyManager)) map { f => 
              (a: APIKeyRecord, p: Path) => f(a,p) map { r => r.copy(content = r.content map jvalueToChunk) } 
            }
          } ~
          // Handle OPTIONS requests internally to simplify the standalone service
          options {
            (request: HttpRequest[ByteChunk]) => {
              (a: APIKeyRecord, p: Path) => optionsResponse[ByteChunk]
            }
          }
        }
      }
    }
  }

  lazy val analyticsService = this.service("quirrel", "1.0") {
    requestLogging(timeout) {
      healthMonitor(timeout, List(eternity)) { monitor => context =>
        startup {
          logger.info("Starting shard with config:\n" + context.config)
          configureShardState(context.config)
        } ->
        request { state =>
          asyncHandler(state) ~ syncHandler(state)
        } ->
        shutdown { state =>
          Stoppable.stoppableStop.stop(state.stoppable)
        }
      }
    }
  }
}
