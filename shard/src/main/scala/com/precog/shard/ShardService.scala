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

import com.precog.util.PrecogUnit
import com.precog.muspelheim._

import com.precog.common.Path
import com.precog.common.accounts._
import com.precog.common.ingest._
import com.precog.common.jobs.JobManager
import com.precog.common.security._
import com.precog.common.services._
import com.precog.daze._
import com.precog.shard.scheduling._
import com.precog.shard.service._
import com.precog.yggdrasil.table.Slice

import akka.actor.ActorRef
import akka.dispatch.{Future, ExecutionContext, Promise}
import akka.util.{Duration, Timeout}

import blueeyes.util.Clock
import blueeyes.json._
import blueeyes.bkka.Stoppable
import blueeyes.core.data._
import blueeyes.core.data.ByteChunk._
import blueeyes.core.http._
import blueeyes.core.http.MimeTypes._
import blueeyes.core.service._
import blueeyes.BlueEyesServiceBuilder

import blueeyes.core.data.{ DefaultBijections, ByteChunk }
import blueeyes.core.http.{HttpHeaders, HttpRequest, HttpResponse}
import blueeyes.core.service.CustomHttpService

import blueeyes.health.metrics.{eternity}
import blueeyes.json.JValue

import DefaultBijections._

import java.util.Arrays
import java.nio.{ CharBuffer, ByteBuffer }
import java.nio.charset.{ Charset, CoderResult }

import org.streum.configrity.Configuration

import com.weiglewilczek.slf4s.Logging
import scalaz._
import scalaz.syntax.monad._
import scalaz.std.function._

sealed trait ShardStateOptions
object ShardStateOptions {
  case object NoOptions extends ShardStateOptions
  case object DisableAsyncQueries extends ShardStateOptions
}

case class ShardState(
    platform: ManagedPlatform,
    apiKeyFinder: APIKeyFinder[Future],
    accountFinder: AccountFinder[Future],
    storedQueries: StoredQueries[Future],
    scheduler: Scheduler[Future],
    jobManager: JobManager[Future],
    clock: Clock,
    stoppable: Stoppable,
    options: ShardStateOptions = ShardStateOptions.NoOptions) {
}

trait ShardService extends
    BlueEyesServiceBuilder with
    ShardServiceCombinators with
    Logging {

  import ShardStateOptions.DisableAsyncQueries

  implicit def executionContext: ExecutionContext
  implicit def M: Monad[Future]

  val timeout = Timeout(120000)
  /**
   * This provides the configuration for the service and expects a `ShardState`
   * in return. The exact `ShardState` can be either a `BasicShardState` or a
   * all features, including async queries and detailed query output are
   * enabled. Otherwise, if only a `BasicShardState` is returned, then only
   * basic synchronous queries will be allowed.
   *
   * On service startup, the platform's `startup` method will be
   * called.
   */
  def configureShardState(config: Configuration): Future[ShardState]

  val utf8 = Charset.forName("UTF-8")
  val BufferSize = 64 * 1024

  def optionsResponse = CORSHeaders.apply[ByteChunk, Future](M)

  private def bufferOutput(stream0: StreamT[Future, CharBuffer]) = {
    val encoder = utf8.newEncoder()

    def loop(stream: StreamT[Future, CharBuffer], buf: ByteBuffer, arr: Array[Byte]): StreamT[Future, Array[Byte]] = {
      StreamT(stream.uncons map {
        case Some((cbuf, tail)) =>
          val result = encoder.encode(cbuf, buf, false)
          if (result == CoderResult.OVERFLOW) {
            val arr2 = new Array[Byte](BufferSize)
            StreamT.Yield(arr, loop(cbuf :: tail, ByteBuffer.wrap(arr2), arr2))
          } else {
            StreamT.Skip(loop(tail, buf, arr))
          }

        case None =>
          val result = encoder.encode(CharBuffer.wrap(""), buf, true)
          if (result == CoderResult.OVERFLOW) {
            val arr2 = new Array[Byte](BufferSize)
            StreamT.Yield(arr, loop(stream, ByteBuffer.wrap(arr2), arr2))
          } else {
            StreamT.Yield(Arrays.copyOf(arr, buf.position), StreamT.empty)
          }
      })
    }

    val arr = new Array[Byte](BufferSize)
    loop(stream0, ByteBuffer.wrap(arr), arr)
  }

  private def queryResultToByteChunk: QueryResult => ByteChunk = {
    (qr: QueryResult) => qr match {
      case Left(jv) => Left(jv.renderCompact.getBytes(utf8))
      case Right(stream) => Right(bufferOutput(stream))
    }
  }

  //private def cf = implicitly[ByteChunk => Future[JValue]]

  def shardService[F[+_]](service: HttpService[ByteChunk, F[Future[HttpResponse[QueryResult]]]])(implicit
      F: Functor[F]): HttpService[ByteChunk, F[Future[HttpResponse[ByteChunk]]]] = {
    service map { _ map { _ map { _ map queryResultToByteChunk } } }
  }

  private def asyncHandler(state: ShardState) = {
    path("/analytics") {
      jsonAPIKey(state.apiKeyFinder) {
        path("/queries") {
          path("/'jobId") {
            get(new AsyncQueryResultServiceHandler(state.jobManager)) ~
            delete(new QueryDeleteHandler[ByteChunk](state.jobManager, state.clock))
          } ~
          // async handler *always* returns a JSON object containing the job ID
          shardService[({ type λ[+α] = (APIKey => α) })#λ] {
            asyncQuery(post(new AsyncQueryServiceHandler(state.platform.asynchronous)))
          }
        }
      }
    }
  }

  private def syncHandler(state: ShardState) = {
    val queryService = new SyncQueryServiceHandler(state.platform.synchronous, state.jobManager, SyncResultFormat.Simple)
    jsonp {
      jsonAPIKey(state.apiKeyFinder) {
        dataPath("/analytics/fs") {
          shardService[({ type λ[+α] = ((APIKey, Path) => α) })#λ] {
            query[QueryResult] {
              {
                get { queryService } ~
                post { queryService }
              }
            }
          } ~
          options {
            (request: HttpRequest[ByteChunk]) => (a: APIKey, p: Path) => optionsResponse
          }
        } ~
        dataPath("/meta/fs") {
          get {
            produce(application/json)(
              new BrowseServiceHandler[ByteChunk](state.platform.metadataClient, state.apiKeyFinder) map { _ map { _ map { _ map { jvalueToChunk } } } }
            )(ResponseModifier.responseFG[({ type λ[α] = (APIKey, Path) => α })#λ, Future, ByteChunk])
          } ~
          options {
            (request: HttpRequest[ByteChunk]) => (a: APIKey, p: Path) => optionsResponse
          }
        }
      }
    }
  }

  private def scheduledHandler(state: ShardState) = {
    implicit val actTimeout = timeout

    jsonp {
      jvalue[ByteChunk] {
        path("/scheduled/") {
          jsonAPIKey(state.apiKeyFinder) {
            post { new AddScheduledQueryServiceHandler(state.scheduler, state.apiKeyFinder, state.accountFinder, state.clock) }
          } ~
          path("'scheduleId") {
            get { new ScheduledQueryStatusServiceHandler[Future[JValue]](state.scheduler) } ~
            delete { new DeleteScheduledQueryServiceHandler(state.scheduler) }
          }
        }
      }
    }
  }

  private def analysisHandler[A](state: ShardState) = {
    dataPath("/analysis/fs") {
      get {
        shardService[({ type λ[+α] = (APIKey, Path) => α })#λ] {
          new AnalysisServiceHandler(state.storedQueries, state.clock)
        }
      }
    }
  }

  lazy val analyticsService = this.service("analytics", "2.0") {
    requestLogging(timeout) {
      healthMonitor("/health", timeout, List(eternity)) { monitor => context =>
        startup {
          logger.info("Starting shard with config:\n" + context.config)
          configureShardState(context.config)
        } ->
        request { state =>
          import CORSHeaderHandler.allowOrigin
          allowOrigin("*", executionContext) {
            // TODO: unhack this
              if (state.scheduler.enabled) {
                analysisHandler(state) ~ asyncHandler(state) ~ syncHandler(state) ~ scheduledHandler(state)
              } else {
                analysisHandler(state) ~ asyncHandler(state) ~ syncHandler(state)
              }
          }
        } ->
        stop { state: ShardState =>
          state.stoppable
        }
      }
    }
  }
}
