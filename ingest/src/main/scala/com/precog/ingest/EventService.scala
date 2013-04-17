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
package com.precog.ingest

import service._
import com.precog.common.accounts._
import com.precog.common.client._
import com.precog.common.jobs._
import com.precog.common.security._
import com.precog.common.security.service._
import com.precog.common.services._

import akka.dispatch.Future
import akka.dispatch.ExecutionContext

import blueeyes.BlueEyesServiceBuilder
import blueeyes.bkka.Stoppable
import blueeyes.core.data._
import blueeyes.core.data.DefaultBijections.jvalueToChunk
import blueeyes.core.http._
import blueeyes.core.service._
import blueeyes.core.service.RestPathPattern._
import blueeyes.health.metrics.{eternity}
import blueeyes.json._
import blueeyes.util.Clock
import blueeyes.json.JValue
import DefaultBijections._
import MimeTypes._
import ByteChunk._

import com.google.common.util.concurrent.ThreadFactoryBuilder

import org.streum.configrity.Configuration
import org.joda.time.Instant

import scalaz._
import scalaz.std.function._
import scalaz.syntax.monad._
import scalaz.syntax.std.option._

import java.util.concurrent.{ArrayBlockingQueue, ExecutorService, ThreadPoolExecutor, TimeUnit}
import com.precog.common.Path

case class EventServiceState(
  accessControl: APIKeyFinder[Future], 
  ingestHandler: IngestServiceHandler, 
  fileCreateHandler: FileCreateHandler,
  archiveHandler: ArchiveServiceHandler[ByteChunk], 
  shardClient: HttpClient[ByteChunk],
  stop: Stoppable)

case class EventServiceDeps[M[+_]](
    apiKeyFinder: APIKeyFinder[M],
    accountFinder: AccountFinder[M],
    eventStore: EventStore[M],
    jobManager: JobManager[({type λ[+α] = ResponseM[M, α]})#λ],
    shardClient: HttpClient[ByteChunk])

object EventService {
  val X_QUIRREL_SCRIPT = MimeType("text", "x-quirrel-script")
}

trait EventService extends BlueEyesServiceBuilder with EitherServiceCombinators with PathServiceCombinators with APIKeyServiceCombinators {
  import EventService._
  implicit def executionContext: ExecutionContext
  implicit def M: Monad[Future]

  def configureEventService(config: Configuration): (EventServiceDeps[Future], Stoppable)

  def eventOptionsResponse = CORSHeaders.apply[JValue, Future](M)

  val eventService = this.service("ingest", "2.0") {
    requestLogging { help("/docs/api") {
      healthMonitor("/health", defaultShutdownTimeout, List(blueeyes.health.metrics.eternity)) { monitor => context =>
        startup {
          import context._
          Future {
            val (deps, stoppable) = configureEventService(config)
            val permissionsFinder = new PermissionsFinder(deps.apiKeyFinder, deps.accountFinder, new Instant(config[Long]("ingest.timestamp_required_after", 1363327426906L)))

            val ingestTimeout = akka.util.Timeout(config[Long]("insert.timeout", 10000l))
            val ingestBatchSize = config[Int]("ingest.batch_size", 500)
            val ingestMaxFields = config[Int]("ingest.max_fields", 1024) // Because kjn says so

            val deleteTimeout = akka.util.Timeout(config[Long]("delete.timeout", 10000l))

            val ingestHandler = new IngestServiceHandler(permissionsFinder, deps.jobManager, Clock.System, deps.eventStore, ingestTimeout, ingestBatchSize, ingestMaxFields)
            val archiveHandler = new ArchiveServiceHandler[ByteChunk](deps.apiKeyFinder, deps.eventStore, Clock.System, deleteTimeout)
            val createHandler = new FileCreateHandler(Clock.System)

            EventServiceState(deps.apiKeyFinder, ingestHandler, createHandler, archiveHandler, deps.shardClient, stoppable)
          }
        } ->
        request { (state: EventServiceState) =>
          import CORSHeaderHandler.allowOrigin
          implicit val FR = M.compose[({ type l[a] = Function2[APIKey, Path, a] })#l] 

          allowOrigin("*", executionContext) {
            encode[ByteChunk, Future[HttpResponse[JValue]], Future[HttpResponse[ByteChunk]]] {
              produce(application / json) {
                //jsonp {
                  fsService(state) ~
                  dataService(state)
                //}
              }
            } ~ 
            shardProxy(state.shardClient)
          }
        } ->
        stop { state =>
          state.stop
        }
      }
    }}
  }

  def fsService(state: EventServiceState): AsyncHttpService[ByteChunk, JValue] = {
    jsonAPIKey(state.accessControl) {
      dataPath("/fs") {
        post(state.ingestHandler) ~
        delete(state.archiveHandler)
      } ~ //legacy handler
      path("/(?<sync>a?sync)") {
        dataPath("/fs") {
          post(state.ingestHandler) ~
          delete(state.archiveHandler)
        }
      }
    }
  }

  def dataService(state: EventServiceState): AsyncHttpService[ByteChunk, JValue] = {
    import HttpRequestHandlerImplicits._
    jsonAPIKey(state.accessControl) {
      path("/data") {
        dataPath("/fs") {
          post {
            accept(application/json, MimeType("application", "x-json-stream")) {
              state.ingestHandler
            } ~
            accept(X_QUIRREL_SCRIPT) {
              state.fileCreateHandler
            }
          } ~ 
          patch {
            accept(application/json, MimeType("application", "x-json-stream")) {
              state.ingestHandler
            }
          } 
        } 
      }
    }
  }

  def shardProxy(shardClient: HttpClient[ByteChunk]): AsyncHttpService[ByteChunk, ByteChunk] = {
    path("/data/fs/'path") {
      get {
        accept(X_QUIRREL_SCRIPT) {
          proxy(shardClient) { _ => true }
        }
      }
    } 
  }
}
