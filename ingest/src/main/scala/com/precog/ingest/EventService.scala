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
import blueeyes.health.metrics.{eternity}
import blueeyes.json._
import blueeyes.util.Clock
import blueeyes.json.JValue
import DefaultBijections._
import MimeTypes._
import ByteChunk._

import com.google.common.util.concurrent.ThreadFactoryBuilder

import org.streum.configrity.Configuration

import scalaz._
import scalaz.syntax.monad._

import java.util.concurrent.{ArrayBlockingQueue, ExecutorService, ThreadPoolExecutor, TimeUnit}
import com.precog.common.Path

case class EventServiceState(accessControl: APIKeyFinder[Future], ingestHandler: IngestServiceHandler, archiveHandler: ArchiveServiceHandler[ByteChunk], stop: Stoppable)

case class EventServiceDeps[M[+_]](
    apiKeyFinder: APIKeyFinder[M],
    accountFinder: AccountFinder[M],
    eventStore: EventStore[M],
    jobManager: JobManager[({type λ[+α] = ResponseM[M, α]})#λ])

trait EventService extends BlueEyesServiceBuilder with EitherServiceCombinators with PathServiceCombinators with APIKeyServiceCombinators with DecompressCombinators {
  implicit def executionContext: ExecutionContext
  implicit def M: Monad[Future]

  def configure(config: Configuration): (EventServiceDeps[Future], Stoppable)

  //TODO: COPY & PASTE from ShardService !!!!!!!
  def optionsResponse = M.point(
    HttpResponse[ByteChunk](headers = HttpHeaders(Seq("Allow" -> "GET,POST,OPTIONS",
      "Access-Control-Allow-Origin" -> "*",
      "Access-Control-Allow-Methods" -> "GET, POST, OPTIONS, DELETE",
      "Access-Control-Allow-Headers" -> "Origin, X-Requested-With, Content-Type, X-File-Name, X-File-Size, X-File-Type, X-Precog-Path, X-Precog-Service, X-Precog-Token, X-Precog-Uuid, Accept")))
  )

  val eventService = this.service("ingest", "1.0") {
    requestLogging {
      healthMonitor(defaultShutdownTimeout, List(blueeyes.health.metrics.eternity)) { monitor => context =>
        startup {
          Future {
            import context._

            val (deps, stoppable) = configure(config)

            val ingestTimeout = akka.util.Timeout(config[Long]("insert.timeout", 10000l))
            val ingestBatchSize = config[Int]("ingest.batch_size", 500)

            val deleteTimeout = akka.util.Timeout(config[Long]("delete.timeout", 10000l))

            val ingestHandler = new IngestServiceHandler(deps.accountFinder, deps.apiKeyFinder, deps.jobManager, Clock.System, deps.eventStore, ingestTimeout, ingestBatchSize)
            val archiveHandler = new ArchiveServiceHandler[ByteChunk](deps.apiKeyFinder, deps.eventStore, deleteTimeout)

            EventServiceState(deps.apiKeyFinder, ingestHandler, archiveHandler, stoppable)
          }
        } ->
        request { (state: EventServiceState) =>
          decompress {
            jsonp[ByteChunk] {
              (jsonAPIKey(state.accessControl) {
                dataPath("/fs") {
                  post(state.ingestHandler) ~
                  delete(state.archiveHandler)
                } ~ //legacy handler
                path("/(?<sync>a?sync)") {
                  dataPath("/fs") {
                    post(state.ingestHandler) ~
                    delete(state.archiveHandler)
                  }
                } ~ //for legacy labcoat compatibility
                path("/ingest/(?<sync>a?sync)") {
                  dataPath("/fs") {
                    post(state.ingestHandler) ~
                    delete(state.archiveHandler) ~
                    options {
                      (request: HttpRequest[ByteChunk]) => (a: APIKey, p: Path) => optionsResponse
                    }
                  }
                }
              })
            }
          }
        } ->
        stop { state =>
          state.stop
        }
      }
    }
  }
}
