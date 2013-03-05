package com.precog.ingest

import service._
import com.precog.common.OptionHeaders
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

  def configureEventService(config: Configuration): (EventServiceDeps[Future], Stoppable)

  def eventOptionsResponse = OptionHeaders.apply[JValue, Future](M)

  val eventService = this.service("ingest", "1.0") {
    requestLogging {
      healthMonitor(defaultShutdownTimeout, List(blueeyes.health.metrics.eternity)) { monitor => context =>
        startup {
          Future {
            import context._

            val (deps, stoppable) = configureEventService(config)

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
            jsonp {
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
                      (request: HttpRequest[ByteChunk]) => (a: APIKey, p: Path) => eventOptionsResponse
                    }
                  }
                }
              }) map {
                _ map { _ map jvalueToChunk }
              }
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
