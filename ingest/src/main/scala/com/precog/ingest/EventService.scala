package com.precog.ingest

import service._
import com.precog.common.accounts._
import com.precog.common.client._
import com.precog.common.ingest._
import com.precog.common.jobs._
import com.precog.common.security._
import com.precog.common.security.service._
import com.precog.common.services._

import akka.dispatch.Future
import akka.dispatch.ExecutionContext
import akka.util.Timeout

import blueeyes.BlueEyesServiceBuilder
import blueeyes.bkka.Stoppable
import blueeyes.core.data._
import blueeyes.core.data.DefaultBijections.jvalueToChunk
import blueeyes.core.http._
import blueeyes.core.service._
import blueeyes.core.service.RestPathPattern._
import blueeyes.core.service.engines.HttpClientXLightWeb
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
import java.io.File

import scalaz._
import scalaz.std.function._
import scalaz.syntax.monad._
import scalaz.syntax.std.option._

import java.util.concurrent.{ArrayBlockingQueue, ExecutorService, ThreadPoolExecutor, TimeUnit}
import com.precog.common.Path

object EventService {
  case class State(
    accessControl: APIKeyFinder[Future],
    ingestHandler: IngestServiceHandler,
    dataHandler: IngestServiceHandler,
    fileCreateHandler: FileStoreHandler,
    archiveHandler: ArchiveServiceHandler[ByteChunk],
    shardClient: HttpClient[ByteChunk],
    stop: Stoppable
  )

  case class ServiceConfig(
    serviceLocation: ServiceLocation,
    shardLocation: ServiceLocation,
    ingestTimeout: Timeout,
    ingestBatchSize: Int,
    ingestMaxFields: Int,
    ingestTmpDir: File, 
    deleteTimeout: Timeout
  )

  object ServiceConfig {
    def fromConfiguration(config: Configuration) = {
      (ServiceLocation.fromConfig(config.detach("eventService")) |@| ServiceLocation.fromConfig(config.detach("shard"))) { (serviceLoc, shardLoc) =>
        ServiceConfig(
          serviceLocation = serviceLoc,
          shardLocation = shardLoc,
          ingestTimeout = akka.util.Timeout(config[Long]("insert.timeout", 10000l)),
          ingestBatchSize = config[Int]("ingest.batch_size", 500),
          ingestMaxFields = config[Int]("ingest.max_fields", 1024),
          ingestTmpDir = config.get[String]("ingest.tmpdir").map(new File(_)).orElse(Option(File.createTempFile("ingest.tmpfile", null).getParentFile)).get, //fail fast 
          deleteTimeout = akka.util.Timeout(config[Long]("delete.timeout", 10000l))
        )
      }
    }
  }
}

trait EventService extends BlueEyesServiceBuilder with EitherServiceCombinators with PathServiceCombinators with APIKeyServiceCombinators {
  import EventService._
  implicit def executionContext: ExecutionContext
  implicit def M: Monad[Future]

  def configureEventService(config: Configuration): State

  protected[this] def buildServiceState(
      serviceConfig: ServiceConfig,
      apiKeyFinder: APIKeyFinder[Future],
      permissionsFinder: PermissionsFinder[Future],
      eventStore: EventStore[Future],
      jobManager: JobManager[Response],
      stoppable: Stoppable): State = {
    import serviceConfig._

    val ingestHandler = new IngestServiceHandler(permissionsFinder, jobManager, Clock.System, eventStore, ingestTimeout, ingestBatchSize, ingestMaxFields, ingestTmpDir, AccessMode.Append)
    val dataHandler = new IngestServiceHandler(permissionsFinder, jobManager, Clock.System, eventStore, ingestTimeout, ingestBatchSize, ingestMaxFields, ingestTmpDir, AccessMode.Create)
    val archiveHandler = new ArchiveServiceHandler[ByteChunk](apiKeyFinder, eventStore, Clock.System, deleteTimeout)
    val createHandler = new FileStoreHandler(serviceLocation, jobManager, Clock.System, eventStore, ingestTimeout)
    val shardClient = (new HttpClientXLightWeb).protocol(shardLocation.protocol).host(shardLocation.host).port(shardLocation.port)

    EventService.State(apiKeyFinder, ingestHandler, dataHandler, createHandler, archiveHandler, shardClient, stoppable)
  }

  def eventOptionsResponse = CORSHeaders.apply[JValue, Future](M)

  val eventService = this.service("ingest", "2.0") {
    requestLogging { help("/docs/api") {
      healthMonitor("/health", defaultShutdownTimeout, List(blueeyes.health.metrics.eternity)) { monitor => context =>
        startup {
          import context._
          Future(configureEventService(config))
        } ->
        request { (state: State) =>
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

  def fsService(state: State): AsyncHttpService[ByteChunk, JValue] = {
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

  def dataService(state: State): AsyncHttpService[ByteChunk, JValue] = {
    import FileContent._
    import HttpRequestHandlerImplicits._
    jsonAPIKey(state.accessControl) {
      path("/data") {
        dataPath("/fs") {
          accept(ApplicationJson, XJsonStream) {
            post { state.dataHandler } ~
            put { state.dataHandler } ~
            patch { state.dataHandler }
          } ~ {
            post { state.fileCreateHandler } ~
            put { state.fileCreateHandler }
          }
        }
      }
    }
  }

  def shardProxy(shardClient: HttpClient[ByteChunk]): AsyncHttpService[ByteChunk, ByteChunk] = {
    path("/data/fs/'path") {
      get {
        accept(FileContent.XQuirrelScript) {
          proxy(shardClient) { _ => true }
        }
      }
    }
  }
}
