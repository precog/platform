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

import java.nio.CharBuffer

import org.streum.configrity.Configuration

import com.weiglewilczek.slf4s.Logging
import scalaz._

case class ShardState(queryExecutorFactory: AsyncQueryExecutorFactory, apiKeyManager: APIKeyManager[Future], jobManager: JobManager[Future])

trait ShardService extends 
    BlueEyesServiceBuilder with 
    ShardServiceCombinators with 
    Logging {

  implicit val timeout = akka.util.Timeout(120000) //for now

  implicit def M: Monad[Future]

  def queryExecutorFactoryFactory(config: Configuration,
    accessControl: AccessControl[Future],
    accountManager: BasicAccountManager[Future],
    jobManager: JobManager[Future]): AsyncQueryExecutorFactory // [Future, StreamT[Future, CharBuffer]]

  def apiKeyManagerFactory(config: Configuration): APIKeyManager[Future]

  def accountManagerFactory(config: Configuration): BasicAccountManager[Future]

  def jobManagerFactory(config: Configuration): JobManager[Future]

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

  def optionsResponse = Promise.successful(
    HttpResponse[QueryResult](headers = HttpHeaders(Seq("Allow" -> "GET,POST,OPTIONS",
      "Access-Control-Allow-Origin" -> "*",
      "Access-Control-Allow-Methods" -> "GET, POST, OPTIONS, DELETE",
      "Access-Control-Allow-Headers" -> "Origin, X-Requested-With, Content-Type, X-File-Name, X-File-Size, X-File-Type, X-Precog-Path, X-Precog-Service, X-Precog-Token, X-Precog-Uuid, Accept")))
  )

  import java.nio.ByteBuffer
  import java.nio.charset.Charset

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

  val analyticsService = this.service("quirrel", "1.0") {
    requestLogging(timeout) {
      healthMonitor(timeout, List(eternity)) { monitor => context =>
        startup {
          import context._

          
          logger.info("Using config: " + config)
          logger.info("Security config = " + config.detach("security"))

          val apiKeyManager = apiKeyManagerFactory(config.detach("security"))

          logger.trace("apiKeyManager loaded")

          val accountManager = accountManagerFactory(config.detach("accounts"))

          logger.trace("accountManager loaded")

          val jobManager = jobManagerFactory(config.detach("jobs"))

          logger.trace("jobManager loaded")

          val queryExecutorFactory = queryExecutorFactoryFactory(config.detach("queryExecutor"), apiKeyManager, accountManager, jobManager)

          logger.trace("queryExecutorFactory loaded")

          queryExecutorFactory.startup.map { _ =>
            ShardState(
              queryExecutorFactory,
              apiKeyManager,
              jobManager
            )
          }
        } ->
        request { case ShardState(queryExecutorFactory, apiKeyManager, jobManager) =>
          apiKey[ByteChunk, HttpResponse[ByteChunk]](apiKeyManager) {
            path("/analytics/queries") {
              path("'jobId") {
                get(new AsyncQueryServiceHandler(jobManager))
              }
            }
          } ~
          apiKey(apiKeyManager) {
            path("/analytics/queries") {
              asyncQuery {
                post(new QueryServiceHandler(queryExecutorFactory))
              }
            }
          } ~
          jvalue[ByteChunk] {
            path("/actors/status") {
              get(new ActorStatusHandler(queryExecutorFactory))
            }
          } ~ jsonpcb[QueryResult] {
            apiKey(apiKeyManager) {
              dataPath("analytics/fs") {
                query {
                  get(new QueryServiceHandler(queryExecutorFactory)) ~
                  // Handle OPTIONS requests internally to simplify the standalone service
                  options {
                    (request: HttpRequest[ByteChunk]) => {
                      (a: APIKeyRecord, p: Path, s: String, o: QueryOptions) => optionsResponse
                    }
                  }
                }
              } ~
              dataPath("meta/fs") {
                get(new BrowseServiceHandler(queryExecutorFactory, apiKeyManager)) ~
                // Handle OPTIONS requests internally to simplify the standalone service
                options {
                  (request: HttpRequest[ByteChunk]) => {
                    (a: APIKeyRecord, p: Path) => optionsResponse
                  }
                }
              }
            } ~ path("actors/status") {
              get(new ActorStatusHandler(queryExecutorFactory))
            }
          }
        } ->
        shutdown { state =>
          for {
            shardShutdown <- state.queryExecutorFactory.shutdown()
            _ <- state.apiKeyManager.close()
          } yield {
            logger.info("Shard system clean shutdown: " + shardShutdown)
            Option.empty[Stoppable]
          }
        }
      }
    }
  }
}
