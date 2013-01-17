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

  implicit val failureToQueryResult: (HttpFailure, String) => HttpResponse[QueryResult] = { (failure, msg) =>
    HttpResponse[QueryResult](status = failure, content = Some(Left(JString(msg))))
  }

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
  /*
  implicit def futureMapper[A, B](implicit a2b: A => B): Future[HttpResponse[A]] => Future[HttpResponse[B]] = { fa =>
    fa map { res => res.copy(content = res.content map a2b) }
  }
  */

  def optionsResponse = M.point(
    HttpResponse[QueryResult](headers = HttpHeaders(Seq("Allow" -> "GET,POST,OPTIONS",
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
          delete(new QueryDeleteHandler(jobManager, clock))
        } ~
        asyncQuery {
          post(new AsyncQueryServiceHandler(queryExecutorFactory.asynchronous))
        }
      }
    }
  }

  def basicQueryService(queryExecutorFactory: SyncQueryExecutorFactory, apiKeyFinder: APIKeyFinder[Future]) = {
    jsonp[ByteChunk] {
      path("/actors/status") {
        get(new ActorStatusHandler(queryExecutorFactory))
      } ~ 
      transcode {
        apiKey(k => apiKeyFinder.findAPIKey(k).map(_.map(_.apiKey))) {
          dataPath("analytics/fs") {
            query {
              get(new SyncQueryServiceHandler(queryExecutorFactory)) ~
              options {
                (request: HttpRequest[ByteChunk]) => (a: APIKey, p: Path, s: String, o: QueryOptions) => optionsResponse 
              }
            }
          } ~
          dataPath("/meta/fs") {
            get(new BrowseServiceHandler[ByteChunk](queryExecutorFactory, apiKeyFinder)) ~
            options {
              (request: HttpRequest[ByteChunk]) => (a: APIKey, p: Path) => optionsResponse 
            }
          }
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

