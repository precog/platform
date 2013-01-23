package com.precog.shard

import service._

import com.precog.common.jobs.JobManager
import com.precog.common.Path
import com.precog.common.security._
import com.precog.common.accounts._
import com.precog.daze._

import akka.dispatch.{Future, ExecutionContext, Promise}

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

import java.nio.{ CharBuffer, ByteBuffer }
import java.nio.charset.Charset

import org.streum.configrity.Configuration

import com.weiglewilczek.slf4s.Logging
import scalaz._
import scalaz.syntax.monad._
import scalaz.std.function._


trait ShardService extends 
    BlueEyesServiceBuilder with 
    ShardServiceCombinators with 
    Logging {

  sealed trait ShardState {
    def queryExecutorFactory: QueryExecutorFactory[Future, StreamT[Future, CharBuffer]]
    def apiKeyFinder: APIKeyFinder[Future]
  }

  case class ManagedQueryShardState(
    queryExecutorFactory: ManagedQueryExecutorFactory,
    apiKeyFinder: APIKeyFinder[Future],
    //accountFinder: AccountFinder[Future],
    jobManager: JobManager[Future],
    clock: Clock) extends ShardState

  case class BasicShardState(
    queryExecutorFactory: QueryExecutorFactory[Future, StreamT[Future, CharBuffer]],
    apiKeyFinder: APIKeyFinder[Future]) extends ShardState

    implicit val timeout = akka.util.Timeout(120000) //for now

    implicit def executionContext: ExecutionContext
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

  // TODO: maybe some of these implicits should be moved, but for now i
  // don't have the patience to figure out where.

  //implicit def err: (HttpFailure, String) => HttpResponse[ByteChunk] = { (failure, s) =>
  //  HttpResponse(failure, content = Some(ByteChunk(s.getBytes("UTF-8"))))
  //}

  val utf8 = Charset.forName("UTF-8")

  def optionsResponse = M.point(
    HttpResponse[ByteChunk](headers = HttpHeaders(Seq("Allow" -> "GET,POST,OPTIONS",
      "Access-Control-Allow-Origin" -> "*",
      "Access-Control-Allow-Methods" -> "GET, POST, OPTIONS, DELETE",
      "Access-Control-Allow-Headers" -> "Origin, X-Requested-With, Content-Type, X-File-Name, X-File-Size, X-File-Type, X-Precog-Path, X-Precog-Service, X-Precog-Token, X-Precog-Uuid, Accept")))
  )

  private implicit val onQRError: (HttpFailure, String) => HttpResponse[QueryResult] = { (failure, msg) =>
    HttpResponse[QueryResult](status = failure, content = Some(Left(JString(msg))))
  }

  private implicit val onChunkError: (HttpFailure, String) => HttpResponse[ByteChunk] = { (failure, msg) =>
    HttpResponse[ByteChunk](status = failure, content = Some(Left(ByteBuffer.wrap(msg.getBytes))))
  }

  private val queryResultToByteChunk: QueryResult => ByteChunk = {
    (qr: QueryResult) => qr match {
      case Left(jv) => Left(ByteBuffer.wrap(jv.renderCompact.getBytes))
      case Right(stream) => Right(stream.map(cb => utf8.encode(cb)))
    }
  }

//  def asyncQueryService(queryExecutorFactory: AsyncQueryExecutorFactory, apiKeyFinder: APIKeyFinder[Future], jobManager: JobManager[Future], clock: Clock) = {
//    apiKey(k => apiKeyFinder.findAPIKey(k).map(_.map(_.apiKey))) {
//      path("/analytics/queries") {
//        path("'jobId") {
//          get(new AsyncQueryResultServiceHandler(jobManager)) ~ 
//          delete(new QueryDeleteHandler[ByteChunk](jobManager, clock))
//        } ~
//        asyncQuery {
//          post {
//            new AsyncQueryServiceHandler(queryExecutorFactory.asynchronous) map { 
//              //function4, future, httpreponse
//              _ map { _ map { _ map queryResultToByteChunk } }
//            }
//          }
//        }
//  implicit def failureToQueryResult: (HttpFailure, String) => HttpResponse[QueryResult] = { (fail: HttpFailure, msg: String) =>
//    HttpResponse[QueryResult](status = fail, content = Some(Left(JString(msg))))
//  }
//
//  implicit def futureJValueToFutureQueryResult: Future[HttpResponse[JValue]] => Future[HttpResponse[QueryResult]] = {
//    (fr: Future[HttpResponse[JValue]]) => fr.map { r => 
//      r.copy(content = r.content.map(Left(_)))
//    }
//  }
//
//  lazy val utf8 = Charset.forName("UTF-8")
//
//  implicit def queryResultToFutureByteChunk: QueryResult => Future[ByteChunk] = {
//    (qr: QueryResult) => qr match {
//      case Left(jv) => Future(Left(ByteBuffer.wrap(jv.renderCompact.getBytes)))
//      case Right(stream) => Future(Right(stream.map(cb => utf8.encode(cb))))
//    }
//  }
//
//  implicit def futureByteChunk2byteChunk: Future[ByteChunk] => ByteChunk = { fb =>
//    Right(StreamT.wrapEffect[Future, ByteBuffer] {
//      fb map {
//        case Left(buffer) => buffer :: StreamT.empty[Future, ByteBuffer]
//        case Right(stream) => stream
//      }
//    })
//  }
//
//  implicit def queryResult2byteChunk = futureByteChunk2byteChunk compose queryResultToFutureByteChunk
//
//  // I feel dirty inside.
//  implicit def futureMapper[A, B](implicit a2b: A => B): Future[HttpResponse[A]] => Future[HttpResponse[B]] = { fa =>
//    fa map { res => res.copy(content = res.content map a2b) }
//  }

  private def asyncQueryService(state: ShardState): QueryServiceHandler.Service = state match {
    case BasicShardState(_, _) =>
      new QueryServiceNotAvailable

    case ManagedQueryShardState(queryExecutorFactory, _, _, _) =>
      new AsyncQueryServiceHandler(queryExecutorFactory.asynchronous)
  }

  private def syncQueryService(state: ShardState): QueryServiceHandler.Service = state match {
    case BasicShardState(queryExecutorFactory, _) =>
      new BasicQueryServiceHandler(queryExecutorFactory)

    case ManagedQueryShardState(queryExecutorFactory, _, jobManager, _) =>
      new SyncQueryServiceHandler(queryExecutorFactory.synchronous, jobManager, SyncResultFormat.Simple)
  }

  private def asyncHandler(state: ShardState) = {
    val queryHandler: HttpService[Future[JValue], Future[HttpResponse[QueryResult]]] = {
      apiKey(k => state.apiKeyFinder.findAPIKey(k).map(_.map(_.apiKey))) {
        path("/analytics/queries") {
          asyncQuery(post(asyncQueryService(state)))
        }
      }
    }

    state match {
      case ManagedQueryShardState(_, apiKeyFinder, jobManager, clock) =>
        // [ByteChunk, Future[HttpResponse[ByteChunk]]]
        apiKey(k => apiKeyFinder.findAPIKey(k).map(_.map(_.apiKey))) {
          path("/analytics/queries") {
            path("'jobId") {
              get(new AsyncQueryResultServiceHandler(jobManager)) ~
              delete(new QueryDeleteHandler[ByteChunk](jobManager, clock))
            }
          }
        } ~ { 
          queryHandler map { _ map { _ map queryResultToByteChunk } } contramap implicitly[ByteChunk => Future[JValue]]
        }

      case _ =>
        queryHandler map { _ map { _ map queryResultToByteChunk } } contramap implicitly[ByteChunk => Future[JValue]]
    }
  }

  private def syncHandler(state: ShardState) = {
    jsonp[ByteChunk] {
      transcode[ByteChunk, JValue] {
        path("/actors/status") {
          get(new ActorStatusHandler(state.queryExecutorFactory))
        }
      } ~
      apiKey(k => state.apiKeyFinder.findAPIKey(k).map(_.map(_.apiKey))) {
        dataPath("analytics/fs") {
          query {
            get(syncQueryService(state) map { _ map { _ map { _ map queryResultToByteChunk } } }) ~
            options {
              (request: HttpRequest[ByteChunk]) => (a: APIKey, p: Path, s: String, o: QueryOptions) => optionsResponse 
            }
          }
        } ~
        dataPath("meta/fs") {
          get {
            new BrowseServiceHandler[ByteChunk](state.queryExecutorFactory, state.apiKeyFinder) map {
              _ map { _ map { _ map queryResultToByteChunk } }
            }
          } ~
          options {
            (request: HttpRequest[ByteChunk]) => (a: APIKey, p: Path) => optionsResponse 
          }//): HttpService[ByteChunk, (APIKey, Path) => Future[HttpResponse[ByteChunk]]]
        }
      } ~ 
      path("actors/status") {
        transcode[ByteChunk, JValue] {
          get(new ActorStatusHandler(state.queryExecutorFactory))
        }
      }
    }
  }

  lazy val analyticsService = this.service("quirrel", "1.0") {
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
        stop { state: ShardState =>
          Stoppable.fromFuture(state.queryExecutorFactory.shutdown())
        }
      }
    }
  }
}
