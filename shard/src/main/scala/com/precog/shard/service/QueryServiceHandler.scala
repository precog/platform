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
package service

import com.precog.daze._
import com.precog.common._
import com.precog.common.ingest.FileContent

import com.precog.common.security._
import com.precog.common.jobs._
import com.precog.muspelheim._
import com.precog.yggdrasil.table.Slice
import com.precog.yggdrasil.vfs._
import com.precog.util.InstantOrdering

import blueeyes.core.data._
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._
import blueeyes.json._
import blueeyes.json.serialization.SerializationImplicits._
import blueeyes.util.Clock

import akka.dispatch.Future
import akka.dispatch.ExecutionContext

import com.weiglewilczek.slf4s.Logging

import java.nio.CharBuffer
import java.io.{ StringWriter, PrintWriter }

import scala.math.Ordered._
import scalaz._
import scalaz.NonEmptyList.nels
import scalaz.Validation.{ success, failure }
import scalaz.std.stream._
import scalaz.std.option._
import scalaz.std.anyVal._
import scalaz.syntax.apply._
import scalaz.syntax.applicative._
import scalaz.syntax.monad._
import scalaz.syntax.semigroup._
import scalaz.syntax.traverse._
import scalaz.syntax.std.option._


abstract class QueryServiceHandler[A](implicit M: Monad[Future])
    extends CustomHttpService[ByteChunk, (APIKey, Path, String, QueryOptions) => Future[HttpResponse[QueryResult]]] with Logging {

  def platform: Platform[Future, A]
  def extractResponse(request: HttpRequest[_], a: A, outputType: MimeType): Future[HttpResponse[QueryResult]]

  private val Command = """:(\w+)\s+(.+)""".r

  private def handleErrors[A](query: String, result: EvaluationError): HttpResponse[QueryResult] = result match {
    case SystemError(error) =>
      logger.error("An error occurred processing the query: " + query, error)
      HttpResponse[QueryResult](HttpStatus(InternalServerError, "A problem was encountered processing your query. We're looking into it!"))

    case InvalidStateError(error) =>
      HttpResponse[QueryResult](HttpStatus(PreconditionFailed, error))
  }

  private def appendHeaders[A](opts: QueryOptions)(response: HttpResponse[A]): HttpResponse[A] = {
    import blueeyes.core.http.HttpHeaders._
    import blueeyes.core.http.DispositionTypes._
    import blueeyes.core.http.MimeTypes._

    opts.output match {
      case FileContent.TextCSV => response.copy(headers = response.headers + `Content-Type`(text/csv) + `Content-Disposition`(attachment(Some("results.csv"))))
      case _ => response.copy(headers = response.headers + `Content-Type`(application/json))
    }
  }

  val service = (request: HttpRequest[ByteChunk]) => {
    success { (apiKey: APIKey, path: Path, query: String, opts: QueryOptions) =>
      platform.executorFor(apiKey) flatMap {
        case Success(executor) =>
          executor.execute(apiKey, query, path, opts) flatMap {
            case Success(result) =>
              extractResponse(request, result, opts.output) map (appendHeaders(opts))
            case Failure(error) =>
              handleErrors(query, error).point[Future]
          }

        case Failure(error) =>
          logger.error("Failure during evaluator setup: " + error)
          HttpResponse[QueryResult](HttpStatus(InternalServerError, "A problem was encountered processing your query. We're looking into it!")).point[Future]
      }
    }
  }

  def metadata = DescriptionMetadata("""Takes a quirrel query and returns the result of evaluating the query.""")
}

class AnalysisServiceHandler(storedQueries: StoredQueries[Future], clock: Clock)(implicit M: Monad[Future])
    extends CustomHttpService[ByteChunk, (APIKey, Path) => Future[HttpResponse[QueryResult]]] with Logging {
  import blueeyes.core.http.HttpHeaders._
  import blueeyes.core.http.CacheDirectives.{ `max-age`, `no-cache`, `only-if-cached`, `max-stale` }

  val service = (request: HttpRequest[ByteChunk]) => {
    ShardServiceCombinators.queryOpts(request) map { queryOptions =>
      { (apiKey: APIKey, path: Path) =>
        val cacheDirectives = request.headers.header[`Cache-Control`].toSeq.flatMap(_.directives)
        logger.debug("Received analysis request with cache directives: " + cacheDirectives)
        // Internally maxAge/maxStale are compared against ms times
        val maxAge = cacheDirectives.collectFirst { case `max-age`(Some(n)) => n.number * 1000 }
        val maxStale = cacheDirectives.collectFirst { case `max-stale`(Some(n)) => n.number * 1000 }
        val cacheable = cacheDirectives exists { _ != `no-cache`}
        val onlyIfCached = cacheDirectives exists { _ == `only-if-cached`}
        storedQueries.executeStoredQuery(apiKey, path, queryOptions, maxAge |+| maxStale, maxAge, cacheable, onlyIfCached) map {
          case Success(StoredQueryResult(stream, cachedAt)) =>
            val headers = cachedAt map { lastTime =>
              HttpHeaders(`Last-Modified`(HttpDateTimes.StandardDateTime(lastTime.toDateTime)))
            } getOrElse HttpHeaders()
            HttpResponse(OK, headers = headers, content = Some(Right(Resource.toCharBuffers(queryOptions.output, stream))))
          case Failure(evaluationError) =>
            logger.error("Evaluation errors prevented returning results from stored query: " + evaluationError)
            HttpResponse(InternalServerError)
        }
      }
    }
  }

  def metadata = DescriptionMetadata("""Returns the result of executing a stored query.""")
}

sealed trait SyncResultFormat
object SyncResultFormat {
  case object Simple extends SyncResultFormat
  case object Detailed extends SyncResultFormat
}

class SyncQueryServiceHandler(
    val platform: Platform[Future, (Option[JobId], StreamT[Future, Slice])],
    jobManager: JobManager[Future],
    defaultFormat: SyncResultFormat)(implicit
    M: Monad[Future]) extends QueryServiceHandler[(Option[JobId], StreamT[Future, Slice])] {

  import scalaz.syntax.std.option._
  import scalaz.std.option._
  import JobManager._

  def ensureTermination(data0: StreamT[Future, CharBuffer]) =
    TerminateJson.ensure(silenceShardQueryExceptions(data0))

  def extractResponse(request: HttpRequest[_], result: (Option[JobId], StreamT[Future, Slice]), outputType: MimeType): Future[HttpResponse[QueryResult]] = {
    import SyncResultFormat._

    val (jobId, slices) = result
    val charBuffers = Resource.toCharBuffers(outputType, slices)

    val format = request.parameters get 'format map {
      case "simple" => Right(Simple)
      case "detailed" => Right(Detailed)
      case badFormat => Left("unknown format '%s'" format badFormat)
    } getOrElse Right(defaultFormat)

    (format, jobId, charBuffers) match {
      case (Left(msg), _, _) =>
        HttpResponse[QueryResult](HttpStatus(BadRequest, msg)).point[Future]

      case (Right(Simple), None, data) =>
        HttpResponse[QueryResult](OK, content = Some(Right(ensureTermination(data)))).point[Future]

      case (Right(Simple), Some(jobId), data) =>
        val errorsM = jobManager.listMessages(jobId, channels.Error, None)
        errorsM map { errors =>
          if (errors.isEmpty) {
            HttpResponse[QueryResult](OK, content = Some(Right(ensureTermination(data))))
          } else {
            val json = JArray(errors.toList map (_.value))
            HttpResponse[QueryResult](BadRequest, content = Some(Left(json)))
          }
        }

      case (Right(Detailed), None, data0) =>
        val data = ensureTermination(data0)
        val prefix = CharBuffer.wrap("""{"errors":[],"warnings":[],"serverWarnings":[{"message":"Job service is down; errors/warnings are disabled."}],"data":""")
        val result: StreamT[Future, CharBuffer] = (prefix :: data) ++ (CharBuffer.wrap("}") :: StreamT.empty[Future, CharBuffer])
        HttpResponse[QueryResult](OK, content = Some(Right(result))).point[Future]

      case (Right(Detailed), Some(jobId), data0) =>
        val prefix = CharBuffer.wrap("""{ "data" : """)
        val data = ensureTermination(data0)
        val result = StreamT.unfoldM(some(prefix :: data)) {
          case Some(stream) =>
            stream.uncons flatMap {
              case Some((buffer, tail)) =>
                M.point(Some((buffer, Some(tail))))
              case None =>
                val warningsM = jobManager.listMessages(jobId, channels.Warning, None)
                val errorsM = jobManager.listMessages(jobId, channels.Error, None)
                val serverErrorsM = jobManager.listMessages(jobId, channels.ServerError, None)
                val serverWarningsM = jobManager.listMessages(jobId, channels.ServerWarning, None)
                (warningsM |@| errorsM |@| serverErrorsM |@| serverWarningsM) { (warnings, errors, serverErrors, serverWarnings) =>
                  val suffix = """, "errors": %s, "warnings": %s, "serverErrors": %s, "serverWarnings": %s }""" format (
                    JArray(errors.toList map (_.value)).renderCompact,
                    JArray(warnings.toList map (_.value)).renderCompact,
                    JArray(serverErrors.toList map (_.value)).renderCompact,
                    JArray(serverWarnings.toList map (_.value)).renderCompact
                  )
                  Some((CharBuffer.wrap(suffix), None))
                }
            }

          case None =>
            M.point(None)
        }

        HttpResponse[QueryResult](OK, content = Some(Right(result))).point[Future]
    }
  }

  /**
   * This will catch ShardQuery-specific exceptions in a Future (ie.
   * QueryCancelledException and QueryExpiredException) and recover the Future
   * by simply terminating the stream normally.
   */
  private def silenceShardQueryExceptions(stream0: StreamT[Future, CharBuffer]): StreamT[Future, CharBuffer] = {
    def loop(stream: StreamT[Future, CharBuffer]): StreamT[Future, CharBuffer] = {
      StreamT(stream.uncons map {
        case Some((s, tail)) => StreamT.Yield(s, loop(tail))
        case None => StreamT.Done
      } recover {
        case _: QueryCancelledException =>
          StreamT.Done
        case _: QueryExpiredException =>
          StreamT.Done
        case ex =>
          val msg = new StringWriter()
          ex.printStackTrace(new PrintWriter(msg))
          logger.error("Error executing shard query:\n" + msg.toString())
          StreamT.Done
      })
    }

    loop(stream0)
  }
}

class AsyncQueryServiceHandler(val platform: Platform[Future, JobId])(implicit M: Monad[Future]) extends QueryServiceHandler[JobId] {
  def extractResponse(request: HttpRequest[_], jobId: JobId, outputType: MimeType): Future[HttpResponse[QueryResult]] = {
    val result = JObject(JField("jobId", JString(jobId)) :: Nil)
    HttpResponse[QueryResult](Accepted, content = Some(Left(result))).point[Future]
  }
}
