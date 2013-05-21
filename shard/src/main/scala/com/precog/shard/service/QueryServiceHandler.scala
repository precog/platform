package com.precog.shard
package service

import com.precog.daze._
import com.precog.common._

import com.precog.common.security._
import com.precog.common.jobs._
import com.precog.common.accounts._
import com.precog.muspelheim._

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

import org.joda.time.DateTime

import scalaz._
import scalaz.Validation.{ success, failure }
import scalaz.syntax.monad._

final class QueryServiceNotAvailable(implicit M: Monad[Future]) extends CustomHttpService[ByteChunk, (APIKey, AccountDetails, Path, String, QueryOptions) => Future[HttpResponse[QueryResult]]] {
  val service = { (request: HttpRequest[ByteChunk]) =>
    success({ (r: APIKey, a: AccountDetails, p: Path, q: String, opts: QueryOptions) =>
      M.point(HttpResponse(HttpStatus(NotFound, "This service is not available in this version.")))
    })
  }

  val metadata = DescriptionMetadata("Takes a quirrel query and returns the result of evaluating the query.")
}

abstract class QueryServiceHandler[A](implicit M: Monad[Future]) extends CustomHttpService[ByteChunk, (APIKey, AccountDetails, Path, String, QueryOptions) => Future[HttpResponse[QueryResult]]] with Logging {

  def platform: Platform[Future, A]

  def extractResponse(request: HttpRequest[_], a: A): Future[HttpResponse[QueryResult]]

  private val Command = """:(\w+)\s+(.+)""".r

  private def handleErrors[A](qt: String, result: EvaluationError): HttpResponse[QueryResult] = result match {
    case SystemError(error) =>
      error.printStackTrace()
      logger.error("An error occurred processing the query: " + qt, error)
      HttpResponse[QueryResult](HttpStatus(InternalServerError, "A problem was encountered processing your query. We're looking into it!"))

    case InvalidStateError(error) =>
      HttpResponse[QueryResult](HttpStatus(PreconditionFailed, error))
  }

  private def appendHeaders[A](opts: QueryOptions)(response: HttpResponse[A]): HttpResponse[A] = {
    import blueeyes.core.http.HttpHeaders._
    import blueeyes.core.http.DispositionTypes._
    import blueeyes.core.http.MimeTypes._

    opts.output match {
      case CSVOutput => response.copy(headers = response.headers + `Content-Type`(text/csv) + `Content-Disposition`(attachment(Some("results.csv"))))
      case _ => response.copy(headers = response.headers + `Content-Type`(application/json))
    }
  }

  lazy val service = (request: HttpRequest[ByteChunk]) => {
    success((apiKey: APIKey, account: AccountDetails, path: Path, query: String, opts: QueryOptions) => query.trim match {
      case Command("ls", arg) => list(apiKey, Path(arg.trim))
      case Command("list", arg) => list(apiKey, Path(arg.trim))
      case Command("ds", arg) => describe(apiKey, Path(arg.trim))
      case Command("describe", arg) => describe(apiKey, Path(arg.trim))
      case qt =>
        platform.executorFor(apiKey) flatMap { //TODO: executorFor can just take an Account.
          case Success(executor) =>
            val ctx = EvaluationContext(apiKey, account, path, new DateTime)
            executor.execute(query, ctx, opts) flatMap {
              case Success(result) =>
                extractResponse(request, result) map (appendHeaders(opts))
              case Failure(error) =>
                handleErrors(qt, error).point[Future]
            }

          case Failure(error) =>
            logger.error("Failure during evaluator setup: " + error)
            HttpResponse[QueryResult](HttpStatus(InternalServerError, "A problem was encountered processing your query. We're looking into it!")).point[Future]
        }
    })
  }

  def metadata = DescriptionMetadata("""Takes a quirrel query and returns the result of evaluating the query.""")

  def list(apiKey: APIKey, p: Path) = {
    platform.metadataClient.browse(apiKey, p).map {
      case Success(r) => HttpResponse[QueryResult](OK, content = Some(Left(r)))
      case Failure(e) => HttpResponse[QueryResult](BadRequest, content = Some(Left(JString("Error listing path: " + p))))
    }
  }

  def describe(apiKey: APIKey, p: Path) = {
    platform.metadataClient.structure(apiKey, p, CPath.Identity).map {
      case Success(r) => HttpResponse[QueryResult](OK, content = Some(Left(r)))
      case Failure(e) => HttpResponse[QueryResult](BadRequest, content = Some(Left(JString("Error describing path: " + p))))
    }
  }
}

class BasicQueryServiceHandler(
    val platform: Platform[Future, (Set[Fault], StreamT[Future, CharBuffer])])(implicit
    val M: Monad[Future]) extends QueryServiceHandler[(Set[Fault], StreamT[Future, CharBuffer])] {

  def extractResponse(request: HttpRequest[_], result: (Set[Fault], StreamT[Future, CharBuffer])): Future[HttpResponse[QueryResult]] = M.point {
    val (faults, stream) = result
    if (faults.isEmpty) {
      HttpResponse[QueryResult](OK, content = Some(Right(stream)))
    } else {
      val json = JArray(faults.toList map { fault =>
        JObject(
          JField("message", JString(fault.message)) ::
          (fault.pos map { pos =>
            JField("position", pos.serialize) :: Nil
          } getOrElse Nil)
        )
      })
      HttpResponse[QueryResult](BadRequest, content = Some(Left(json)))
    }
  }
}

sealed trait SyncResultFormat
object SyncResultFormat {
  case object Simple extends SyncResultFormat
  case object Detailed extends SyncResultFormat
}

class SyncQueryServiceHandler(
    val platform: Platform[Future, (Option[JobId], StreamT[Future, CharBuffer])],
    jobManager: JobManager[Future],
    defaultFormat: SyncResultFormat)(implicit
    M: Monad[Future]) extends QueryServiceHandler[(Option[JobId], StreamT[Future, CharBuffer])] {

  import scalaz.syntax.std.option._
  import scalaz.std.option._
  import JobManager._

  def ensureTermination(data0: StreamT[Future, CharBuffer]) =
    TerminateJson.ensure(silenceShardQueryExceptions(data0))

  def extractResponse(request: HttpRequest[_], result: (Option[JobId], StreamT[Future, CharBuffer])): Future[HttpResponse[QueryResult]] = {
    import SyncResultFormat._

    val format = request.parameters get 'format map {
      case "simple" => Right(Simple)
      case "detailed" => Right(Detailed)
      case badFormat => Left("unknown format '%s'" format badFormat)
    } getOrElse Right(defaultFormat)

    (format, result._1, result._2) match {
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
  def extractResponse(request: HttpRequest[_], jobId: JobId): Future[HttpResponse[QueryResult]] = {
    val result = JObject(JField("jobId", JString(jobId)) :: Nil)
    HttpResponse[QueryResult](Accepted, content = Some(Left(result))).point[Future]
  }
}
