package com.precog.shard
package service

import com.precog.daze._
import com.precog.common._
import com.precog.common.security._
import com.precog.common.jobs._

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

import scalaz._
import scalaz.Validation.{ success, failure }
import scalaz.syntax.monad._

final class QueryServiceNotAvailable(implicit M: Monad[Future])
    extends CustomHttpService[Future[JValue], (APIKey, Path, String, QueryOptions) => Future[HttpResponse[QueryResult]]] {
  val service = { (request: HttpRequest[Future[JValue]]) =>
    success({ (r: APIKey, p: Path, q: String, opts: QueryOptions) =>
      M.point(HttpResponse(HttpStatus(NotFound, "This service is not available in this version.")))
    })
  }

  val metadata = Some(DescriptionMetadata("Takes a quirrel query and returns the result of evaluating the query."))
}

object QueryServiceHandler {
  type Service = HttpService[Future[JValue], (APIKey, Path, String, QueryOptions) => Future[HttpResponse[QueryResult]]] 
}

abstract class QueryServiceHandler[A](implicit M: Monad[Future])
    extends CustomHttpService[Future[JValue], (APIKey, Path, String, QueryOptions) => Future[HttpResponse[QueryResult]]] with Logging {

  def queryExecutorFactory: QueryExecutorFactory[Future, A]
  def extractResponse(request: HttpRequest[Future[JValue]], a: A): HttpResponse[QueryResult]

  private val Command = """:(\w+)\s+(.+)""".r

  private def handleErrors[A](qt: String, result: EvaluationError): HttpResponse[QueryResult] = result match {
    case UserError(errorData) =>
      HttpResponse[QueryResult](UnprocessableEntity, content = Some(Left(errorData)))
  
    case AccessDenied(reason) =>
      HttpResponse[QueryResult](HttpStatus(Unauthorized, reason))
  
    case TimeoutError =>
      HttpResponse[QueryResult](RequestEntityTooLarge)
  
    case SystemError(error) =>
      error.printStackTrace()
      logger.error("An error occurred processing the query: " + qt, error)
      HttpResponse[QueryResult](HttpStatus(InternalServerError, "A problem was encountered processing your query. We're looking into it!"))

    case InvalidStateError(error) =>
      HttpResponse[QueryResult](HttpStatus(PreconditionFailed, error))
  }

  lazy val service = (request: HttpRequest[Future[JValue]]) => {
    success((apiKey: APIKey, path: Path, query: String, opts: QueryOptions) => query.trim match {
      case Command("ls", arg) => list(apiKey, Path(arg.trim))
      case Command("list", arg) => list(apiKey, Path(arg.trim))
      case Command("ds", arg) => describe(apiKey, Path(arg.trim))
      case Command("describe", arg) => describe(apiKey, Path(arg.trim))
      case qt =>
        val executorV = queryExecutorFactory.executorFor(apiKey)
        executorV flatMap {
          case Success(executor) =>
            executor.execute(apiKey, query, path, opts) map {
              case Success(result) =>
                extractResponse(request, result)
              case Failure(error) =>
                handleErrors(qt, error)
            }

          case Failure(error) =>
            logger.error("Failure during evaluator setup: " + error)
            M.point(HttpResponse[QueryResult](HttpStatus(InternalServerError, "A problem was encountered processing your query. We're looking into it!")))
        }
    })
  }

  def metadata = Some(DescriptionMetadata(
    """
Takes a quirrel query and returns the result of evaluating the query.
    """
  ))

  
  def list(apiKey: APIKey, p: Path) = {
    queryExecutorFactory.browse(apiKey, p).map {
      case Success(r) => HttpResponse[QueryResult](OK, content = Some(Left(r)))
      case Failure(e) => HttpResponse[QueryResult](BadRequest, content = Some(Left(JString("Error listing path: " + p))))
    }
  }

  def describe(apiKey: APIKey, p: Path) = {
    queryExecutorFactory.structure(apiKey, p).map {
      case Success(r) => HttpResponse[QueryResult](OK, content = Some(Left(r)))
      case Failure(e) => HttpResponse[QueryResult](BadRequest, content = Some(Left(JString("Error describing path: " + p))))
    }
  }
}

class BasicQueryServiceHandler(val queryExecutorFactory: BasicQueryExecutorFactory[Future])(implicit M: Monad[Future])
    extends QueryServiceHandler[StreamT[Future, CharBuffer]] {
  def extractResponse(request: HttpRequest[Future[JValue]], stream: StreamT[Future, CharBuffer]): HttpResponse[QueryResult] = {
    HttpResponse[QueryResult](OK, content = Some(Right(stream)))
  }
}

sealed trait SyncResultFormat
object SyncResultFormat {
  case object Simple extends SyncResultFormat
  case object Detailed extends SyncResultFormat
}

class SyncQueryServiceHandler(val queryExecutorFactory: SyncQueryExecutorFactory[Future], jobManager: JobManager[Future], defaultFormat: SyncResultFormat)(implicit M: Monad[Future])
    extends QueryServiceHandler[(Option[JobId], StreamT[Future, CharBuffer])] {

  import scalaz.syntax.std.option._
  import scalaz.std.option._
  import JobManager._

  def extractResponse(request: HttpRequest[Future[JValue]], result: (Option[JobId], StreamT[Future, CharBuffer])): HttpResponse[QueryResult] = {
    import SyncResultFormat._

    val format = request.parameters get 'format map {
      case "simple" => Right(Simple)
      case "detailed" => Right(Detailed)
      case badFormat => Left("unknown format '%s'" format badFormat)
    } getOrElse Right(defaultFormat)

    (format, result._1, result._2) match {
      case (Left(msg), _, _) =>
        HttpResponse[QueryResult](HttpStatus(BadRequest, msg))

      case (Right(Simple), _, data) =>
        HttpResponse[QueryResult](OK, content = Some(Right(data)))

      case (Right(Detailed), None, data) =>
        val prefix = CharBuffer.wrap("""{"errors":[],"warnings":[{"message":"Job service is down; errors/warnings are disabled."}],"data":""")
        val result: StreamT[Future, CharBuffer] = (prefix :: data) ++ (CharBuffer.wrap("}") :: StreamT.empty[Future, CharBuffer])
        HttpResponse[QueryResult](OK, content = Some(Right(result)))

      case (Right(Detailed), Some(jobId), data) =>
        val prefix = CharBuffer.wrap("""{ "data" : """)
        val result = StreamT.unfoldM(some(prefix :: data)) {
          case Some(stream) =>
            stream.uncons flatMap {
              case Some((buffer, tail)) =>
                M.point(Some((buffer, Some(tail))))
              case None =>
                for {
                  warnings <- jobManager.listMessages(jobId, channels.Warning, None)
                  errors <- jobManager.listMessages(jobId, channels.Error, None)
                } yield {
                  val suffix = """, "errors": %s, "warnings": %s }""" format (
                    JArray(errors.toList map (_.value)).renderCompact,
                    JArray(warnings.toList map (_.value)).renderCompact
                  )
                  Some((CharBuffer.wrap(suffix), None))
                }
            }

          case None =>
            M.point(None)
        }
        HttpResponse[QueryResult](OK, content = Some(Right(result)))
    }
  }
}

class AsyncQueryServiceHandler(val queryExecutorFactory: AsyncQueryExecutorFactory[Future])(implicit M: Monad[Future]) extends QueryServiceHandler[JobId] {
  def extractResponse(request: HttpRequest[Future[JValue]], jobId: JobId): HttpResponse[QueryResult] = {
    val result = JObject(JField("jobId", JString(jobId)) :: Nil)
    HttpResponse[QueryResult](Accepted, content = Some(Left(result)))
  }
}

