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
import blueeyes.util.Clock

import akka.dispatch.Future
import akka.dispatch.ExecutionContext

import com.weiglewilczek.slf4s.Logging

import java.nio.CharBuffer

import scalaz._
import scalaz.Validation.{ success, failure }
import scalaz.syntax.monad._

<<<<<<< HEAD
class QueryServiceHandler[A](queryExecutor: QueryExecutor[Future])(implicit M: Monad[Future])
extends CustomHttpService[A, (APIKey, Path, String, QueryOptions) => Future[HttpResponse[QueryResult]]]
with Logging {
  def queryExecutorFactory: QueryExecutorFactory[Future, A]
  def extractResponse(a: A): HttpResponse[QueryResult]

  val Command = """:(\w+)\s+(.+)""".r

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

  val service = (request: HttpRequest[A]) => {
    success((apiKey: APIKey, path: Path, query: String, opts: QueryOptions) => query.trim match {
      case Command("ls", arg) => list(apiKey, Path(arg.trim))
      case Command("list", arg) => list(apiKey, Path(arg.trim))
      case Command("ds", arg) => describe(apiKey, Path(arg.trim))
      case Command("describe", arg) => describe(apiKey, Path(arg.trim))
      case qt =>
        val executorV = queryExecutorFactory.executorFor(apiKey)
        executorV flatMap {
          case Success(executor) =>
            executor.execute(r.apiKey, q, p, opts) map {
              case Success(result) =>
                extractResponse(result)
              case Failure(error) =>
                handleErrors(qt, error)
            }

          case Failure(error) =>
            logger.error("Failure during evaluator setup: " + error)
            Future(HttpResponse[QueryResult](HttpStatus(InternalServerError, "A problem was encountered processing your query. We're looking into it!")))
        }
    })
  }

  val metadata = Some(DescriptionMetadata(
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

class SyncQueryServiceHandler(
    val queryExecutorFactory: QueryExecutorFactory[Future, StreamT[Future, CharBuffer]])(implicit
    val dispatcher: MessageDispatcher,
    val M: Monad[Future]) extends QueryServiceHandler[StreamT[Future, CharBuffer]] {

  def extractResponse(stream: StreamT[Future, CharBuffer]): HttpResponse[QueryResult] = {
    HttpResponse[QueryResult](OK, content = Some(Right(stream)))
  }
}

class AsyncQueryServiceHandler(
    val queryExecutorFactory: QueryExecutorFactory[Future, JobId])(implicit
    val dispatcher: MessageDispatcher,
    val M: Monad[Future]) extends QueryServiceHandler[JobId] {

  def extractResponse(jobId: JobId): HttpResponse[QueryResult] = {
    val result = JObject(JField("jobId", JString(jobId)) :: Nil)
    HttpResponse[QueryResult](Accepted, content = Some(Left(result)))
  }
}

