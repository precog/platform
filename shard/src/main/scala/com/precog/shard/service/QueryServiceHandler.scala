package com.precog.shard
package service

import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._
import blueeyes.json._
import blueeyes.util.Clock

import akka.dispatch.Future
import akka.dispatch.MessageDispatcher

import com.weiglewilczek.slf4s.Logging

import java.nio.CharBuffer

import com.precog.daze._
import com.precog.common._
import com.precog.common.security._

import scalaz._
import scalaz.Validation.{ success, failure }

class QueryServiceHandler(queryExecutorFactory: AsyncQueryExecutorFactory)(implicit dispatcher: MessageDispatcher, M: Monad[Future])
extends CustomHttpService[Future[JValue], (APIKeyRecord, Path, String, QueryOptions) => Future[HttpResponse[QueryResult]]]
with Logging {
  import scalaz.syntax.monad._

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

  val service = { (request: HttpRequest[Future[JValue]]) =>
    success((r: APIKeyRecord, p: Path, q: String, opts: QueryOptions) => q.trim match {
      case Command("ls", arg) => list(r.apiKey, Path(arg.trim))
      case Command("list", arg) => list(r.apiKey, Path(arg.trim))
      case Command("ds", arg) => describe(r.apiKey, Path(arg.trim))
      case Command("describe", arg) => describe(r.apiKey, Path(arg.trim))
      case qt =>

        def executeWith[A](executorV: Future[Validation[String, QueryExecutor[Future, A]]])(f: A => HttpResponse[QueryResult]): Future[HttpResponse[QueryResult]] = {
          executorV flatMap {
            case Success(executor) =>
              executor.execute(r.apiKey, q, p, opts) map {
                case Success(result) =>
                  f(result)
                case Failure(error) =>
                  handleErrors(qt, error)
              }

            case Failure(error) =>
              logger.error("Failure during evaluator setup: " + error)
              Future(HttpResponse[QueryResult](HttpStatus(InternalServerError, "A problem was encountered processing your query. We're looking into it!")))
          }
        }

        val async = request.parameters.get('sync) map (_ == "async") getOrElse false

        if (async) {
          executeWith(queryExecutorFactory.asyncExecutorFor(r.apiKey)) { jobId =>
            val result = JObject(JField("jobId", JString(jobId)) :: Nil)
            HttpResponse[QueryResult](Accepted, content = Some(Left(result)))
          }
        } else {
          executeWith(queryExecutorFactory.executorFor(r.apiKey)) { stream =>
            HttpResponse[QueryResult](OK, content = Some(Right(stream)))
          }
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
