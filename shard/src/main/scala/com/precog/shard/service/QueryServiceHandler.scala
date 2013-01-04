package com.precog.shard
package service

import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._
import blueeyes.json._
import blueeyes.util.Clock

import akka.dispatch.Future
import akka.dispatch.MessageDispatcher

import scalaz.{ Monad, Success, Failure }
import scalaz.Validation._

import com.weiglewilczek.slf4s.Logging

import com.precog.daze._
import com.precog.common._
import com.precog.common.security._


class QueryServiceHandler(queryExecutor: QueryExecutorFactory[Future])(implicit dispatcher: MessageDispatcher, M: Monad[Future])
extends CustomHttpService[Future[JValue], (APIKeyRecord, Path, String, QueryOptions) => Future[HttpResponse[QueryResult]]]
with Logging {
  import scalaz.syntax.monad._

  val Command = """:(\w+)\s+(.+)""".r

  val service = (request: HttpRequest[Future[JValue]]) => {
    success((r: APIKeyRecord, p: Path, q: String, opts: QueryOptions) => q.trim match {
      case Command("ls", arg) => list(r.apiKey, Path(arg.trim))
      case Command("list", arg) => list(r.apiKey, Path(arg.trim))
      case Command("ds", arg) => describe(r.apiKey, Path(arg.trim))
      case Command("describe", arg) => describe(r.apiKey, Path(arg.trim))
      case qt =>
        queryExecutor.executorFor(r.apiKey).map { 
          case Success(evaluator) => evaluator.execute(r.apiKey, q, p, opts) match {
            case Success(stream) =>
              HttpResponse[QueryResult](OK, content = Some(Right(stream)))
          
            case Failure(UserError(errorData)) =>
              HttpResponse[QueryResult](UnprocessableEntity, content = Some(Left(errorData)))
          
            case Failure(AccessDenied(reason)) =>
              HttpResponse[QueryResult](HttpStatus(Unauthorized, reason))
          
            case Failure(TimeoutError) =>
              HttpResponse[QueryResult](RequestEntityTooLarge)
          
            case Failure(SystemError(error)) =>
              error.printStackTrace()
              logger.error("An error occurred processing the query: " + qt, error)
            HttpResponse[QueryResult](HttpStatus(InternalServerError, "A problem was encountered processing your query. We're looking into it!"))
          }
          case Failure(error) => {
            logger.error("Failure during evaluator setup: " + error)
            HttpResponse[QueryResult](HttpStatus(InternalServerError, "A problem was encountered processing your query. We're looking into it!"))
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
    queryExecutor.browse(apiKey, p).map {
      case Success(r) => HttpResponse[QueryResult](OK, content = Some(Left(r)))
      case Failure(e) => HttpResponse[QueryResult](BadRequest, content = Some(Left(JString("Error listing path: " + p))))
    }
  }

  def describe(apiKey: APIKey, p: Path) = {
    queryExecutor.structure(apiKey, p).map {
      case Success(r) => HttpResponse[QueryResult](OK, content = Some(Left(r)))
      case Failure(e) => HttpResponse[QueryResult](BadRequest, content = Some(Left(JString("Error describing path: " + p))))
    }
  }
}
