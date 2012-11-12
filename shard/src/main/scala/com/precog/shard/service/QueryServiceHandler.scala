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


class QueryServiceHandler(queryExecutor: QueryExecutor[Future])(implicit dispatcher: MessageDispatcher, M: Monad[Future])
extends CustomHttpService[Future[JValue], (APIKeyRecord, Path, String, QueryOptions) => Future[HttpResponse[QueryResult]]]
with Logging {
  import scalaz.syntax.monad._

  val Command = """:(\w+)\s+(.+)""".r

  val service = (request: HttpRequest[Future[JValue]]) => {
    success((t: APIKeyRecord, p: Path, q: String, opts: QueryOptions) => q.trim match {
      case Command("ls", arg) => list(t.tid, Path(arg.trim))
      case Command("list", arg) => list(t.tid, Path(arg.trim))
      case Command("ds", arg) => describe(t.tid, Path(arg.trim))
      case Command("describe", arg) => describe(t.tid, Path(arg.trim))
      case qt =>
        queryExecutor.execute(t.tid, q, p, opts) match {
          case Success(stream) =>
            Future(HttpResponse[QueryResult](OK, content = Some(Right(stream))))
          
          case Failure(UserError(errorData)) =>
            Future(HttpResponse[QueryResult](UnprocessableEntity, content = Some(Left(errorData))))
          
          case Failure(AccessDenied(reason)) =>
            Future(HttpResponse[QueryResult](HttpStatus(Unauthorized, reason)))
          
          case Failure(TimeoutError) => 
            Future(HttpResponse[QueryResult](RequestEntityTooLarge))
          
          case Failure(SystemError(error)) =>
            error.printStackTrace()
            logger.error("An error occurred processing the query: " + qt, error)
            Future(HttpResponse[QueryResult](HttpStatus(InternalServerError, "A problem was encountered processing your query. We're looking into it!")))
        }
    })
  }

  val metadata = Some(DescriptionMetadata(
    """
Takes a quirrel query and returns the result of evaluating the query.
    """
  ))

  
  def list(u: UID, p: Path) = {
    queryExecutor.browse(u, p).map {
      case Success(r) => HttpResponse[QueryResult](OK, content = Some(Left(r)))
      case Failure(e) => HttpResponse[QueryResult](BadRequest, content = Some(Left(JString("Error listing path: " + p))))
    }
  }

  def describe(u: UID, p: Path) = {
    queryExecutor.structure(u, p).map {
      case Success(r) => HttpResponse[QueryResult](OK, content = Some(Left(r)))
      case Failure(e) => HttpResponse[QueryResult](BadRequest, content = Some(Left(JString("Error describing path: " + p))))
    }
  }
}
