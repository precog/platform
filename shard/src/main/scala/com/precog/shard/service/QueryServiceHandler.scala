package com.precog.shard
package service

import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._
import blueeyes.json.JsonAST._
import blueeyes.util.Clock

import akka.dispatch.Future
import akka.dispatch.MessageDispatcher

import scalaz.{ Monad, Success, Failure }
import scalaz.Validation._

import com.weiglewilczek.slf4s.Logging

import com.precog.daze._
import com.precog.common._
import com.precog.common.security._


class QueryServiceHandler(queryExecutor: QueryExecutor[Future])(implicit dispatcher: MessageDispatcher, m: Monad[Future])
extends CustomHttpService[Future[JValue], (Token, Path, String, QueryOptions) => Future[HttpResponse[QueryResult]]]
with Logging {
  import scalaz.syntax.monad._

  val Command = """:(\w+)\s+(.+)""".r

  val service = (request: HttpRequest[Future[JValue]]) => {
    success((t: Token, p: Path, q: String, opts: QueryOptions) => q.trim match {
      case Command("ls", arg) => list(t.tid, Path(arg.trim))
      case Command("list", arg) => list(t.tid, Path(arg.trim))
      case Command("ds", arg) => describe(t.tid, Path(arg.trim))
      case Command("describe", arg) => describe(t.tid, Path(arg.trim))
      case qt => Future {
        queryExecutor.execute(t.tid, q, p, opts) match {
          case Success(result)               => HttpResponse[QueryResult](OK, content = Some(Right(result)))
          case Failure(UserError(errorData)) => HttpResponse[QueryResult](UnprocessableEntity, content = Some(Left(errorData)))
          case Failure(AccessDenied(reason)) => HttpResponse[QueryResult](HttpStatus(Unauthorized, reason))
          case Failure(TimeoutError)         => HttpResponse[QueryResult](RequestEntityTooLarge)
          case Failure(SystemError(error))   =>
            error.printStackTrace()
            logger.error("An error occurred processing the query: " + qt, error)
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

// class PathQueryServiceHandler(queryExecutor: QueryExecutor[Future])(implicit dispatcher: MessageDispatcher, m: Monad[Future])
// extends CustomHttpService[Future[JValue], (Token, Path, String) => Validation[NotServed, Future[HttpResponse[JValue]]]] with Logging {
// 
//   // class QueryServiceHandler(queryExecutor: QueryExecutor[Future])(implicit dispatcher: MessageDispatcher)
//   // extends CustomHttpService[Future[JValue], (Token, Path, String) => Future[HttpResponse[JValue]]] with Logging {
// 
// 
//   val service = (request: HttpRequest[Future[JValue]]) => {
//     success((t: Token, p: Path, q: String) => 
//      // if(p != Path("/")) {
//       //  Future(HttpResponse[JValue](HttpStatus(Unauthorized, "Queries made at non-root paths are not yet available.")))
//      // } else {
//         q.trim match {
//           case Command("ls", arg) => success(list(t.tid, Path(arg.trim)))
//           case Command("list", arg) => success(list(t.tid, Path(arg.trim)))
//           case Command("ds", arg) => success(describe(t.tid, Path(arg.trim)))
//           case Command("describe", arg) => success(describe(t.tid, Path(arg.trim)))
//           case _ => failure(inapplicable)
//           // case qt =>
//           //   Future(queryExecutor.execute(t.tid, qt, p) match {
//           //     case Success(result)               => HttpResponse[JValue](OK, content = Some(result))
//           //     case Failure(UserError(errorData)) => HttpResponse[JValue](UnprocessableEntity, content = Some(errorData))
//           //     case Failure(AccessDenied(reason)) => HttpResponse[JValue](HttpStatus(Unauthorized, reason))
//           //     case Failure(TimeoutError)         => HttpResponse[JValue](RequestEntityTooLarge)
//           //     case Failure(SystemError(error))   => 
//           //       error.printStackTrace() 
//           //       logger.error("An error occurred processing the query: " + qt, error)
//           //       HttpResponse[JValue](HttpStatus(InternalServerError, "A problem was encountered processing your query. We're looking into it!"))
//           //   })
//         }
//       //}
//       )
//   }
  
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

//   val metadata = Some(DescriptionMetadata(
//     """
// List and describe paths available to query.
//     """
//   ))
}
