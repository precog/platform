package com.precog.shard
package service

import blueeyes._
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._
import blueeyes.json._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.DefaultSerialization._

import akka.dispatch.Future
import akka.dispatch.MessageDispatcher

import com.precog.common.Path
import com.precog.common.security._

import scalaz.Validation._

trait ShardServiceCombinators extends HttpRequestHandlerCombinators {

  type Query = String

  def query[A, B](next: HttpService[A, (Token, Path, Query) => Future[B]]) = {
    new DelegatingService[A, (Token, Path) => Future[B], A, (Token, Path, Query) => Future[B]] {
      val delegate = next
      val metadata = None
      val service = (request: HttpRequest[A]) => {
        val query: Option[String] = request.parameters.get('q).filter(_ != null)
        query map { q =>
          next.service(request) map { f => (token: Token, path: Path) => f(token, path, q) }
        } getOrElse {
          failure(DispatchError(HttpException(BadRequest, "No query string was provided.")))
        }
      }
    }
  }
}
