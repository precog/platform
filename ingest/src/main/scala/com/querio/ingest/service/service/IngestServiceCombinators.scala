package com.querio.ingest.service
package service

import blueeyes._
import blueeyes.concurrent.Future
import blueeyes.core.http._
import blueeyes.core.service._
import blueeyes.json._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.DefaultSerialization._

import com.reportgrid.analytics._

trait IngestServiceCombinators extends HttpRequestHandlerCombinators {
  implicit val jsonErrorTransform = (failure: HttpFailure, s: String) => HttpResponse(failure, content = Some(s.serialize))

  def token[A, B](tokenManager: TokenManager)(service: HttpService[A, Token => Future[B]])(implicit err: (HttpFailure, String) => B) = {
    new TokenRequiredService[A, B](tokenManager, service)
  }

  def dataPath[A, B](prefix: String)(next: HttpService[A, (Token, Path) => Future[B]]) = {
    path("""/%s/(?:(?<prefixPath>(?:[^\n.](?:[^\n/]|/[^\n\.])*)/?)?)""".format(prefix)) { 
      new DelegatingService[A, Token => Future[B], A, (Token, Path) => Future[B]] {
        val delegate = next
        val service = (request: HttpRequest[A]) => {
          val path: Option[String] = request.parameters.get('prefixPath).filter(_ != null) 
          next.service(request) map { f => (token: Token) => f(token, Path(path.getOrElse(""))) }
        }

        val metadata = None
      }
    }
  }
}

// vim: set ts=4 sw=4 et:
