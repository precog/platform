package com.precog.ingest
package service

import com.precog.common.Path
import com.precog.common.accounts.AccountServiceCombinators
import com.precog.common.ingest._
import com.precog.common.security._


import blueeyes._
import blueeyes.core.data._
import blueeyes.core.http._
import blueeyes.core.service._
import blueeyes.json._
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.json.serialization.DefaultSerialization._

import akka.dispatch.Future
import akka.dispatch.ExecutionContext

trait EventServiceCombinators extends APIKeyServiceCombinators with AccountServiceCombinators {
  import DefaultBijections._

  def left[A, B, C](h: HttpService[Either[A, B], C]): HttpService[A, C] = {
    new CustomHttpService[A, C] {
      val service = (req: HttpRequest[A]) =>
        h.service(req.copy(content = req.content map (Left(_))))

      val metadata = None
    }
  }

  def right[A, B, C](h: HttpService[Either[A, B], C]): HttpService[B, C] = {
    new CustomHttpService[B, C] {
      val service = (req: HttpRequest[B]) =>
        h.service(req.copy(content = req.content map (Right(_))))

      val metadata = None
    }
  }

  def dataPath[A, B](prefix: String)(next: HttpService[A, (APIKey, Path) => Future[B]]) = {
    path("""/%s/(?:(?<prefixPath>(?:[^\n.](?:[^\n/]|/[^\n\.])*)/?)?)""".format(prefix)) { 
      new DelegatingService[A, APIKey => Future[B], A, (APIKey, Path) => Future[B]] {
        val delegate = next
        val service = (request: HttpRequest[A]) => {
          val path: Option[String] = request.parameters.get('prefixPath).filter(_ != null) 
          next.service(request) map { f => (apiKey: APIKey) => f(apiKey, Path(path.getOrElse(""))) }
        }

        val metadata = None
      }
    }
  }
}

// vim: set ts=4 sw=4 et:
