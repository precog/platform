package com.precog.common
package services

import security._

import blueeyes._
import blueeyes.core.data._
import blueeyes.core.http._
import blueeyes.core.service._
import blueeyes.json._
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.json.serialization.DefaultSerialization._

import akka.dispatch.Future
import akka.dispatch.ExecutionContext

trait PathServiceCombinators extends HttpRequestHandlerCombinators {
  def dataPath[A, B](prefix: String)(next: HttpService[A, (APIKey, Path) => Future[B]]) = {
    path("""%s/(?:(?<prefixPath>(?:[^\n.](?:[^\n/]|/[^\n\.])*)/?)?)""".format(prefix)) { 
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
