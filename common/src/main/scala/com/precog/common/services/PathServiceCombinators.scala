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

import com.weiglewilczek.slf4s.Logging
import scalaz.syntax.show._

trait PathServiceCombinators extends HttpRequestHandlerCombinators with Logging {
  def dataPath[A, B, C](prefix: String)(next: HttpService[A, (B, Path) => Future[C]]) = {
    path("""%s/(?:(?<prefixPath>(?:[^\n.](?:[^\n/]|/[^\n])*)/?)?)""".format(prefix)) { 
      new DelegatingService[A, B => Future[C], A, (B, Path) => Future[C]] {
        val delegate = next
        val service = (request: HttpRequest[A]) => {
          logger.debug("Handling dataPath request " + request.shows)
        
          val path: Option[String] = request.parameters.get('prefixPath).filter(_ != null) 
          next.service(request) map { f => (b: B) => f(b, Path(path.getOrElse(""))) }
        }

        val metadata = AboutMetadata(
          PathPatternMetadata(prefix),
          DescriptionMetadata("The portion of the URL path following this prefix will be treated as a path in the Precog virtual filesystem.")
        )
      }
    }
  }
}


// vim: set ts=4 sw=4 et:
