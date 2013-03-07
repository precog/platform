package com.precog.common
package services

import akka.dispatch.Future

import blueeyes.core.http._
import blueeyes.core.service._

object CORSHeaderHandler {
  def allowOrigin[A, B](value: String)(service: HttpService[A, Future[HttpResponse[B]]]): HttpService[A, Future[HttpResponse[B]]] = service map { _.map {
    case resp @ HttpResponse(_, headers, _, _) => resp.copy(headers = headers + ("Access-Control-Allow-Origin", value))
  } }
}
