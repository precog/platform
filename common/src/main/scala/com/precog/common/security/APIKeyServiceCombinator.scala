package com.precog.common
package security

import blueeyes._
import blueeyes.core.http._
import blueeyes.core.service._
import blueeyes.json._
import blueeyes.json.serialization.DefaultSerialization._

import akka.dispatch.Future
import akka.dispatch.ExecutionContext

import scalaz._

trait APIKeyServiceCombinators extends HttpRequestHandlerCombinators {
  implicit val jsonErrorTransform = (failure: HttpFailure, s: String) => HttpResponse(failure, content = Some(s.serialize))

  def apiKey[A, B](apiKeyManager: APIKeyFinder[Future])(service: HttpService[A, APIKey => Future[B]])(implicit err: (HttpFailure, String) => B, M: Monad[Future]) = {
    new APIKeyRequiredService[A, B](apiKeyManager, service, err, M)
  }
}
