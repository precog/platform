package com.precog
package auth

import com.precog.common.security._

import akka.dispatch.Future
import akka.dispatch.MessageDispatcher
import akka.util.Timeout

import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._

import blueeyes.json.JsonAST._

import com.weiglewilczek.slf4s.Logging

import scalaz.{Validation, Success}

class GetTokenHandler(tokenManager: TokenManager)(implicit dispatcher: MessageDispatcher) extends CustomHttpService[Future[JValue], Token => Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[Future[JValue]]) => {
    Success { (t: Token) =>
      Future(HttpResponse[JValue](ServiceUnavailable))
    }
  }
  val metadata = None
}

class CreateTokenHandler(tokenManager: TokenManager)(implicit dispatcher: MessageDispatcher) extends CustomHttpService[Future[JValue], Token => Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[Future[JValue]]) => {
    Success { (t: Token) =>
      Future(HttpResponse[JValue](ServiceUnavailable))
    }
  }
  val metadata = None
}

class DeleteTokenHandler(tokenManager: TokenManager)(implicit dispatcher: MessageDispatcher) extends CustomHttpService[Future[JValue], Token => Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[Future[JValue]]) => {
    Success { (t: Token) =>
      Future(HttpResponse[JValue](ServiceUnavailable))
    }
  }
  val metadata = None
}

class UpdateTokenHandler(tokenManager: TokenManager)(implicit dispatcher: MessageDispatcher) extends CustomHttpService[Future[JValue], Token => Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[Future[JValue]]) => {
    Success { (t: Token) =>
      Future(HttpResponse[JValue](ServiceUnavailable))
    }
  }
  val metadata = None
}
