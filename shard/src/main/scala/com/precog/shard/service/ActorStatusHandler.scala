package com.precog.shard
package service

import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._
import blueeyes.json._
import blueeyes.util.Clock

import akka.dispatch.Future
import akka.dispatch.MessageDispatcher

import scalaz.Success
import scalaz.Failure
import scalaz.Validation._

import com.weiglewilczek.slf4s.Logging

import com.precog.daze._
import com.precog.common._
import com.precog.common.security._

class ActorStatusHandler(queryExecutor: QueryExecutor[Future])(implicit dispatcher: MessageDispatcher)
extends CustomHttpService[Future[JValue], Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[Future[JValue]]) => {
    success(queryExecutor.status() map {
      case Success(result) => HttpResponse[JValue](OK, content = Some(result))
      case Failure(error) => HttpResponse[JValue](HttpStatus(BadRequest, error))
    })
  }

  val metadata = Some(DescriptionMetadata(
"""
Shard server actor status.
"""
  ))
}
