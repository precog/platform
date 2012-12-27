package com.precog.shard
package service

import blueeyes.core.data._
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._
import blueeyes.json._
import blueeyes.util.Clock

import akka.dispatch.Future

import scalaz._
import scalaz.Validation._

import com.weiglewilczek.slf4s.Logging

import com.precog.daze._
import com.precog.common._
import com.precog.common.security._

class ActorStatusHandler[A](queryExecutor: QueryExecutor[Future])
extends CustomHttpService[A, Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[A]) => {
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
