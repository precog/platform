package com.precog.shard
package service

import com.precog.daze._
import com.precog.common._
import com.precog.common.security._

import blueeyes.core.data._
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._
import blueeyes.json._
import blueeyes.util.Clock

import akka.dispatch.Future
import akka.dispatch.ExecutionContext

import scalaz._
import scalaz.Validation._

import com.weiglewilczek.slf4s.Logging

class ActorStatusHandler(queryExecutorFactory: QueryExecutorFactory[Future, _])(implicit executor: ExecutionContext)
extends CustomHttpService[Future[JValue], Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[Future[JValue]]) => {
    success(queryExecutorFactory.status() map {
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
