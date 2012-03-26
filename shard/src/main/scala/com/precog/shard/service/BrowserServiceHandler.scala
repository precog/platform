package com.precog.shard
package service

import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._
import blueeyes.json.JsonAST._
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
import com.precog.yggdrasil.metadata.MetadataView

class BrowseServiceHandler(queryExecutor: QueryExecutor)(implicit dispatcher: MessageDispatcher)
extends CustomHttpService[Future[JValue], (Token, Path) => Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[Future[JValue]]) => { 
    success((t: Token, p: Path) => {
      val metadata = queryExecutor.metadata(t.uid)
      Future(HttpResponse[JValue](ServiceUnavailable, content = None))
    })
  }

  val metadata = Some(DescriptionMetadata(
"""
Browse the children of the given path. 
"""
  ))
}
