package com.precog.ingest
package service

import com.precog.common.Path
import com.precog.common.client._
import com.precog.common.ingest._
import com.precog.common.jobs._
import com.precog.common.security._

import blueeyes.core.data.ByteChunk
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.http.HttpHeaders._
import blueeyes.core.service._
import blueeyes.json._
import blueeyes.util.Clock

import akka.dispatch.Future

import scalaz._
import scalaz.syntax.monad._

class FileCreateHandler(clock: Clock)(implicit M: Monad[Future]) extends CustomHttpService[ByteChunk, (APIKey, Path) => Future[HttpResponse[JValue]]] {
  val service: HttpRequest[ByteChunk] => Validation[NotServed, (APIKey, Path) => Future[HttpResponse[JValue]]] = (request: HttpRequest[ByteChunk]) => {
    Success((apiKey: APIKey, path: Path) => HttpResponse[JValue](InternalServerError, content = Some(JString("Not yet implemented."))).point[Future])
  }

  val metadata = NoMetadata
}





