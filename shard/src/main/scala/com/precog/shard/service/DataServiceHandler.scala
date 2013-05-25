package com.precog.shard
package service

import com.precog.common.Path
import com.precog.common.jobs._
import com.precog.common.security._
import com.precog.yggdrasil.execution._
import com.precog.yggdrasil.table._
import com.precog.yggdrasil.vfs.Version

import blueeyes.util.Clock
import blueeyes.core.data._
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.http.HttpHeaders._
import blueeyes.core.http.MimeTypes._
import blueeyes.core.service._
import blueeyes.json._

import akka.dispatch.{ ExecutionContext, Future }
import com.weiglewilczek.slf4s.Logging

import java.nio.ByteBuffer

import scalaz._
import scalaz.syntax.show._
import scalaz.effect.IO

// having access to the whole platform is a bit of overkill, but whatever
class DataServiceHandler[A](platform: Platform[Future, Slice, StreamT[Future, Slice]])(implicit M: Monad[Future])
    extends CustomHttpService[A, (APIKey, Path) => Future[HttpResponse[ByteChunk]]] with Logging {

  val service = (request: HttpRequest[A]) => Success {
    (apiKey: APIKey, path: Path) => {
      val mimeType = request.headers.header[Accept].flatMap(_.mimeTypes.headOption)
      platform.vfs.readResource(apiKey, path, Version.Current, AccessMode.Read).run flatMap {
        _.fold(
          error => {
            logger.error("Read failure: " + error.shows)
            sys.error("fixme... return an informative HTTP repsonse.")
          },
          resource => resource.byteStream(mimeType).run map { byteStream =>
            HttpResponse(OK, headers = HttpHeaders(mimeType.map(`Content-Type`(_)).toSeq: _*), content = byteStream.map(Right(_)))
          }
        )
      } recover {
        case ex => 
            logger.error("Exception thrown in readResource evaluation.", ex)
            HttpResponse(InternalServerError)
      }
    }
  }

  val metadata = NoMetadata //FIXME
}
