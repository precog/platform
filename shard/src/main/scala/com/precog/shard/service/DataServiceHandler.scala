package com.precog.shard
package service

import com.precog.common.Path
import com.precog.common.jobs._
import com.precog.common.security._
import com.precog.common.services.ServiceHandlerUtil
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
      val mimeTypes = request.headers.header[Accept].toSeq.flatMap(_.mimeTypes)
      platform.vfs.readResource(apiKey, path, Version.Current, AccessMode.Read).run flatMap {
        _.fold(
          error => {
            logger.error("Read failure: " + error.shows)
            sys.error("fixme... return an informative HTTP repsonse.")
          },
          resource => resource.byteStream(mimeTypes).run map {
            case Some((reportedType, byteStream)) =>
              HttpResponse(OK, headers = HttpHeaders(`Content-Type`(reportedType)), content = Some(Right(byteStream)))

            case None =>
              HttpResponse(NotFound, content = Some(Left(("Could not locate content for path " + path).getBytes("UTF-8"))))
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
