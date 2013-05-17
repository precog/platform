package com.precog.shard
package service

import com.precog.common.Path
import com.precog.common.jobs._
import com.precog.common.security._
import com.precog.yggdrasil.vfs._

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

class DataServiceHandler[A](vfs: SecureVFS[Future])(implicit M: Monad[Future])
    extends CustomHttpService[A, (APIKey, Path) => Future[HttpResponse[ByteChunk]]] with Logging {

  val service = (request: HttpRequest[A]) => Success {
    (apiKey: APIKey, path: Path) => {
      val mimeType = request.headers.header[Accept].flatMap(_.mimeTypes.headOption)
      vfs.readResource(path, Version.Current) flatMap {
        case ReadSuccess(_, Some(resource)) => 
          resource.byteStream(mimeType) map { byteStream =>
            HttpResponse(OK, headers = HttpHeaders(mimeType.map(`Content-Type`(_)).toSeq: _*), content = byteStream.map(Right(_)))
          }

        case ReadFailure(_, errors) =>
          // FIXXE: Make this better
          logger.error("Read failure: " + errors.shows)
          M.point(HttpResponse(InternalServerError))
      }
    }
  }

  val metadata = NoMetadata //FIXME
}
