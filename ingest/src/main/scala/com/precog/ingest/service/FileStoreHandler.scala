package com.precog.ingest
package service

import com.precog.common.Path
import com.precog.common.client._
import com.precog.common.ingest._
import com.precog.common.jobs._
import com.precog.common.security._
import com.precog.common.services.ServiceLocation

import blueeyes.core.data.ByteChunk
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.http.HttpHeaders._
import blueeyes.core.service._
import blueeyes.json._
import blueeyes.util.Clock

import akka.dispatch.Future
import akka.dispatch.Promise
import akka.dispatch.ExecutionContext
import akka.util.Timeout

import scalaz._
import scalaz.EitherT._
import scalaz.std.string._
import scalaz.std.option._
import scalaz.syntax.monad._
import scalaz.syntax.std.option._
import scalaz.syntax.semigroup._
import com.weiglewilczek.slf4s._


class FileStoreHandler(serviceLocation: ServiceLocation, jobManager: JobManager[Response], clock: Clock, eventStore: EventStore[Future], ingestTimeout: Timeout)(implicit M: Monad[Future], executor: ExecutionContext) extends CustomHttpService[ByteChunk, (APIKey, Path) => Future[HttpResponse[JValue]]] with Logging {
  private val baseURI = serviceLocation.toURI

  private def validateFileName(name: String): Validation[String, Path] = {
    sys.error("todo")
  }

  private def validateStoreMode(contentType: MimeType, method: HttpMethod): Validation[String, StoreMode] = {
    import FileContent._
    (contentType, method) match {
      case (XQuirrelScript, HttpMethods.POST) => Success(StoreMode.Create)
      case (XQuirrelScript, HttpMethods.PUT) => Success(StoreMode.Replace)
      case (otherType, otherMethod) => Failure("Content-Type %s is not supported for use with method %s.".format(otherType, otherMethod))
    }
  }

  val service: HttpRequest[ByteChunk] => Validation[NotServed, (APIKey, Path) => Future[HttpResponse[JValue]]] = (request: HttpRequest[ByteChunk]) => {
    val fileName0 = request.headers.get("X-File-Name").toSuccess("X-File-Name header must be provided.") flatMap validateFileName
    val contentType0 = request.headers.header[`Content-Type`].flatMap(_.mimeTypes.headOption).toSuccess("Unable to determine content type for file create.")

    val storeMode0 = contentType0 flatMap { validateStoreMode(_: MimeType, request.method) }

    (fileName0.toValidationNel |@| contentType0.toValidationNel |@| storeMode0.toValidationNel) { (fileName, contentType, storeMode) => 
      (apiKey: APIKey, path: Path) => {
        val timestamp = clock.now()
        val fullPath = path / fileName

        request.content map { content =>
        // TODO: check ingest permissions
          (for {
            jobId <- jobManager.createJob(apiKey, "ingest-" + path, "ingest", None, Some(timestamp)).map(_.id)
            bytes <- right(ByteChunk.forceByteArray(content))
            storeFile = StoreFile(apiKey, fullPath, jobId, FileContent(bytes, contentType, RawUTF8Encoding), timestamp.toInstant, None, storeMode)
            _ <- right(eventStore.save(storeFile, ingestTimeout))
          } yield {
            val resultsPath = (baseURI.path |+| Some("/data/fs/" + fullPath.path)).map(_.replaceAll("//", "/"))
            val locationHeader = Location(baseURI.copy(path = resultsPath))
            HttpResponse[JValue](Accepted, headers = HttpHeaders(List(locationHeader)))
          }) valueOr { errors =>
            logger.error("File creation failed due to errors in job service: " + errors)
            HttpResponse[JValue](HttpStatus(InternalServerError, "An error occurred connecting to job tracking service."))
          }
        } getOrElse {
          Promise successful HttpResponse[JValue](HttpStatus(BadRequest, "Attempt to create a file without body content."))
        }
      }
    } leftMap { errors =>
      DispatchError(BadRequest, errors.list.mkString("; "))
    }

  }

  val metadata = NoMetadata
}





