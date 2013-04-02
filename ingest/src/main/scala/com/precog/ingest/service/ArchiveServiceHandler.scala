package com.precog.ingest
package service

import com.precog.common._
import com.precog.common.security._
import com.precog.common.ingest._

import akka.dispatch.Future
import akka.dispatch.ExecutionContext
import akka.util.Timeout

import blueeyes.core.data.ByteChunk
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._

import blueeyes.json._
import blueeyes.util.Clock

import com.weiglewilczek.slf4s.Logging

import scalaz._

class ArchiveServiceHandler[A](accessControl: AccessControl[Future], eventStore: EventStore[Future], clock: Clock, archiveTimeout: Timeout)(implicit M: Monad[Future])
extends CustomHttpService[A, (APIKey, Path) => Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[A]) => {
    Success { (apiKey: APIKey, path: Path) =>
      import Permission._
      // FIXME: This delete permission check may be excessively restrictive, need to look at the implementation
      accessControl.hasCapability(apiKey, Set(DeletePermission(path, WrittenByAny)), Some(clock.now())) flatMap {
        case true =>
          //FIXME: this should spawn a job
          val archiveInstance = Archive(apiKey, path, None, clock.instant())
          logger.trace("Archiving path: " + archiveInstance)
          eventStore.save(archiveInstance, archiveTimeout) map {
            _ => HttpResponse[JValue](OK)
          }

        case false =>
          M.point(HttpResponse[JValue](Unauthorized, content=Some(JString("Your API key does not have permissions to archive this path."))))
      }
    }
  }

  val metadata = DescriptionMetadata("""This service can be used to delete all data at the given path. Note: This does not imply recursive deletion by default.""")
}
