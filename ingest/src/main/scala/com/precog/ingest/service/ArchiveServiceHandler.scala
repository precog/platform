package com.precog
package ingest
package service

import common._
import common.security._

import akka.dispatch.Future
import akka.dispatch.MessageDispatcher
import akka.util.Timeout

import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._

import blueeyes.json.JsonAST._

import com.weiglewilczek.slf4s.Logging

import scalaz.{Validation, Success}

class ArchiveServiceHandler(accessControl: AccessControl[Future], eventStore: EventStore, archiveTimeout: Timeout)(implicit dispatcher: MessageDispatcher)
extends CustomHttpService[Future[JValue], (Token, Path) => Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[Future[JValue]]) => {
    Success { (t: Token, p: Path) =>
      accessControl.mayAccess(t.tid, p, Set(), OwnerPermission) flatMap { mayAccess =>
        if(mayAccess) {
          try { 
            val archiveInstance = Archive(p, t.tid)
            logger.trace("Archiving path: " + archiveInstance)
            eventStore.save(archiveInstance, archiveTimeout).map {
              _ => HttpResponse[JValue](OK)
            }
          } catch {
            case ex => {
              logger.error("Error during archive", ex)
              Future(HttpResponse[JValue](ServiceUnavailable))
            }
          }
        } else {
          Future(HttpResponse[JValue](Unauthorized, content=Some(JString("Your token does not have permissions to archive this path."))))
        }
      }
    }
  }

  val metadata = Some(DescriptionMetadata(
    """
      This service can be used to archive the data under the given path.
    """
  ))
}
