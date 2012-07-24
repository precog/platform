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

class TrackingServiceHandler(accessControl: AccessControl[Future], eventStore: EventStore, usageLogging: UsageLogging, insertTimeout: Timeout)(implicit dispatcher: MessageDispatcher)
extends CustomHttpService[Future[JValue], (Token, Path) => Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[Future[JValue]]) => {
    Success { (t: Token, p: Path) =>
      accessControl.mayAccessPath(t.tid, p, PathWrite) flatMap { mayAccess =>
        if(mayAccess) {
          request.content map { futureContent =>
            try { 
              for {
                event <- futureContent
                _ <- { 
                  val eventInstance = Event.fromJValue(p, event, t.tid)
                  logger.trace("Saving event: " + eventInstance)
                  eventStore.save(eventInstance, insertTimeout)
                }
              } yield {
                // could return the eventId to the user?
                HttpResponse[JValue](OK)
              }
            } catch {
              case ex => Future(HttpResponse[JValue](ServiceUnavailable))
            }
          } getOrElse {
            Future(HttpResponse[JValue](BadRequest, content=Some(JString("Missing event data."))))
          }
        } else {
          Future(HttpResponse[JValue](Unauthorized, content=Some(JString("Your token does not have permissions to write at this location."))))
        }
      }
    }
  }

  val metadata = Some(DescriptionMetadata(
    """
      This service can be used to store an data point with or without an associated timestamp. 
      Timestamps are not added by default.
    """
  ))
}
