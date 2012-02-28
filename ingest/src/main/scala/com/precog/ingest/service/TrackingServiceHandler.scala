package com.precog.ingest
package service

import blueeyes._
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.persistence.cache._
import blueeyes.util.Clock

import akka.dispatch.Future
import akka.dispatch.MessageDispatcher

import IngestService._
//import com.precog.api.ReportGridTrackingClient
//import com.precog.api.Trackable
//import rosetta.json.blueeyes._

import java.util.Properties
import java.util.concurrent.TimeUnit
import scalaz.Scalaz._
import scalaz.Success
import scalaz.Failure
import scalaz.Semigroup
import scalaz.NonEmptyList

import com.weiglewilczek.slf4s.Logging

import com.precog.analytics.Path
import com.precog.common._
import com.precog.common.security._

import scalaz.Validation

class TrackingServiceHandler(accessControl: AccessControl, eventStore: EventStore, usageLogging: UsageLogging)(implicit dispatcher: MessageDispatcher)
extends CustomHttpService[Future[JValue], (Token, Path) => Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[Future[JValue]]) => {
    Success { (t: Token, p: Path) =>
      if(accessControl.mayAccessPath(t.uid, p, PathWrite)) {
        request.content map { futureContent =>
          try { 
            for {
              event <- futureContent
              _ <- eventStore.save(Event.fromJValue(p, event, t.uid))
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

  val metadata = Some(DescriptionMetadata(
    """
      This service can be used to store an data point with or without an associated timestamp. 
      Timestamps are not added by default.
    """
  ))
}
