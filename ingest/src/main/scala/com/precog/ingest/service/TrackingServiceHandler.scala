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

import com.precog.analytics._
import com.precog.common._

import scalaz.Validation

class TrackingServiceHandler(eventStore: EventStore, usageLogging: UsageLogging, clock: Clock, autoTimestamp: Boolean)(implicit dispatcher: MessageDispatcher)
extends CustomHttpService[Future[JValue], (Token, Path) => Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[Future[JValue]]) => {
    Success { (t: Token, p: Path) =>
      request.content map { futureContent =>
        for {
          event <- futureContent
          _ <- eventStore.save(Event.fromJValue(p, event, t.accountTokenId))
        } yield {
          // could return the eventId to the user?
          HttpResponse[JValue](OK)
        }
      } getOrElse {
        Future(HttpResponse[JValue](BadRequest, content=Some(JString("Missing event data."))))
      }
    }
  }

  private def accountPath(path: Path): Path = path.parent match {
    case Some(parent) if parent.equals(Path.Root) => path
    case Some(parent)                             => accountPath(parent)
    case None                                     => sys.error("Traversal to parent of root path should never occur.")
  }

  val metadata = Some(DescriptionMetadata(
    if (autoTimestamp) {
      """
        This service can be used to store a temporal event. If no timestamp tag is specified, then
        the service will be timestamped in UTC with the time on the ReportGrid servers.
      """
    } else {
      """
        This service can be used to store an data point with or without an associated timestamp. 
        Timestamps are not added by default.
      """
    }
  ))
}
