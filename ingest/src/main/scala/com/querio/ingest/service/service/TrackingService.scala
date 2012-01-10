package com.querio.ingest.service
package service

import blueeyes._
import blueeyes.concurrent.Future
import blueeyes.concurrent.FutureImplicits
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.persistence.cache._
import blueeyes.util.Clock

import IngestService._
import com.reportgrid.api.ReportGridTrackingClient
import com.reportgrid.api.Trackable
import rosetta.json.blueeyes._

import java.util.Properties
import java.util.concurrent.TimeUnit
import scalaz.Scalaz._
import scalaz.Success
import scalaz.Failure
import scalaz.Semigroup
import scalaz.NonEmptyList

import com.weiglewilczek.slf4s.Logging

import com.reportgrid.analytics._
import com.reportgrid.common._

trait StorageReporting {
  def tokenId: String
  def stored(path: Path, count: Int)
  def stored(path: Path, count: Int, complexity: Long)
}

class NullStorageReporting(val tokenId: String) extends StorageReporting {
  def stored(path: Path, count: Int) = Unit
  def stored(path: Path, count: Int, complexity: Long) = Unit
}

class ReportGridStorageReporting(val tokenId: String, client: ReportGridTrackingClient[JValue]) extends StorageReporting {
  def expirationPolicy = ExpirationPolicy(
    timeToIdle = Some(30), 
    timeToLive = Some(120),
    timeUnit = TimeUnit.SECONDS
  )

  val stage = Stage[Path, StorageMetrics](expirationPolicy, 0) { 
    case (path, StorageMetrics(count, complexity)) =>
      client.track(
        Trackable(
          path = path.toString,
          name = "stored",
          properties = JObject(JField("#timestamp", "auto") :: JField("count", count) :: JField("complexity", complexity) :: Nil),
          rollup = true
        )
      )
  }

  def stored(path: Path, count: Int) = {
    stage.put(path, StorageMetrics(count, None))
  }
  
  def stored(path: Path, count: Int, complexity: Long) = {
    stage.put(path, StorageMetrics(count, Some(complexity)))
  }
}

case class StorageMetrics(count: Int, complexity: Option[Long])
object StorageMetrics {
  implicit val Semigroup: Semigroup[StorageMetrics] = new Semigroup[StorageMetrics] {
    override def append(s1: StorageMetrics, s2: => StorageMetrics) = {
      val count = s1.count + s2.count
      val complexity = (s1.complexity, s2.complexity) match {
        case (None, None)         => None
        case (Some(c), None)      => Some(c)
        case (None, Some(c))      => Some(c)
        case (Some(c1), Some(c2)) => Some(c1 + c2)
      }
      StorageMetrics(count, complexity)
    }
  }
}

class TrackingService(eventStore: EventStore, storageReporting: StorageReporting, clock: Clock, autoTimestamp: Boolean)
extends CustomHttpService[Future[JValue], (Token, Path) => Future[HttpResponse[JValue]]] with Logging with FutureImplicits {
  val service = (request: HttpRequest[Future[JValue]]) => {
    Success{ (t: Token, p: Path) =>
      request.content.map { _.flatMap { event  => 
        eventStore.save(Event.fromJValue(p.toString, event, t.accountTokenId)).map(_ => HttpResponse[JValue](OK)).toBlueEyes
      }}.getOrElse(Future.sync(HttpResponse[JValue](BadRequest, content=Some(JString("Missing event data.")))))
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

class EchoServiceHandler
extends CustomHttpService[Future[JValue], (Token, Path) => Future[HttpResponse[JValue]]] with Logging with FutureImplicits {
  val service = (request: HttpRequest[Future[JValue]]) => { 
    Success{ (t: Token, p: Path) => Future.sync(HttpResponse[JValue](OK, content=Some(JString("Testing 123.")))) }
  }

  val metadata = Some(DescriptionMetadata(
    """
      This is a dummy echo service.
    """
  ))
}
