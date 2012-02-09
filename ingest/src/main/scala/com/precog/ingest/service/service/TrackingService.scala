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

trait StorageReporting {
  def tokenId: String
  def stored(path: Path, count: Int)
  def stored(path: Path, count: Int, complexity: Long)
}

class NullStorageReporting(val tokenId: String) extends StorageReporting {
  def stored(path: Path, count: Int) = Unit
  def stored(path: Path, count: Int, complexity: Long) = Unit
}

class ReportGridStorageReporting(val tokenId: String) extends StorageReporting {
  def expirationPolicy = ExpirationPolicy(
    timeToIdle = Some(30), 
    timeToLive = Some(120),
    timeUnit = TimeUnit.SECONDS
  )

  val stage = Stage[Path, StorageMetrics](expirationPolicy, 0) { 
    case (path, StorageMetrics(count, complexity)) =>
 //     client.track(
 //       Trackable(
 //         path = path.toString,
 //         name = "stored",
 //         properties = JObject(JField("#timestamp", "auto") :: JField("count", count) :: JField("complexity", complexity) :: Nil),
 //         rollup = true
 //       )
 //     )
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

class MinimalTrackingService1(eventStore: EventStore, storageReporting: StorageReporting, clock: Clock, autoTimestamp: Boolean)(implicit dispatcher: MessageDispatcher)
extends CustomHttpService[Future[JValue], (Token, Path) => Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[Future[JValue]]) => {
    Success{ (t: Token, p: Path) =>
      Future(HttpResponse[JValue](content=Some(JString("KO"))))
    }
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

class TrackingService(eventStore: EventStore, storageReporting: StorageReporting, clock: Clock, autoTimestamp: Boolean)(implicit dispatcher: MessageDispatcher)
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

class QueryServiceHandler(queryService: QueryService)(implicit dispatcher: MessageDispatcher)
extends CustomHttpService[Future[JValue], Token => Future[HttpResponse[JValue]]] with Logging {
 
  private val InvalidQuery = HttpResponse[JValue](BadRequest, content=Some(JString("Expected query as json string.")))

  val service = (request: HttpRequest[Future[JValue]]) => { 
    Success{ (t: Token) => request.content.map { _.map { 
      case JString(s) => HttpResponse[JValue](OK, content=Some(queryService.execute(s)))
      case _          => InvalidQuery 
    }}.getOrElse( Future { InvalidQuery } ) }
  }

  val metadata = Some(DescriptionMetadata(
    """
Takes a quirrel query and returns the result of evaluating the query.
    """
  ))
}

class EchoServiceHandler(implicit dispatcher: MessageDispatcher)
extends CustomHttpService[Future[JValue], (Token, Path) => Future[HttpResponse[JValue]]] with Logging {

  val service = (request: HttpRequest[Future[JValue]]) => { 
    Success{ (t: Token, p: Path) => Future(HttpResponse[JValue](OK, content=Some(JString("Testing 123.")))) }
  }

  val metadata = Some(DescriptionMetadata(
    """
      This is a dummy echo service.
    """
  ))
}
