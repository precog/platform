/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
package com.precog.ingest
package service

import kafka._

import com.precog.daze._
import com.precog.common.Path
import com.precog.common.security._

import org.specs2.mutable.Specification
import org.specs2.specification._
import org.scalacheck.Gen._

import akka.actor.ActorSystem
import akka.dispatch.Future
import akka.dispatch.ExecutionContext
import akka.util.Duration

import org.joda.time._

import org.streum.configrity.Configuration
import org.streum.configrity.io.BlockFormat

import scalaz.{Success, NonEmptyList}
import scalaz.Scalaz._

import blueeyes.concurrent.test._

import blueeyes.core.data._
import blueeyes.bkka.AkkaDefaults
import blueeyes.core.service.test.BlueEyesServiceSpecification
import blueeyes.core.http.HttpResponse
import blueeyes.core.http.HttpStatus
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.http.MimeTypes
import blueeyes.core.http.MimeTypes._

import blueeyes.json.JsonAST._

import blueeyes.util.Clock


case class PastClock(duration: org.joda.time.Duration) extends Clock {
  def now() = new DateTime().minus(duration)
  def instant() = now().toInstant
  def nanoTime = sys.error("nanotime not available in the past")
}

trait TestTokens {
  import TestTokenManager._
  val TestTokenUID = testUID
  val TrackingTokenUID = usageUID
  val ExpiredTokenUID = expiredUID
}

trait TestIngestService extends BlueEyesServiceSpecification with IngestService with TestTokens with AkkaDefaults with MongoTokenManagerComponent {
  val asyncContext = defaultFutureDispatch

  import BijectionsChunkJson._
  import BijectionsChunkString._
  import BijectionsByteArray._
  import BijectionsChunkByteArray._

  val config = """
    security {
      test = true
      mongo {
        mock = true
        servers = [localhost]
        database = test
      }
    }
  """

  override val configuration = "services { ingest { v1 { " + config + " } } }"

  def usageLoggingFactory(config: Configuration) = new ReportGridUsageLogging(TrackingTokenUID) 

  val messaging = new CollectingMessaging

  def queryExecutorFactory(config: Configuration) = new NullQueryExecutor {
    lazy val actorSystem = ActorSystem("ingestServiceSpec")
    implicit lazy val executionContext = ExecutionContext.defaultExecutionContext(actorSystem)
  }

  def eventStoreFactory(config: Configuration): EventStore = {
    val defaultAddresses = NonEmptyList(MailboxAddress(0))

    val routeTable = new ConstantRouteTable(defaultAddresses)

    new KafkaEventStore(new EventRouter(routeTable, messaging), 0)
  }

  override def tokenManagerFactory(config: Configuration) = TestTokenManager.testTokenManager[Future]

  lazy val ingestService = service.contentType[JValue](application/(MimeTypes.json)).path("/sync/fs/")

  lazy val asyncIngestService =
    service.contentType[ByteChunk](application/(MimeTypes.json)).path("/async/fs/")

  lazy val syncIngestService =
    service.contentType[ByteChunk](application/(MimeTypes.json)).path("/sync/fs/")

  override implicit val defaultFutureTimeouts: FutureTimeouts = FutureTimeouts(20, Duration(1, "second"))
  val shortFutureTimeouts = FutureTimeouts(5, Duration(50, "millis"))
}

class IngestServiceSpec extends TestIngestService with FutureMatchers {

  def asyncTrack(data: ByteChunk, token: Option[String] = Some(TestTokenUID), path: String = "unittest") =
    token.map(asyncIngestService.query("apiKey", _)).getOrElse(asyncIngestService).post[ByteChunk](path)(data)

  def syncTrack(data: ByteChunk, token: Option[String] = Some(TestTokenUID), path: String = "unittest") =
    token.map(syncIngestService.query("apiKey", _)).getOrElse(syncIngestService).post[ByteChunk](path)(data)

  def track(data: JValue, token: Option[String] = Some(TestTokenUID), path: String = "unittest"): Future[HttpResponse[JValue]] = {
    token.map{ ingestService.query("apiKey", _) }.getOrElse(ingestService).post(path)(data)
  }

  def testValue = JObject(List(JField("testing", JNum(123))))

  def testChunkValue = Chunk(
    "{ testing: 123 }\n".getBytes("UTF-8"),
    Some(Future {
      Chunk("{ testing: 321 }".getBytes("UTF-8"), None)
    })
  )

  def badChunkValue = Chunk(
    "178234#!!@#$\n".getBytes("UTF-8"),
    Some(Future {
      Chunk("{ testing: 321 }".getBytes("UTF-8"), None)
    })
  )

  "Ingest service" should {
    "track event with valid token" in {
      track(testValue) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(OK, _), _, None, _) => ok
      }}
    }
    "track asynchronous event with valid token" in {
      asyncTrack(testChunkValue) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(Accepted, _), _, None, _) => ok
      }}
    }
    "track asynchronous event with bad row" in {
      syncTrack(badChunkValue) must whenDelivered { beLike {
        // TODO Check that the value in Some(...) is { errors: [ 0 ] }.
        case HttpResponse(HttpStatus(BadRequest, _), _, Some(_), _) => ok
      }}
    }
    "reject track request when token not found" in {
      track(testValue, Some("not gonna find it")) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(BadRequest, _), _, Some(JString("The specified token does not exist")), _) => ok 
      }}
    }
    "reject track request when no token provided" in {
      track(testValue, None) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(BadRequest, _), _, _, _) => ok 
      }}
    }
    "reject track request when grant is expired" in {
      track(testValue, Some(ExpiredTokenUID)) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(Unauthorized, _), _, Some(JString("Your token does not have permissions to write at this location.")), _) => ok 
      }}
    }
    "reject track request when path is not accessible by token" in {
      track(testValue, path = "") must whenDelivered { beLike {
        case HttpResponse(HttpStatus(Unauthorized, _), _, Some(JString("Your token does not have permissions to write at this location.")), _) => ok 
      }}
    }
  }
}
