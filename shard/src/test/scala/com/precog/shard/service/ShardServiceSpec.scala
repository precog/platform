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
package com.precog.shard
package service

import kafka._

import com.precog.daze._
import com.precog.common.Path
import com.precog.common.security._

import org.specs2.mutable.Specification
import org.specs2.specification._
import org.scalacheck.Gen._

import akka.actor.ActorSystem
import akka.dispatch.ExecutionContext
import akka.dispatch.{Await, Future}
import akka.util.Duration

import org.joda.time._

import org.streum.configrity.Configuration
import org.streum.configrity.io.BlockFormat

import scalaz.{Success, NonEmptyList}
import scalaz.Scalaz._
import scalaz.Validation._

import blueeyes.concurrent.test._

import blueeyes.core.data._
import blueeyes.bkka.{ AkkaDefaults, AkkaTypeClasses }
import blueeyes.core.service.test.BlueEyesServiceSpecification
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.http.MimeTypes
import blueeyes.core.http.MimeTypes._

import blueeyes.json._

import blueeyes.util.Clock

import scalaz._

import java.nio.CharBuffer

case class PastClock(duration: org.joda.time.Duration) extends Clock {
  def now() = new DateTime().minus(duration)
  def instant() = now().toInstant
  def nanoTime = sys.error("nanotime not available in the past")
}

trait TestShardService extends BlueEyesServiceSpecification with ShardService with AkkaDefaults with MongoAPIKeyManagerComponent { self =>
  
  val apiKeyManager = new InMemoryAPIKeyManager[Future]
  
  lazy val testAPIKey: APIKey = sys.error("FIXME")
  lazy val expiredAPIKey: APIKey = sys.error("FIXME")

  import BijectionsChunkJson._
  import BijectionsChunkQueryResult._
  import AkkaTypeClasses._

  val config = """ 
    security {
      test = true
      mongo {
        servers = [localhost]
        database = test
      }
    }
  """

  override val configuration = "services { quirrel { v1 { " + config + " } } }"

  val actorSystem = ActorSystem("ingestServiceSpec")
  val asyncContext = ExecutionContext.defaultExecutionContext(actorSystem)
  implicit val M: Monad[Future] = AkkaTypeClasses.futureApplicative(asyncContext)
  
  
  def queryExecutorFactory(config: Configuration, accessControl: AccessControl[Future]) = new TestQueryExecutor {
    val actorSystem = self.actorSystem
    val executionContext = self.asyncContext
    val allowedUID = testAPIKey
  }

  override def apiKeyManagerFactory(config: Configuration) = apiKeyManager

  lazy val queryService = service.contentType[QueryResult](application/(MimeTypes.json))
                                 .path("/analytics/fs/")

  lazy val metaService = service.contentType[QueryResult](application/(MimeTypes.json))
                                .path("/meta/fs/")

  override implicit val defaultFutureTimeouts: FutureTimeouts = FutureTimeouts(20, Duration(1, "second"))
  val shortFutureTimeouts = FutureTimeouts(5, Duration(50, "millis"))
  
  implicit def AwaitBijection(implicit bi: Bijection[QueryResult, Future[ByteChunk]]): Bijection[QueryResult, ByteChunk] = new Bijection[QueryResult, ByteChunk] {
    def unapply(chunk: ByteChunk): QueryResult = bi.unapply(Future(chunk))
    def apply(res: QueryResult) = Await.result(bi(res), Duration(1, "second"))
  }
}

class ShardServiceSpec extends TestShardService with FutureMatchers {

  def query(query: String, apiKey: Option[String] = Some(testAPIKey), path: String = ""): Future[HttpResponse[QueryResult]] = {
    apiKey.map{ queryService.query("apiKey", _) }.getOrElse(queryService).query("q", query).get(path)
  }

  val testQuery = "1 + 1"

  "Shard query service" should {
    "handle query from root path" in {
      query(testQuery) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(Left(_)), _) => ok
      }}
    }
    "handle query from non-root path" in {
      pending
      //query("//numbers", path = "/hom") must whenDelivered { 
      //  beLike {
      //    case HttpResponse(HttpStatus(OK, _), _, Some(data), _) => data must be_==(JString("fixme"))
      //  }
      //}
    }
    "reject query when no API key provided" in {
      query(testQuery, None) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(BadRequest, "An apiKey query parameter is required to access this URL"), _, None, _) => ok
      }}
    }
    "reject query when API key not found" in {
      query(testQuery, Some("not-gonna-find-it")) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(BadRequest, _), _, Some(Left(JString("The specified API key does not exist"))), _) => ok
      }}
    }
    "reject query when grant is expired" in {
      query(testQuery, Some(expiredAPIKey)) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(UnprocessableEntity, _), _, Some(Left(JArray(List(JString("No data accessable at the specified path."))))), _) => ok
      }}
    }
  }
  
  def browse(apiKey: Option[String] = Some(testAPIKey), path: String = "unittest/"): Future[HttpResponse[QueryResult]] = {
    apiKey.map{ metaService.query("apiKey", _) }.getOrElse(metaService).get(path)
  }
 
  "Shard browse service" should {
    "handle browse for API key accessible path" in {
      val obj = JObject(Map("foo" -> JArray(JString("foo")::JString("bar")::Nil)))
      browse() must whenDelivered { beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(Left(obj)), _) => ok
      }}
    }
    "reject browse for non-API key accessible path" in {
      browse(path = "") must whenDelivered { beLike {
        case HttpResponse(HttpStatus(Unauthorized, "The specified API key may not browse this location"), _, None, _) => ok
      }}
    }
    "reject browse when no API key provided" in {
      browse(None) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(BadRequest, "An apiKey query parameter is required to access this URL"), _, None, _) => ok
      }}
    }
    "reject browse when API key not found" in {
      browse(Some("not-gonna-find-it")) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(BadRequest, _), _, Some(Left(JString("The specified API key does not exist"))), _) => ok
      }}
    }
    "reject browse when grant expired" in {
      browse(Some(expiredAPIKey), path = "/expired") must whenDelivered { beLike {
        case HttpResponse(HttpStatus(Unauthorized, "The specified API key may not browse this location"), _, None, _) => ok
      }}
    }
  }
}

trait TestQueryExecutor extends QueryExecutor[Future] {
  def actorSystem: ActorSystem  
  implicit def executionContext: ExecutionContext
  def allowedUID: String

  import scalaz.syntax.monad._
  import AkkaTypeClasses._
  
  private def wrap(a: JArray): StreamT[Future, CharBuffer] = {
    val str = a.toString
    val buffer = CharBuffer.allocate(str.length)
    buffer.put(str)
    buffer.flip()
    
    StreamT.fromStream(Stream(buffer).point[Future])
  }

  def execute(userUID: String, query: String, prefix: Path, opts: QueryOptions) = {
    if(userUID != allowedUID) {
      failure(UserError(JArray(List(JString("No data accessable at the specified path.")))))
    } else {
      success(wrap(JArray(List(JNum(2)))))
    } 
  }
  
  def browse(userUID: String, path: Path) = {
    Future(success(JArray(List(JString("foo"), JString("bar")))))
  }
  
  def structure(userUID: String, path: Path) = {
    Future(success(JObject(List(
      JField("test1", JString("foo")),
      JField("test2", JString("bar"))
    ))))
  }
 
  def status() = Future(Success(JArray(List(JString("status")))))

  def startup = Future(true)
  def shutdown = Future { actorSystem.shutdown; true }
}
