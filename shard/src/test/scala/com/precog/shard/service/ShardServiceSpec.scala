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

trait TestShardService extends
  BlueEyesServiceSpecification with
  ShardService with
  AkkaDefaults { self =>
  
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
    
    val accessControl = apiKeyManager
    val ownerMap = Map(
      Path("/")     ->             Set("root"),
      Path("/test") ->             Set("test"),
      Path("/test/foo") ->         Set("test"),
      Path("/expired") ->          Set("expired"),
      Path("/inaccessible") ->     Set("other"),
      Path("/inaccessible/foo") -> Set("other")
    )
  }

  val apiKeyManager = new InMemoryAPIKeyManager[Future]
  val to = Duration(1, "seconds")
  val rootAPIKey = Await.result(apiKeyManager.rootAPIKey, to)
  
  val testPath = Path("/test")
  val testAPIKey = Await.result(apiKeyManager.newStandardAPIKeyRecord("test", testPath).map(_.apiKey), to)
  
  val accessTest = Set[Permission](
    ReadPermission(testPath, Set("test")),
    ReducePermission(testPath, Set("test")),
    WritePermission(testPath, Set()),
    DeletePermission(testPath, Set())
  )
  val expiredPath = Path("expired")
  val expiredAPIKey = Await.result(apiKeyManager.newStandardAPIKeyRecord("expired", expiredPath).map(_.apiKey).flatMap { expiredAPIKey => 
    apiKeyManager.deriveAndAddGrant(None, None, testAPIKey, accessTest, expiredAPIKey, Some(new DateTime().minusYears(1000))).map(_ => expiredAPIKey)
  }, to)
  
  def apiKeyManagerFactory(config: Configuration) = apiKeyManager

  lazy val queryService = service.contentType[QueryResult](application/(MimeTypes.json))
                                 .path("/analytics/fs/")

  lazy val metaService = service.contentType[QueryResult](application/(MimeTypes.json))
                                .path("/meta/fs/")

  override implicit val defaultFutureTimeouts: FutureTimeouts = FutureTimeouts(1, Duration(1, "second"))
  val shortFutureTimeouts = FutureTimeouts(1, Duration(50, "millis"))
  
  implicit def AwaitBijection(implicit bi: Bijection[QueryResult, Future[ByteChunk]]): Bijection[QueryResult, ByteChunk] = new Bijection[QueryResult, ByteChunk] {
    def unapply(chunk: ByteChunk): QueryResult = bi.unapply(Future(chunk))
    def apply(res: QueryResult) = Await.result(bi(res), Duration(1, "second"))
  }
}

class ShardServiceSpec extends TestShardService with FutureMatchers {

  def query(query: String, apiKey: Option[String] = Some(testAPIKey), path: String = ""): Future[HttpResponse[QueryResult]] = {
    apiKey.map{ queryService.query("apiKey", _) }.getOrElse(queryService).query("q", query).get(path)
  }

  val simpleQuery = "1 + 1"
  val relativeQuery = "//foo"
  val accessibleAbsoluteQuery = "//test/foo"
  val inaccessibleAbsoluteQuery = "//inaccessible/foo"

  "Shard query service" should {
    "handle absolute accessible query from root path" in {
      query(accessibleAbsoluteQuery) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(Left(_)), _) => ok
      }}
    }
    "reject absolute inaccessible query from root path" in {
      query(inaccessibleAbsoluteQuery) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(Unauthorized, "No data accessable at the specified path"), _, None, _) => ok
      }}
    }
    "handle relative query from accessible non-root path" in {
      query(relativeQuery, path = "/test") must whenDelivered { beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(Left(_)), _) => ok
      }}
    }
    "reject relative query from inaccessible non-root path" in {
      query(relativeQuery, path = "/inaccessible") must whenDelivered { beLike {
        case HttpResponse(HttpStatus(Unauthorized, "No data accessable at the specified path"), _, None, _) => ok
      }}
    }
    "reject query when no API key provided" in {
      query(simpleQuery, None) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(BadRequest, "An apiKey query parameter is required to access this URL"), _, None, _) => ok
      }}
    }
    "reject query when API key not found" in {
      query(simpleQuery, Some("not-gonna-find-it")) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(BadRequest, _), _, Some(Left(JString("The specified API key does not exist: not-gonna-find-it"))), _) => ok
      }}
    }
    "reject query when grant is expired" in {
      query(accessibleAbsoluteQuery, Some(expiredAPIKey)) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(Unauthorized, "No data accessable at the specified path"), _, None, _) => ok
      }}
    }
  }
  
  def browse(apiKey: Option[String] = Some(testAPIKey), path: String = "/test"): Future[HttpResponse[QueryResult]] = {
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
        case HttpResponse(HttpStatus(BadRequest, "The specified API key may not browse this location"), _, None, _) => ok
      }}
    }
    "reject browse when no API key provided" in {
      browse(None) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(BadRequest, "An apiKey query parameter is required to access this URL"), _, None, _) => ok
      }}
    }
    "reject browse when API key not found" in {
      browse(Some("not-gonna-find-it")) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(BadRequest, _), _, Some(Left(JString("The specified API key does not exist: not-gonna-find-it"))), _) => ok
      }}
    }
    "reject browse when grant expired" in {
      browse(Some(expiredAPIKey), path = "/test") must whenDelivered { beLike {
        case HttpResponse(HttpStatus(BadRequest, "The specified API key may not browse this location"), _, None, _) => ok
      }}
    }
  }
}

trait TestQueryExecutor extends QueryExecutor[Future] {
  import scalaz.syntax.monad._
  import scalaz.syntax.traverse._
  import AkkaTypeClasses._
  
  def actorSystem: ActorSystem  
  implicit def executionContext: ExecutionContext
  val to = Duration(1, "seconds")
  
  val accessControl: AccessControl[Future]
  val ownerMap: Map[Path, Set[AccountID]]

  private def wrap(a: JArray): StreamT[Future, CharBuffer] = {
    val str = a.toString
    val buffer = CharBuffer.allocate(str.length)
    buffer.put(str)
    buffer.flip()
    
    StreamT.fromStream(Stream(buffer).point[Future])
  }

  def execute(apiKey: APIKey, query: String, prefix: Path, opts: QueryOptions) = {
    val requiredPaths = if(query startsWith "//") Set(prefix / Path(query.substring(1))) else Set.empty[Path]
    val allowed = Await.result(Future.sequence(requiredPaths.map {
      path => accessControl.hasCapability(apiKey, Set(ReadPermission(path, ownerMap(path))), Some(new DateTime))
    }).map(_.forall(identity)), to)
    
    if(allowed)
      success(wrap(JArray(List(JNum(2)))))
    else
      failure(AccessDenied("No data accessable at the specified path"))
  }

  def browse(apiKey: APIKey, path: Path) = {
    accessControl.hasCapability(apiKey, Set(ReadPermission(path, ownerMap(path))), Some(new DateTime)).map { allowed =>
      if(allowed) {
        success(JArray(List(JString("foo"), JString("bar"))))
      } else {
        failure("The specified API key may not browse this location")
      }
    }
  }
  
  def structure(apiKey: APIKey, path: Path) = {
    accessControl.hasCapability(apiKey, Set(ReadPermission(path, ownerMap(path))), Some(new DateTime)).map { allowed =>
      if(allowed) {
        success(JObject(List(
          JField("test1", JString("foo")),
          JField("test2", JString("bar"))
        )))
      } else {
        failure("The specified API key may not browse this location")
      }
    }
  }

  def status() = Future(Success(JArray(List(JString("status")))))

  def startup = Future(true)
  def shutdown = Future { actorSystem.shutdown; true }
}
