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
import akka.dispatch.Future
import akka.util.Duration

import org.joda.time._

import org.streum.configrity.Configuration
import org.streum.configrity.io.BlockFormat

import scalaz.{Success, NonEmptyList}
import scalaz.Scalaz._
import scalaz.Validation._

import blueeyes.concurrent.test._

import blueeyes.core.data._
import blueeyes.bkka.AkkaDefaults
import blueeyes.core.service.test.BlueEyesServiceSpecification
import blueeyes.core.http._
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
  val ExpiredTokenUID = expiredUID
}

trait TestShardService extends BlueEyesServiceSpecification with ShardService with TestTokens with AkkaDefaults with MongoTokenManagerComponent { self =>

  import BijectionsChunkJson._

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

  def queryExecutorFactory(config: Configuration, accessControl: AccessControl[Future]) = new TestQueryExecutor {
    val actorSystem = self.actorSystem
    val executionContext = self.asyncContext
    val allowedUID = TestTokenUID
  }

  override def tokenManagerFactory(config: Configuration) = TestTokenManager.testTokenManager[Future]

  lazy val shardService = service.contentType[JValue](application/(MimeTypes.json))
                                 .path("/vfs/")

  override implicit val defaultFutureTimeouts: FutureTimeouts = FutureTimeouts(20, Duration(1, "second"))
  val shortFutureTimeouts = FutureTimeouts(5, Duration(50, "millis"))
}

class ShardServiceSpec extends TestShardService with FutureMatchers {

  def query(query: String, token: Option[String] = Some(TestTokenUID), path: String = ""): Future[HttpResponse[JValue]] = {
    token.map{ shardService.query("tokenId", _) }.getOrElse(shardService).query("q", query).get(path)
  }

  val testQuery = "1 + 1"

  "Shard query service" should {
    "handle query from root path" in {
      query(testQuery) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(JArray(JInt(i)::Nil)), _) => ok
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
    "reject query when no token provided" in {
      query(testQuery, None) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(BadRequest, "A tokenId query parameter is required to access this URL"), _, None, _) => ok
      }}
    }
    "reject query when token not found" in {
      query(testQuery, Some("not-gonna-find-it")) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(BadRequest, _), _, Some(JString("The specified token does not exist")), _) => ok
      }}
    }
    "reject query when grant is expired" in {
      query(testQuery, Some(ExpiredTokenUID)) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(UnprocessableEntity, _), _, Some(JArray(List(JString("No data accessable at the specified path.")))), _) => ok
      }}
    }
  }
  
  def browse(token: Option[String] = Some(TestTokenUID), path: String = "unittest/"): Future[HttpResponse[JValue]] = {
    token.map{ shardService.query("tokenId", _) }.getOrElse(shardService).get(path)
  }
 
  "Shard browse service" should {
    "handle browse for token accessible path" in {
      browse() must whenDelivered { beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(JArray(JString("foo")::JString("bar")::Nil)), _) => ok
      }}
    }
    "reject browse for non-token accessible path" in {
      browse(path = "") must whenDelivered { beLike {
        case HttpResponse(HttpStatus(Unauthorized, "The specified token may not browse this location"), _, None, _) => ok
      }}
    }
    "reject browse when no token provided" in {
      browse(None) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(BadRequest, "A tokenId query parameter is required to access this URL"), _, None, _) => ok
      }}
    }
    "reject browse when token not found" in {
      browse(Some("not-gonna-find-it")) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(BadRequest, _), _, Some(JString("The specified token does not exist")), _) => ok
      }}
    }
    "reject browse when grant expired" in {
      browse(Some(ExpiredTokenUID), path = "/expired") must whenDelivered { beLike {
        case HttpResponse(HttpStatus(Unauthorized, "The specified token may not browse this location"), _, None, _) => ok
      }}
    }
  }
}

trait TestQueryExecutor extends QueryExecutor {
  def actorSystem: ActorSystem  
  implicit def executionContext: ExecutionContext
  def allowedUID: String
  
  def execute(userUID: String, query: String, prefix: Path) = {
    if(userUID != allowedUID) {
      failure(UserError(JArray(List(JString("No data accessable at the specified path.")))))
    } else {
      success(JArray(List(JInt(2))))
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
