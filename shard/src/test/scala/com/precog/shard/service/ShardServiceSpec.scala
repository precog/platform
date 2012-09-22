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
import blueeyes.bkka.{ AkkaDefaults, AkkaTypeClasses }
import blueeyes.core.service.test.BlueEyesServiceSpecification
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.http.MimeTypes
import blueeyes.core.http.MimeTypes._

import blueeyes.json.JsonAST._

import blueeyes.util.Clock

import scalaz.StreamT

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

  def queryExecutorFactory(config: Configuration, accessControl: AccessControl[Future]) = new TestQueryExecutor {
    val actorSystem = self.actorSystem
    val executionContext = self.asyncContext
    val allowedUID = TestTokenUID
  }

  override def tokenManagerFactory(config: Configuration) = TestTokenManager.testTokenManager[Future]

  lazy val queryService = service.contentType[QueryResult](application/(MimeTypes.json))
                                 .path("/analytics/fs/")

  lazy val metaService = service.contentType[QueryResult](application/(MimeTypes.json))
                                .path("/meta/fs/")

  override implicit val defaultFutureTimeouts: FutureTimeouts = FutureTimeouts(20, Duration(1, "second"))
  val shortFutureTimeouts = FutureTimeouts(5, Duration(50, "millis"))
}

class ShardServiceSpec extends TestShardService with FutureMatchers {

  def query(query: String, token: Option[String] = Some(TestTokenUID), path: String = ""): Future[HttpResponse[QueryResult]] = {
    token.map{ queryService.query("tokenId", _) }.getOrElse(queryService).query("q", query).get(path)
  }

  val testQuery = "1 + 1"

  "Shard query service" should {
    "handle query from root path" in {
      query(testQuery) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(Right(_)), _) => ok
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
        case HttpResponse(HttpStatus(BadRequest, _), _, Some(Left(JString("The specified token does not exist"))), _) => ok
      }}
    }
    "reject query when grant is expired" in {
      query(testQuery, Some(ExpiredTokenUID)) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(UnprocessableEntity, _), _, Some(Left(JArray(List(JString("No data accessable at the specified path."))))), _) => ok
      }}
    }
  }
  
  def browse(token: Option[String] = Some(TestTokenUID), path: String = "unittest/"): Future[HttpResponse[QueryResult]] = {
    token.map{ metaService.query("tokenId", _) }.getOrElse(metaService).get(path)
  }
 
  "Shard browse service" should {
    "handle browse for token accessible path" in {
      browse() must whenDelivered { beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(Left(JObject(
          JField("children", JArray(JString("foo")::JString("bar")::Nil)):: Nil
        ))), _) => ok
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
        case HttpResponse(HttpStatus(BadRequest, _), _, Some(Left(JString("The specified token does not exist"))), _) => ok
      }}
    }
    "reject browse when grant expired" in {
      browse(Some(ExpiredTokenUID), path = "/expired") must whenDelivered { beLike {
        case HttpResponse(HttpStatus(Unauthorized, "The specified token may not browse this location"), _, None, _) => ok
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
  
  private def wrap(a: JArray): StreamT[Future, List[JValue]] = {
    StreamT.fromStream(Stream(a.elements).point[Future])
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
