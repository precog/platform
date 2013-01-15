package com.precog.shard
package service

import kafka._

import com.precog.accounts.{BasicAccountManager, InMemoryAccountManager}
import com.precog.daze._
import com.precog.common.Path
import com.precog.common.security._
import com.precog.common.jobs._

import java.nio.ByteBuffer

import org.specs2.mutable.Specification
import org.specs2.specification._
import org.scalacheck.Gen._

import akka.actor.ActorSystem
import akka.dispatch.ExecutionContext
import akka.dispatch.{Await, Future, Promise}
import akka.util.Duration

import org.joda.time._

import org.streum.configrity.Configuration
import org.streum.configrity.io.BlockFormat

import scalaz.{Success, NonEmptyList}
import scalaz.Scalaz._
import scalaz.Validation._

import blueeyes.akka_testing._

import blueeyes.core.data._
import blueeyes.bkka.{ AkkaDefaults, AkkaTypeClasses }
import blueeyes.core.data._
import blueeyes.core.service.test.BlueEyesServiceSpecification
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.http.MimeTypes
import blueeyes.core.http.MimeTypes._
import blueeyes.core.service._

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
  AsyncShardService with
  AkkaDefaults { self =>
  
  import DefaultBijections._
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
  
  def queryExecutorFactoryFactory(config: Configuration, accessControl: AccessControl[Future], extAccountManager: BasicAccountManager[Future], extJobManager: JobManager[Future]) = new TestQueryExecutorFactory {
    val actorSystem = self.actorSystem
    val executionContext = self.asyncContext
    
    val accessControl = apiKeyManager
    val accountManager = inMemAccountMgr
    val jobManager = self.jobManager

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
  val inMemAccountMgr = new InMemoryAccountManager[Future]
  val jobManager = new InMemoryJobManager[Future]
  val to = Duration(3, "seconds")
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

  def accountManagerFactory(config: Configuration) = inMemAccountMgr

  def jobManagerFactory(config: Configuration) = jobManager

  def clock = Clock.System

  import java.nio.ByteBuffer

  implicit val queryResultByteChunkTranscoder =
   new AsyncHttpTranscoder[QueryResult, ByteChunk] {
     def apply(req: HttpRequest[QueryResult]): HttpRequest[ByteChunk] =
       req.copy(content = req.content.map {
         case Left(jv) =>
           Left(ByteBuffer.wrap(jv.renderCompact.getBytes(utf8)))
         case Right(stream) =>
           Right(stream.map(utf8.encode))
       })
     def unapply(res: Future[HttpResponse[ByteChunk]]): Future[HttpResponse[QueryResult]] =
       res.map { r =>
         val q: Option[QueryResult] = r.content.map {
           case Left(bb) =>
             Left(JParser.parseFromByteBuffer(bb).valueOr(throw _))
           case Right(stream) =>
             Right(stream.map(utf8.decode))
         }
         r.copy(content = q)
       }
   }

  lazy val queryService = client.contentType[QueryResult](application/(MimeTypes.json))
                                 .path("/analytics/fs/")

  lazy val metaService = client.contentType[QueryResult](application/(MimeTypes.json))
                                .path("/meta/fs/")

  lazy val asyncService = client.contentType[QueryResult](application/(MimeTypes.json))
                                .path("/analytics/queries")

  override implicit val defaultFutureTimeouts: FutureTimeouts = FutureTimeouts(1, Duration(3, "second"))
  val shortFutureTimeouts = FutureTimeouts(1, Duration(50, "millis"))
  
  implicit def AwaitBijection(implicit bi: Bijection[QueryResult, Future[ByteChunk]]): Bijection[QueryResult, ByteChunk] = new Bijection[QueryResult, ByteChunk] {
    def unapply(chunk: ByteChunk): QueryResult = bi.unapply(Future(chunk))
    def apply(res: QueryResult) = Await.result(bi(res), Duration(3, "second"))
  }
}

class ShardServiceSpec extends TestShardService with FutureMatchers {

  val executionContext = ExecutionContext.defaultExecutionContext(actorSystem)

  def query(query: String, apiKey: Option[String] = Some(testAPIKey), path: String = ""): Future[HttpResponse[QueryResult]] = {
    apiKey.map{ queryService.query("apiKey", _) }.getOrElse(queryService).query("q", query).get(path)
  }

  def asyncQuery(query: String, apiKey: Option[String] = Some(testAPIKey), path: String = ""): Future[HttpResponse[QueryResult]] = {
    apiKey.map { asyncService.query("apiKey", _) }.getOrElse(asyncService)
        .query("q", query).query("prefixPath", path).post[QueryResult]("") {
      Right(StreamT.empty[Future, CharBuffer])
    }
  }

  def asyncQueryResults(jobId: JobId, apiKey: Option[String] = Some(testAPIKey)): Future[HttpResponse[QueryResult]] = {
    apiKey.map { asyncService.query("apiKey", _) }.getOrElse(asyncService).get(jobId)
  }

  val simpleQuery = "1 + 1"
  val relativeQuery = "//foo"
  val accessibleAbsoluteQuery = "//test/foo"
  val inaccessibleAbsoluteQuery = "//inaccessible/foo"

  def extractResult(data: StreamT[Future, CharBuffer]): Future[JValue] = {
    data.foldLeft("") { _ + _.toString } map (JParser.parseUnsafe(_))
  }

  def extractJobId(stream: StreamT[Future, CharBuffer]): Future[JobId] = {
    extractResult(stream) map (_ \ "jobId") map {
      case JString(jobId) => jobId
      case _ => sys.error("This is not JSON! GIVE ME JSON!")
    }
  }

  def waitForJobCompletion(jobId: JobId): Future[Either[String, (Option[MimeType], StreamT[Future, Array[Byte]])]] = {
    import JobState._

    jobManager.findJob(jobId) flatMap {
      case Some(Job(_, _, _, _, _, NotStarted | Started(_, _))) =>
        waitForJobCompletion(jobId)
      case Some(_) =>
        jobManager.getResult(jobId)
      case None =>
        Future(Left("The job doesn't even exist!!!"))
    }
  }

  "Shard query service" should {
    "handle absolute accessible query from root path" in {
      query(accessibleAbsoluteQuery) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(_), _) => ok
      }}
    }
    "create a job when an async query is posted" in {
      val res = for {
        HttpResponse(HttpStatus(Accepted, _), _, Some(Right(res)), _) <- asyncQuery(simpleQuery)
        jobId <- extractJobId(res)
        job <- jobManager.findJob(jobId)
      } yield job

      res must whenDelivered { beLike {
        case Some(Job(_, _, _, _, _, _)) => ok
      }}
    }
    "results of an async job must eventually be made available" in {
      val res = for {
        HttpResponse(HttpStatus(Accepted, _), _, Some(Right(res)), _) <- asyncQuery(simpleQuery)
        jobId <- extractJobId(res)
        _ <- waitForJobCompletion(jobId)
        HttpResponse(HttpStatus(OK, _), _, Some(Right(data)), _) <- asyncQueryResults(jobId)
        result <- extractResult(data)
      } yield result

      val expected = JObject(
        JField("warnings", JArray(Nil)) ::
        JField("errors", JArray(Nil)) ::
        JField("data", JArray(JNum(2) :: Nil)) ::
        Nil)

      res must whenDelivered { beLike {
        case `expected` => ok
      }}
    }
    "reject absolute inaccessible query from root path" in {
      query(inaccessibleAbsoluteQuery) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(Unauthorized, "No data accessable at the specified path"), _, None, _) => ok
      }}
    }
    "handle relative query from accessible non-root path" in {
      query(relativeQuery, path = "/test") must whenDelivered { beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(_), _) => ok
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

trait TestQueryExecutorFactory extends AsyncQueryExecutorFactory with ManagedQueryModule { self =>
  import scalaz.syntax.monad._
  import scalaz.syntax.traverse._
  import AkkaTypeClasses._
  
  def actorSystem: ActorSystem  
  implicit def executionContext: ExecutionContext
  val to = Duration(3, "seconds")
  
  val accessControl: AccessControl[Future]
  val ownerMap: Map[Path, Set[AccountId]]

  private def wrap[M[+_]: Monad](a: JArray): StreamT[M, CharBuffer] = {
    val str = a.toString
    val buffer = CharBuffer.allocate(str.length)
    buffer.put(str)
    buffer.flip()
    
    StreamT.fromStream(Stream(buffer).point[M])
  }

  type YggConfig = ManagedQueryModuleConfig
  object yggConfig extends YggConfig {
    val jobPollFrequency = Duration(2, "seconds")
    val clock = Clock.System
  }

  def asyncExecutorFor(apiKey: APIKey): Future[Validation[String, QueryExecutor[Future, JobId]]] = {
    Future(Success(new AsyncQueryExecutor {
      val executionContext = self.executionContext
    }))
  }

  def syncExecutorFor(apiKey: APIKey): Future[Validation[String, QueryExecutor[Future, (Option[JobId], StreamT[Future, CharBuffer])]]] = {
    Future(Success(new SyncQueryExecutor {
      val executionContext = self.executionContext
    }))
  }


  // def executorFor(apiKey: APIKey): Future[Validation[String, QueryExecutor[Future, StreamT[Future, CharBuffer]]]] = Future {
  protected def executor(implicit shardQueryMonad: ShardQueryMonad): QueryExecutor[ShardQuery, StreamT[ShardQuery, CharBuffer]] = {
    new QueryExecutor[ShardQuery, StreamT[ShardQuery, CharBuffer]] {
      def execute(apiKey: APIKey, query: String, prefix: Path, opts: QueryOptions) = {
        val requiredPaths = if(query startsWith "//") Set(prefix / Path(query.substring(1))) else Set.empty[Path]
        val allowed = Await.result(Future.sequence(requiredPaths.map {
          path => accessControl.hasCapability(apiKey, Set(ReadPermission(path, ownerMap(path))), Some(new DateTime))
        }).map(_.forall(identity)), to)
        
        if(allowed)
          shardQueryMonad.point(success(wrap(JArray(List(JNum(2))))))
        else
          shardQueryMonad.point(failure(AccessDenied("No data accessable at the specified path")))
      }
    }
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

  def startup = Promise.successful(true)
  def shutdown = Future { actorSystem.shutdown; true }
}
