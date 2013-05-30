package com.precog.shard
package service

import kafka._

import com.precog.daze._
import com.precog.common._
import com.precog.common.Path
import com.precog.common.accounts._
import com.precog.common.security._
import com.precog.common.jobs._
import com.precog.yggdrasil.execution._
import com.precog.yggdrasil.scheduling.NoopScheduler
import com.precog.yggdrasil.table.Slice
import com.precog.yggdrasil.vfs._

import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit

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

import blueeyes.akka_testing._
import blueeyes.core.data._
import blueeyes.bkka._
import blueeyes.core.data._
import blueeyes.core.service.test.BlueEyesServiceSpecification
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.http.HttpHeaders._
import blueeyes.core.http.MimeTypes
import blueeyes.core.http.MimeTypes._
import blueeyes.core.service._
import blueeyes.json._
import blueeyes.util.Clock
import DefaultBijections._

import scalaz._
import scalaz.effect._
import scalaz.Validation._
import scalaz.std.option._
import scalaz.syntax.comonad._

import java.nio.CharBuffer
import java.nio.charset.Charset

case class PastClock(duration: org.joda.time.Duration) extends Clock {
  def now() = new DateTime().minus(duration)
  def instant() = now().toInstant
  def nanoTime = sys.error("nanotime not available in the past")
}

trait TestShardService extends BlueEyesServiceSpecification 
    with ShardService 
    with AkkaDefaults { self =>

  val config = """
    security {
      test = true
      mongo {
        servers = [localhost]
        database = test
      }
    }
  """

  private val to = Duration(3, "seconds")

  private val actorSystem = ActorSystem("shardServiceSpec")
  val executionContext = ExecutionContext.defaultExecutionContext(actorSystem)

  implicit val M: Monad[Future] with Comonad[Future] = new blueeyes.bkka.UnsafeFutureComonad(executionContext, Duration(5, "seconds"))

  private val apiKeyManager = new InMemoryAPIKeyManager[Future](blueeyes.util.Clock.System)
  private val apiKeyFinder = new DirectAPIKeyFinder[Future](apiKeyManager)
  protected val jobManager = new InMemoryJobManager[Future] //TODO: should be private?
  private val clock = Clock.System
  private val accountFinder = new StaticAccountFinder[Future]("test", "who-cares")

  val rootAPIKey = apiKeyManager.rootAPIKey.copoint

  val testPath = Path("/test")
  val testAPIKey = apiKeyManager.newStandardAPIKeyRecord("test").map(_.apiKey).copoint

  import Permission._
  val testPermissions = Set[Permission](
    ReadPermission(Path.Root, WrittenByAccount("test")),
    WritePermission(testPath, WriteAsAny),
    DeletePermission(testPath, WrittenByAny)
  )

  val expiredPath = Path("expired")

  val expiredAPIKey = apiKeyManager.newStandardAPIKeyRecord("expired").map(_.apiKey).flatMap { expiredAPIKey =>
    apiKeyManager.deriveAndAddGrant(None, None, testAPIKey, testPermissions, expiredAPIKey, Some(new DateTime().minusYears(1000))).map(_ => expiredAPIKey)
  } copoint

  def configureShardState(config: Configuration) = Future {
    val accountFinder = new StaticAccountFinder[Future]("test", testAPIKey)
    val scheduler = NoopScheduler[Future]
    val platform = new TestPlatform with SecureVFSModule[Future, Slice] with InMemoryVFSModule[Future] {
      override val jobActorSystem = self.actorSystem
      override val actorSystem = self.actorSystem
      override val executionContext = self.executionContext
      override val accessControl = new DirectAPIKeyFinder(self.apiKeyManager)(self.M)

      val defaultTimeout = Duration(90, TimeUnit.SECONDS)
      implicit val M = self.M
      implicit val IOT = new (IO ~> Future) { def apply[A](io: IO[A]) = Future { io.unsafePerformIO } }

      override val jobManager = self.jobManager

      val ownerMap = Map(
        Path("/")     ->             Set("root"),
        Path("/test") ->             Set("test"),
        Path("/test/foo") ->         Set("test"),
        Path("/expired") ->          Set("expired"),
        Path("/inaccessible") ->     Set("other"),
        Path("/inaccessible/foo") -> Set("other")
      )

      def stubValue(authorities: Authorities):((Array[Byte], MimeType) \/ Vector[JValue], Authorities) = 
        (\/.right(Vector(JObject("foo" -> JString("foov"), "bar" -> JNum(1)), JObject("foo" -> JString("foov2")))), authorities)

      val stubData = ownerMap mapValues { accounts => stubValue(Authorities.ifPresent(accounts).get) }

      val rawVFS = new InMemoryVFS(stubData, clock)
      val permissionsFinder = new PermissionsFinder(self.apiKeyFinder, accountFinder, clock.instant())
      val vfs = new SecureVFS(rawVFS, permissionsFinder, jobManager, clock)
    }

    ShardState(platform, self.apiKeyFinder, self.accountFinder, scheduler, jobManager, clock, Stoppable.Noop)
  }

  val utf8 = Charset.forName("UTF-8")

  def charBufferToBytes(cb: CharBuffer): Array[Byte] = {
    val bb = utf8.encode(cb)
    val arr = new Array[Byte](bb.remaining)
    bb.get(arr)
    arr
  }

  implicit val queryResultByteChunkTranscoder = new AsyncHttpTranscoder[QueryResult, ByteChunk] {
    def apply(req: HttpRequest[QueryResult]): HttpRequest[ByteChunk] =
      req map {
        case Left(jv) => Left(jv.renderCompact.getBytes(utf8))
        case Right(stream) => Right(stream.map(charBufferToBytes))
      }

    def unapply(fres: Future[HttpResponse[ByteChunk]]): Future[HttpResponse[QueryResult]] =
      fres map { response =>
        val contentType = response.headers.header[`Content-Type`].flatMap(_.mimeTypes.headOption)
        response.status.code match {
          case OK | Accepted => //assume application/json
            response map {
              case Left(bytes) => Left(JParser.parseFromByteBuffer(ByteBuffer.wrap(bytes)).valueOr(throw _))
              case Right(stream) => Right(stream.map(bytes => utf8.decode(ByteBuffer.wrap(bytes))))
            }

          case error =>
            if (contentType.exists(_ == MimeTypes.application/json)) {
              response map {
                case Left(bytes) => Left(JParser.parseFromByteBuffer(ByteBuffer.wrap(bytes)).valueOr(throw _))
                case Right(stream) => Right(stream.map(bytes => utf8.decode(ByteBuffer.wrap(bytes))))
              }
            } else {
              response map {
                case Left(bb) => Left(JString(new String(bb.array, "UTF-8")))
                case chunk => Right(StreamT.wrapEffect(chunkToFutureString.apply(chunk).map(s => CharBuffer.wrap(JString(s).renderCompact) :: StreamT.empty[Future, CharBuffer])))
              }
            }
        }
      }
   }

  lazy val queryService = client.contentType[QueryResult](application/(MimeTypes.json))
                                 .path("/analytics/v2/analytics/fs/")

  lazy val metaService = client.contentType[QueryResult](application/(MimeTypes.json))
                                .path("/analytics/v2/meta/fs/")

  lazy val asyncService = client.contentType[QueryResult](application/(MimeTypes.json))
                                .path("/analytics/v2/analytics/queries")

  override implicit val defaultFutureTimeouts: FutureTimeouts = FutureTimeouts(1, Duration(3, "second"))
  val shortFutureTimeouts = FutureTimeouts(1, Duration(50, "millis"))
}

class ShardServiceSpec extends TestShardService {
  def syncClient(query: String, apiKey: Option[String] = Some(testAPIKey)) = {
    apiKey.map{ queryService.query("apiKey", _) }.getOrElse(queryService).query("q", query)
  }

  def query(query: String, apiKey: Option[String] = Some(testAPIKey), format: Option[String] = None, path: String = ""): Future[HttpResponse[QueryResult]] = {
    val client = syncClient(query, apiKey)
    (format map (client.query("format", _)) getOrElse client).get(path)
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

  def extractJobId(jv: JValue): JobId = {
    jv \ "jobId" match {
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
      query(accessibleAbsoluteQuery).copoint must beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(_), _) => ok
      }
    }

    "create a job when an async query is posted" in {
      val res = for {
        HttpResponse(HttpStatus(Accepted, _), _, Some(Left(res)), _) <- asyncQuery(simpleQuery)
        jobId = extractJobId(res)
        job <- jobManager.findJob(jobId)
      } yield job

      res.copoint must beLike {
        case Some(Job(_, _, _, _, _, _)) => ok
      }
    }
    "results of an async job must eventually be made available" in {
      val res = for {
        HttpResponse(HttpStatus(Accepted, _), _, Some(Left(res)), _) <- asyncQuery(simpleQuery)
        jobId = extractJobId(res)
        _ <- waitForJobCompletion(jobId)
        HttpResponse(HttpStatus(OK, _), _, Some(Right(data)), _) <- asyncQueryResults(jobId)
        result <- extractResult(data)
      } yield result

      val expected = JObject(
        JField("warnings", JArray(Nil)) ::
        JField("errors", JArray(Nil)) ::
        JField("data", JArray(JNum(2) :: Nil)) ::
        Nil)

      res.copoint must_== expected
    }
    "handle relative query from accessible non-root path" in {
      query(relativeQuery, path = "/test").copoint must beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(_), _) => ok
      }
    }
    "reject query when no API key provided" in {
      query(simpleQuery, None).copoint must beLike {
        case HttpResponse(HttpStatus(BadRequest, "An apiKey query parameter is required to access this URL"), _, _, _) => ok
      }
    }
    "reject query when API key not found" in {
      query(simpleQuery, Some("not-gonna-find-it")).copoint must beLike {
        case HttpResponse(HttpStatus(Forbidden, _), _, Some(content), _) =>
          content must_== Left(JString("The specified API key does not exist: not-gonna-find-it"))
      }
    }
    "return 400 and errors if format is 'simple'" in {
      val result = for {
        HttpResponse(HttpStatus(BadRequest, _), _, Some(Left(result)), _) <- query("bad query")
      } yield result

      result.copoint must beLike {
        case JArray(JString("ERROR!") :: Nil) => ok
      }
    }
    "return warnings/errors if format is 'detailed'" in {
      val result = for {
        HttpResponse(HttpStatus(OK, _), _, Some(Right(data)), _) <- query(simpleQuery, format = Some("detailed"))
        result <- extractResult(data)
      } yield result

      val expected = JObject(
        JField("serverErrors", JArray(Nil)) ::
        JField("serverWarnings", JArray(Nil)) ::
        JField("warnings", JArray(Nil)) ::
        JField("errors", JArray(Nil)) ::
        JField("data", JArray(JNum(2) :: Nil)) ::
        Nil)

      result.copoint must_== expected
    }
    "return just the results if format is 'simple'" in {
      val result = for {
        HttpResponse(HttpStatus(OK, _), _, Some(Right(data)), _) <- query(simpleQuery, format = Some("simple"))
        result <- extractResult(data)
      } yield result

      val expected = JArray(JNum(2) :: Nil)
      result.copoint must_== expected
    }
  }

  "Data service" should {
    "reject read in the absence of read permissions" in todo
  }

  def browse(apiKey: Option[String] = Some(testAPIKey), path: String = "/test"): Future[HttpResponse[QueryResult]] = {
    apiKey.map{ metaService.query("apiKey", _) }.getOrElse(metaService).get(path)
  }

  def structure(apiKey: Option[String] = Some(testAPIKey), path: String = "/test", cpath: CPath = CPath.Identity): Future[HttpResponse[QueryResult]] = {
    apiKey.map{ metaService.query("apiKey", _) }.getOrElse(metaService).query("type", "structure").query("property", cpath.toString).get(path)
  }

  "Shard browse service" should {
    "handle browse for API key accessible path" in {
      val obj = JObject(Map("foo" -> JArray(JString("foo")::JString("bar")::Nil)))
      browse().copoint must beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(Left(obj)), _) => ok
        case HttpResponse(HttpStatus(NotFound, _), _, Some(Left(obj)), _) => 
          failure("Not found: " + obj.renderCompact)
      }
    }
    "reject browse when no API key provided" in {
      browse(None).copoint must beLike {
        case HttpResponse(HttpStatus(BadRequest, "An apiKey query parameter is required to access this URL"), _, _, _) => ok
      }
    }
    "reject browse when API key not found" in {
      browse(Some("not-gonna-find-it")).copoint must beLike {
        case HttpResponse(HttpStatus(Forbidden, _), _, Some(content), _) =>
          content must_== Left(JString("The specified API key does not exist: not-gonna-find-it"))
      }
    }
    /* Per John, this is not the desired behavior
    "return error response on browse failure" in {
      browse(path = "/errpath").copoint must beLike {
        case HttpResponse(HttpStatus(NotFound, _), _, Some(Left(response)), _) =>
          (response \ "errors") must beLike {
            case JArray(JString(err) :: Nil) =>
              err must startWith("Could not find any resource that corresponded to path")
          }
      }
    }
     */

    "return empty response on browse failure" in {
      browse(path = "/errpath").copoint must beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(Left(response)), _) =>
          response mustEqual JObject("size" -> JNum(0), "children" -> JArray(), "structure" -> JObject())
      }
    }
  }
}

trait TestPlatform extends ManagedPlatform { self =>
  import scalaz.syntax.monad._
  import scalaz.syntax.traverse._

  def actorSystem: ActorSystem
  implicit def executionContext: ExecutionContext
  val to = Duration(3, "seconds")

  implicit def M: Monad[Future]

  val accessControl: AccessControl[Future]
  val ownerMap: Map[Path, Set[AccountId]]

  private def toSlice[M[+_]: Monad](a: JValue): StreamT[M, Slice] = {
    Slice.fromJValues(Stream(a)) :: StreamT.empty[M, Slice]
  }

  type YggConfig = ManagedQueryModuleConfig
  object yggConfig extends YggConfig {
    val jobPollFrequency = Duration(2, "seconds")
    val clock = Clock.System
  }

  def asyncExecutorFor(apiKey: APIKey): EitherT[Future, String, QueryExecutor[Future, JobId]] = {
    EitherT.right(Future(new AsyncQueryExecutor {
      val executionContext = self.executionContext
    }))
  }

  def syncExecutorFor(apiKey: APIKey): EitherT[Future, String, QueryExecutor[Future, (Option[JobId], StreamT[Future, Slice])]] = {
    EitherT.right(Future(new SyncQueryExecutor {
      val executionContext = self.executionContext
    }))
  }

  protected def executor(implicit shardQueryMonad: JobQueryTFMonad): QueryExecutor[JobQueryTF, StreamT[JobQueryTF, Slice]] = {
    new QueryExecutor[JobQueryTF, StreamT[JobQueryTF, Slice]] {
      def execute(query: String, ctx: EvaluationContext, opts: QueryOptions) = {
        if (query == "bad query") {
          val mu = shardQueryMonad.jobId traverse { jobId =>
            jobManager.addMessage(jobId, JobManager.channels.Error, JString("ERROR!"))
          }

          EitherT[JobQueryTF, EvaluationError, StreamT[JobQueryTF, Slice]] {
            shardQueryMonad.liftM[Future, EvaluationError \/ StreamT[JobQueryTF, Slice]] {
              mu map { _ => \/.right(toSlice(JObject("value" -> JNum(2)))) }
            }
          }
        } else {
          EitherT[JobQueryTF, EvaluationError, StreamT[JobQueryTF, Slice]] {
            shardQueryMonad.point(\/.right(toSlice(JObject("value" -> JNum(2)))))
          }
        }
      }
    }
  }

  def status() = Future(Success(JArray(List(JString("status")))))

  def startup = Promise.successful(true)
  def shutdown = Future { actorSystem.shutdown; true }
}
