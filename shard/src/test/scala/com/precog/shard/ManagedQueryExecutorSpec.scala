package com.precog.shard

import com.precog.common._
import com.precog.common.jobs._

import com.precog.common.security._

import com.precog.daze._
import com.precog.muspelheim._
import com.precog.yggdrasil.execution._
import com.precog.yggdrasil.table.Slice

import java.nio.CharBuffer

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import akka.actor._
import akka.dispatch._
import akka.util.Duration

import blueeyes.util.Clock
import blueeyes.core.http.{ MimeType, MimeTypes }
import blueeyes.json._
import blueeyes.bkka._
import blueeyes.json.serialization.DefaultSerialization.{ DateTimeExtractor => _, DateTimeDecomposer => _, _ }

import org.specs2.mutable.Specification

import scalaz._
import scalaz.std.option._
import scalaz.syntax.monad._
import scalaz.syntax.comonad._

class ManagedQueryExecutorSpec extends TestManagedPlatform with Specification {
  import JobState._

  val JSON = MimeTypes.application / MimeTypes.json

  val actorSystem = ActorSystem("managedQueryModuleSpec")
  val jobActorSystem = ActorSystem("managedQueryModuleSpecJobActorSystem")
  implicit val executionContext = ExecutionContext.defaultExecutionContext(actorSystem)
  implicit val M: Monad[Future] with Comonad[Future] = new blueeyes.bkka.UnsafeFutureComonad(executionContext, Duration(15, "seconds"))
  val defaultTimeout = Duration(90, TimeUnit.SECONDS)

  val jobManager: JobManager[Future] = new InMemoryJobManager[Future]
  val apiKey = "O.o"

  def execute(numTicks: Int, ticksToTimeout: Option[Int] = None): Future[JobId] = {
    val timeout = ticksToTimeout map { t => Duration(clock.duration * t, TimeUnit.MILLISECONDS) }
    (for {
      executor <- asyncExecutorFor(apiKey)
      result <- executor.execute(apiKey, numTicks.toString, Path("/\\\\/\\///\\/"), QueryOptions(timeout = timeout)).leftMap(_.toString)
    } yield result) valueOr sys.error("Lemon curry?")
  }

  def cancel(jobId: JobId, ticks: Int): Future[Boolean] = schedule(ticks) {
    jobManager.cancel(jobId, "Yarrrr", yggConfig.clock.now()).map (_.fold(_ => false, _ => true)).copoint
  }

  def poll(jobId: JobId): Future[Option[(Option[MimeType], String)]] = {
    jobManager.getResult(jobId) flatMap {
      case Left(_) =>
        Future(None)
      case Right((mimeType, stream)) =>
        stream.foldLeft(new Array[Byte](0))(_ ++ _) map { data => Some(mimeType -> new String(data, "UTF-8")) }
    }
  }

  def waitForJobCompletion(jobId: JobId): Future[Job] = {
    import JobState._

    for {
      _ <- waitFor(1)
      Some(job) <- jobManager.findJob(jobId)
      finalJob <- job.state match {
        case NotStarted | Started(_, _) | Cancelled(_, _, _) =>
          waitForJobCompletion(jobId)
        case _ =>
          Future(job)
      }
    } yield finalJob
  }

  val ticker = actorSystem.actorOf(Props(new Ticker(ticks)))

  step {
    actorSystem.scheduler.schedule(Duration(0, "milliseconds"), Duration(clock.duration, "milliseconds")) {
        ticker ! Tick
    }

    startup.copoint
  }

  "An asynchronous query" should {
    "return a job ID" in {
      execute(1).copoint must not(throwA[Exception])
    }

    "return the results of a completed job" in {
      val result = for {
        jobId <- execute(3)
        _ <- waitForJobCompletion(jobId)
        _ <- waitFor(3)
        result <- poll(jobId)
      } yield result

      result.copoint must_== Some((Some(JSON), """[".",".","."]"""))
    }

    "not return results if the job is still running" in {
      val results = for {
        jobId <- execute(20)
        _ <- waitFor(1)
        results <- poll(jobId)
      } yield results

      results.copoint must_== None
    }

    "be in the finished state if the job has finished" in {
      val result = for {
        jobId <- execute(1)
        job <- waitForJobCompletion(jobId)
      } yield job

      result.copoint must beLike {
        case Job(_, _, _, _, _, Finished(_, _)) => ok
      }
    }

    "not return the results of an aborted job" in {
      val result = for {
        jobId <- execute(8)
        _ <- cancel(jobId, 1)
        _ <- waitForJobCompletion(jobId)
        result <- poll(jobId)
      } yield result

      result.copoint must_== None
    }
  }

  step {
    shutdown.copoint
    actorSystem.shutdown()
    actorSystem.awaitTermination()
  }
}

trait TestManagedPlatform extends ManagedPlatform with ManagedQueryModule with SchedulableFuturesModule { self =>
  def actorSystem: ActorSystem
  implicit def executionContext: ExecutionContext
  implicit def M: Monad[Future]
  
  val jobManager: JobManager[Future]

  type YggConfig = ManagedQueryModuleConfig

  object yggConfig extends ManagedQueryModuleConfig {
    val jobPollFrequency: Duration = Duration(10, "milliseconds")
    val clock = self.clock
  }

  protected def executor(implicit shardQueryMonad: JobQueryTFMonad): QueryExecutor[JobQueryTF, StreamT[JobQueryTF, Slice]] = {
    new QueryExecutor[JobQueryTF, StreamT[JobQueryTF, Slice]] {

      import UserQuery.Serialization._

      def execute(apiKey: APIKey, query: String, prefix: Path, opts: QueryOptions) = {
        val numTicks = query.toInt
        schedule(0) {
          Success(StreamT.unfoldM[JobQueryTF, Slice, Int](0) {
            case i if i < numTicks =>
              schedule(1) {
                Some((Slice.fromJValues(Stream(JString("."))), i + 1))
              }.liftM[JobQueryT]

            case _ =>
              shardQueryMonad.point { None }
          })
        }.liftM[JobQueryT]
      }
    }
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

  val metadataClient = new MetadataClient[Future] {
    def size(userUID: String, path: Path) = sys.error("todo")
    def browse(apiKey: APIKey, path: Path) = sys.error("No loitering, move along.")
    def structure(apiKey: APIKey, path: Path, cpath: CPath) = sys.error("I'm an amorphous blob you insensitive clod!")
    def currentVersion(apiKey: APIKey, path: Path) = M.point(None)
    def currentAuthorities(apiKey: APIKey, path: Path) = M.point(None)
  }

  def startup = Future { true }
  def shutdown = Future { actorSystem.shutdown; true }
}
