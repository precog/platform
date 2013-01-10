package com.precog.shard

import com.precog.common._
import com.precog.common.jobs._
import com.precog.common.security._

import com.precog.daze._

import java.nio.CharBuffer

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
import scalaz.syntax.copointed._

class AsyncQueryExecutorSpec extends TestAsyncQueryExecutorFactory with Specification {
  import JobState._

  val JSON = MimeTypes.application / MimeTypes.json

  val actorSystem = ActorSystem("managedQueryModuleSpec")
  implicit val executionContext = ExecutionContext.defaultExecutionContext(actorSystem)
  implicit val M: Monad[Future] with Copointed[Future] = new blueeyes.bkka.FutureMonad(executionContext) with Copointed[Future] {
    def copoint[A](m: Future[A]) = Await.result(m, Duration(15, "seconds"))
  }

  val jobManager: JobManager[Future] = new InMemoryJobManager[Future]
  val apiKey = "O.o"

  def execute(numTicks: Int, ticksToTimeout: Option[Int] = None): Future[JobId] = {
    val timeout = ticksToTimeout map (clock.duration * _)
    for {
      executor <- asyncExecutorFor(apiKey) map (_ getOrElse sys.error("Barrel of monkeys."))
      result <- executor.execute(apiKey, numTicks.toString, Path("/\\\\/\\///\\/"), QueryOptions(timeout = timeout))
    } yield {
      result getOrElse sys.error("Jellybean Sunday")
    }
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
    startup().copoint
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

      result.copoint must_== Some((Some(JSON), "..."))
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
    shutdown().copoint
    actorSystem.shutdown()
    actorSystem.awaitTermination()
  }
}

trait TestAsyncQueryExecutorFactory extends AsyncQueryExecutorFactory with ManagedQueryModule with SchedulableFuturesModule { self =>
  def actorSystem: ActorSystem
  implicit def executionContext: ExecutionContext
  implicit def M: Monad[Future]
  
  val jobManager: JobManager[Future]

  type YggConfig = ManagedQueryModuleConfig

  object yggConfig extends ManagedQueryModuleConfig {
    val jobPollFrequency: Duration = Duration(10, "milliseconds")
    val clock = self.clock
  }

  protected def executor(implicit shardQueryMonad: ShardQueryMonad): QueryExecutor[ShardQuery, StreamT[ShardQuery, CharBuffer]] = {
    new QueryExecutor[ShardQuery, StreamT[ShardQuery, CharBuffer]] {

      import UserQuery.Serialization._

      def execute(apiKey: APIKey, query: String, prefix: Path, opts: QueryOptions) = {
        val numTicks = query.toInt
        schedule(0) {
          Success(StreamT.unfoldM[ShardQuery, CharBuffer, Int](0) {
            case i if i < numTicks =>
              schedule(1) {
                Some((CharBuffer.wrap("."), i + 1))
              }.liftM[JobQueryT]

            case _ =>
              shardQueryMonad.point { None }
          })
        }.liftM[JobQueryT]
      }
    }
  }

  def asyncExecutorFor(apiKey: APIKey): Future[Validation[String, QueryExecutor[Future, JobId]]] = {
    Future(Success(new AsyncQueryExecutor {
      val executionContext = self.executionContext
    }))
  }

  def executorFor(apiKey: APIKey): Future[Validation[String, QueryExecutor[Future, StreamT[Future, CharBuffer]]]] = {
    Future(Success(new SyncQueryExecutor {
      val executionContext = self.executionContext
    }))
  }

  def browse(apiKey: APIKey, path: Path) = sys.error("No loitering, move along.")
  def structure(apiKey: APIKey, path: Path) = sys.error("I'm an amorphous blob you insensitive clod!")
  def status() = sys.error("The lowliest of the low :(")

  def startup = Future { true }
  def shutdown = Future { actorSystem.shutdown; true }
}
