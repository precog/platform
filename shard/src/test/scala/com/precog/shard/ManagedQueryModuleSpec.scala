package com.precog.shard

import com.precog.common._
import com.precog.common.jobs._
import com.precog.common.security._

import com.precog.daze._

import java.nio.CharBuffer

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorSystem
import akka.dispatch._
import akka.util.Duration

import blueeyes.util.Clock
import blueeyes.json._
import blueeyes.bkka._

import org.specs2.mutable.Specification

import scalaz._
import scalaz.std.option._
import scalaz.syntax.monad._
import scalaz.syntax.copointed._

object ManagedQueryTestSupport {

  // We use TestFuture as our monad for QueryExecutor, as this let's us pass
  // some information we need for testing outside of the query execution;
  // namely, the JobId and a counter that is incremented after each tick.
  type TestFuture[+A] = WriterT[Future, FirstOption[(JobId, AtomicInteger)], A]

  // If we have a natural transformation from M ~> Future, then we can go
  // through Future to get to TestFuture. This will let us use completeJob
  // with a StreamT[TestFuture, ?] by using `sink` in ManagedQueryModule.
  implicit def transformThroughFuture[M[+_]](implicit t: M ~> Future) = new (M ~> TestFuture) {
    def apply[A](ma: M[A]): TestFuture[A] = WriterT(t(ma) map (Tag(None) -> _))
  }
}

import ManagedQueryTestSupport._

class ManagedQueryModuleSpec extends TestManagedQueryExecutorFactory with Specification {
  val actorSystem = ActorSystem("managedQueryModuleSpec")
  implicit val executionContext = ExecutionContext.defaultExecutionContext(actorSystem)
  implicit val M: Monad[Future] with Copointed[Future] = new blueeyes.bkka.FutureMonad(executionContext) with Copointed[Future] {
    def copoint[A](m: Future[A]) = Await.result(m, Duration(5, "seconds"))
  }

  val jobManager: JobManager[Future] = new InMemoryJobManager[Future]
  val apiKey = "O.o"
  val tickDuration = 50 // ms

  val dropStreamToFuture = implicitly[Hoist[StreamT]].hoist[TestFuture, Future](new (TestFuture ~> Future) {
    def apply[A](fa: TestFuture[A]): Future[A] = fa.value
  })

  // Waits for `numTicks` ticks before returning, well, nothing useful.
  def waitFor(numTicks: Int): Future[Unit] = Future {
    Thread.sleep(tickDuration * numTicks)
    ()
  }

  // Performs an incredibly intense compuation that requires numTicks ticks.
  def execute(numTicks: Int, ticksToTimeout: Option[Int] = None): Future[(JobId, AtomicInteger, Future[Int])] = {
    val timeout = ticksToTimeout map (tickDuration.toLong * _)
    val result = for {
      executor <- executorFor(apiKey) map (_ getOrElse sys.error("Barrel of monkeys."))
      result0 <- executor.execute(apiKey, numTicks.toString, Path("/\\\\/\\///\\/"), QueryOptions(timeout = timeout)) mapValue {
        case (w, s) => (w, (w: Option[(JobId, AtomicInteger)], s))
      }
    } yield {
      val (Some((jobId, ticks)), result) = result0

      def count(n: Int, cs0: StreamT[Future, CharBuffer]): Future[Int] = cs0.uncons flatMap {
        case Some((_, cs)) => count(n + 1, cs)
        case None => Future(n)
      }

      (jobId, ticks, count(0, result map (dropStreamToFuture(_)) getOrElse sys.error("Escape boat life jacket")))
    }

    result.value
  }

  // Cancels the job after `ticks` ticks.
  def cancel(jobId: JobId, ticks: Int): Future[Boolean] = {
    Future { Thread.sleep(ticks * tickDuration) } flatMap { _ =>
      jobManager.cancel(jobId, "Yarrrr", yggConfig.clock.now()) map (_.fold(_ => false, _ => true))
    }
  }

  step {
    startup().copoint
  }

  "A managed query" should {
    import JobState._

    "start in the start state" in {
      (for {
        (jobId, _, _) <- execute(5)
        job <- jobManager.findJob(jobId)
      } yield job).copoint must beLike {
        case Some(Job(_, _, _, _, Started(_, NotStarted))) => ok
      }
    }

    "be in a finished state if it completes successfully" in {
      (for {
        (jobId, _, _) <- execute(1)
        _ <- waitFor(5)
        job <- jobManager.findJob(jobId)
      } yield job).copoint must beLike {
        case Some(Job(_, _, _, _, Finished(None, _, _))) => ok
      }
    }

    "complete successfully if not cancelled" in {
      val ticks = for {
        (_, _, query) <- execute(13)
        ticks <- query
      } yield ticks

      ticks.copoint must_== 13
    }

    "be cancellable" in {
      val result = for {
        (jobId, ticks, query) <- execute(10)
        cancelled <- cancel(jobId, 5)
      } yield (ticks, query)

      result.copoint must beLike {
        case (ticks, query) =>
          query.copoint must throwA[QueryCancelledException]
          ticks.get must be_<(10)
      }
    }

    "be in an aborted state if cancelled successfully" in {
      val job = for {
        (jobId, _, query) <- execute(6)
        cancelled <- cancel(jobId, 1)
        _ <- waitFor(8)
        job <- jobManager.findJob(jobId)
      } yield job

      job.copoint must beLike {
        case Some(Job(_, _, _, _, Aborted(_, _, Cancelled(_, _, _)))) => ok
      }
    }

    "cannot be cancelled after it has successfully completed" in {
      val ticks = for {
        (jobId, _, query) <- execute(10)
        cancelled <- cancel(jobId, 11)
        _ <- waitFor(12)
        ticks <- query
      } yield ticks

      ticks.copoint must_== 10
    }

    "be expireable" in {
      execute(10, Some(3)).copoint must beLike {
        case (_, ticks, query) =>
          query.copoint must throwA[QueryExpiredException]
          ticks.get must be_<(10)
      }
    }

    "expired queries are put in an expired state" in {
      (for {
        (jobId, _, _) <- execute(10, Some(2))
        _ <- waitFor(5)
        job <- jobManager.findJob(jobId)
      } yield job).copoint must beLike {
        case Some(Job(_, _, _, _, Expired(_, _))) => ok
      }
    }

    "not expire queries that complete before expiration date" in {
      val ticks = for {
        (jobId, _, query) <- execute(5, Some(8))
        _ <- waitFor(10)
        ticks <- query
      } yield ticks

      ticks.copoint must_== 5
    }
  }

  step {
    shutdown().copoint
  }
}

trait TestManagedQueryExecutorFactory extends QueryExecutorFactory[TestFuture] with ManagedQueryModule {

  def actorSystem: ActorSystem  
  implicit def executionContext: ExecutionContext
  implicit def M: Monad[Future]
  
  val jobManager: JobManager[Future]
  def tickDuration: Int

  val tick = CharBuffer.wrap(".")

  type YggConfig = ManagedQueryModuleConfig

  object yggConfig extends ManagedQueryModuleConfig {
    val jobPollFrequency: Duration = Duration(10, "milliseconds")
    val clock = Clock.System
  }

  def executorFor(apiKey: APIKey): TestFuture[Validation[String, QueryExecutor[TestFuture]]] = {
    Pointed[TestFuture].point(Success(new QueryExecutor[TestFuture] {
      def execute(apiKey: APIKey, query: String, prefix: Path, opts: QueryOptions) = {
        val numTicks = query.toInt
        val expiration = opts.timeout map (yggConfig.clock.now().plus(_))
        WriterT(createJob(apiKey, query, expiration) map { implicit M =>
          val ticks = new AtomicInteger()
          val result = StreamT.unfoldM[ShardQuery, CharBuffer, Int](0) {
            case i if i < numTicks =>
              M.point {
                // Super crazy computation.
                ticks.getAndIncrement()
                Thread.sleep(tickDuration)
                Some((tick, i + 1))
              }

            case _ =>
              M.point { None }
          }

          (Tag(M.jobId map (_ -> ticks)), Success(completeJob(result)))
        })
      }
    }))
  }

  def browse(apiKey: APIKey, path: Path) = sys.error("No loitering, move along.")
  def structure(apiKey: APIKey, path: Path) = sys.error("I'm an amorphous blob you insensitive clod!")
  def status() = sys.error("The lowliest of the low :(")

  def startup = Pointed[TestFuture].point { true }
  def shutdown = Pointed[TestFuture].point { actorSystem.shutdown; true }
}

