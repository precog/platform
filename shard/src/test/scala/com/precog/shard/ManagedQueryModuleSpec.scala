package com.precog.shard

import com.precog.common._
import com.precog.common.jobs._

import com.precog.common.security._
import com.precog.daze._
import com.precog.yggdrasil.execution._
import com.precog.yggdrasil.table.Slice

import java.nio.CharBuffer

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic._

import akka.actor._
import akka.dispatch._
import akka.util.Duration

import blueeyes.util.Clock
import blueeyes.json._
import blueeyes.bkka._
import blueeyes.json.serialization.DefaultSerialization.{ DateTimeExtractor => _, DateTimeDecomposer => _, _ }

import org.specs2.mutable.Specification

import org.joda.time._

import scalaz._
import scalaz.std.option._
import scalaz.syntax.monad._
import scalaz.syntax.comonad._

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

class ManagedQueryModuleSpec extends TestManagedQueryModule with Specification {
  val actorSystem = ActorSystem("managedQueryModuleSpec")
  val jobActorSystem = ActorSystem("managedQueryModuleSpecJobs")
  implicit val executionContext = ExecutionContext.defaultExecutionContext(actorSystem)
  implicit val M: Monad[Future] with Comonad[Future] = new blueeyes.bkka.UnsafeFutureComonad(executionContext, Duration(30, "seconds"))

  val defaultTimeout = Duration(90, TimeUnit.SECONDS)

  val jobManager: JobManager[Future] = new InMemoryJobManager[Future]
  val apiKey = "O.o"
  def vfs = sys.error("I guess this is necessary.")

  var ticker: ActorRef = actorSystem.actorOf(Props(new Ticker(ticks)))

  def dropStreamToFuture = implicitly[Hoist[StreamT]].hoist[TestFuture, Future](new (TestFuture ~> Future) {
    def apply[A](fa: TestFuture[A]): Future[A] = fa.value
  })

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

  // Performs an incredibly intense compuation that requires numTicks ticks.
  def execute(numTicks: Int, ticksToTimeout: Option[Int] = None): Future[(JobId, AtomicInteger, Future[Int])] = {
    val timeout = ticksToTimeout map { t => Duration(clock.duration * t, TimeUnit.MILLISECONDS) }
    val result = for {
      // TODO: No idea how to work with EitherT[TestFuture, so sys.error it is]
      executor <- executorFor(apiKey) valueOr { err => sys.error(err.toString) }
      result0 <- executor.execute(apiKey, numTicks.toString, Path("/\\\\/\\///\\/"), QueryOptions(timeout = timeout)).valueOr(err => sys.error(err.toString)) mapValue { 
        case (w, s) => (w, (w: Option[(JobId, AtomicInteger)], s)) 
      }
    } yield {
      val (Some((jobId, ticks)), result) = result0

      def count(n: Int, cs0: StreamT[Future, CharBuffer]): Future[Int] = cs0.uncons flatMap {
        case Some((_, cs)) => count(n + 1, cs)
        case None => Future(n)
      }

      (jobId, ticks, count(0, dropStreamToFuture(result)))
    }

    result.value
  }

  // Cancels the job after `ticks` ticks.
  def cancel(jobId: JobId, ticks: Int): Future[Boolean] = {
    schedule(ticks) {
      jobManager.cancel(jobId, "Yarrrr", yggConfig.clock.now()).map { _.fold(_ => false, _ => true) }.copoint
    }
  }

  step {
    actorSystem.scheduler.schedule(Duration(0, "milliseconds"), Duration(clock.duration, "milliseconds")) {
        ticker ! Tick
    }
    startup.run.copoint
  }

  "A managed query" should {
    import JobState._

    "start in the start state" in {
      (for {
        (jobId, _, _) <- execute(5)
        job <- jobManager.findJob(jobId)
      } yield job).copoint must beLike {
        case Some(Job(_, _, _, _, _, Started(_, NotStarted))) => ok
      }
    }

    "be in a finished state if it completes successfully" in {
      (for {
        (jobId, _, _) <- execute(1)
        job <- waitForJobCompletion(jobId)
      } yield job).copoint must beLike {
        case Job(_, _, _, _, _, Finished(_, _)) => ok
      }
    }

    "complete successfully if not cancelled" in {
      val ticks = for {
        (_, _, query) <- execute(7)
        ticks <- query
      } yield ticks

      ticks.copoint must_== 7
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
        case Some(Job(_, _, _, _, _, Aborted(_, _, Cancelled(_, _, _)))) => ok
      }
    }

    "cannot be cancelled after it has successfully completed" in {
      val ticks = for {
        (jobId, _, query) <- execute(3)
        ticks <- query
        cancelled <- cancel(jobId, 3)
      } yield ticks

      ticks.copoint must_== 3
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
        case Some(Job(_, _, _, _, _, Expired(_, _))) => ok
      }
    }

    "not expire queries that complete before expiration date" in {
      val ticks = for {
        (jobId, _, query) <- execute(1, Some(10))
        _ <- waitFor(20)
        ticks <- query
      } yield ticks

      ticks.copoint must_== 1
    }
  }

  step {
    ticker ! Tick
    shutdown.run.copoint
    actorSystem.shutdown()
    actorSystem.awaitTermination()
  }
}

trait TestManagedQueryModule extends Platform[TestFuture, Slice, StreamT[TestFuture, CharBuffer]]
    with ManagedQueryModule with SchedulableFuturesModule { self =>

  def actorSystem: ActorSystem
  implicit def executionContext: ExecutionContext
  implicit def M: Monad[Future]

  def jobManager: JobManager[Future]

  type YggConfig = ManagedQueryModuleConfig

  object yggConfig extends ManagedQueryModuleConfig {
    val jobPollFrequency: Duration = Duration(20, "milliseconds")
    val clock = self.clock
  }

  def executorFor(apiKey: APIKey): EitherT[TestFuture, String, QueryExecutor[TestFuture, StreamT[TestFuture, CharBuffer]]] = {
    EitherT.right {
      Applicative[TestFuture] point {
        new QueryExecutor[TestFuture, StreamT[TestFuture, CharBuffer]] {
          import UserQuery.Serialization._

          def execute(apiKey: APIKey, query: String, prefix: Path, opts: QueryOptions) = {
            val userQuery = UserQuery(query, prefix, opts.sortOn, opts.sortOrder)
            val numTicks = query.toInt

            EitherT.right[TestFuture, EvaluationError, StreamT[TestFuture, CharBuffer]] {
              WriterT {
                createQueryJob(apiKey, Some(userQuery.serialize), opts.timeout) map { implicit M0 =>
                  val ticks = new AtomicInteger()
                  val result = StreamT.unfoldM[JobQueryTF, CharBuffer, Int](0) {
                    case i if i < numTicks =>
                      schedule(1) {
                        ticks.getAndIncrement()
                        Some((CharBuffer.wrap("."), i + 1))
                      }.liftM[JobQueryT]

                    case _ =>
                      M0.point { None }
                  }

                  (Tag(M0.jobId map (_ -> ticks)), completeJob(result))
                }
              }
            }
          }
        }
      }
    }
  }

  def startup = Applicative[TestFuture].point { true }
  def shutdown = Applicative[TestFuture].point { true }
}
