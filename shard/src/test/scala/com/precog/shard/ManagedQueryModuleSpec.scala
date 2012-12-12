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

import com.precog.common._
import com.precog.common.jobs._
import com.precog.common.security._

import com.precog.daze._

import java.nio.CharBuffer

import akka.actor.ActorSystem
import akka.dispatch._
import akka.util.Duration

import blueeyes.util.Clock
import blueeyes.json._
import blueeyes.bkka._

import org.specs2.mutable.Specification

import scalaz._
import scalaz.syntax.monad._
import scalaz.syntax.copointed._

class ManagedQueryModuleSpec extends TestManagedQueryExecutorFactory with Specification {
  val actorSystem = ActorSystem("managedQueryModuleSpec")
  implicit val executionContext = ExecutionContext.defaultExecutionContext(actorSystem)
  implicit val M: Monad[Future] = AkkaTypeClasses.futureApplicative(executionContext)
  implicit val coM = new Copointed[Future] {
    def map[A, B](m: Future[A])(f: A => B) = m map f
    def copoint[A](f: Future[A]) = Await.result(f, Duration(5, "seconds"))
  }

  val jobManager: JobManager[Future] = new InMemoryJobManager[Future]
  val apiKey = "O.o"
  val tickDuration = 50 // ms

  // Waits for `numTicks` ticks before returning, well, nothing useful.
  def waitFor(numTicks: Int): Future[Unit] = Future {
    Thread.sleep(tickDuration * numTicks)
    ()
  }

  // Performs an incredibly intense compuation that requires numTicks ticks.
  def execute(numTicks: Int): Future[(Option[JobId], Future[Either[Int, Int]])] = {
    for {
      executor <- executorFor(apiKey) map (_ getOrElse sys.error("Barrel of monkeys."))
    } yield {
      val result = executor.execute(apiKey, numTicks.toString, Path("/\\\\/\\///\\/"), QueryOptions())
      val stream = result getOrElse sys.error("I'm a lumberjack")

      executor.jobId -> (stream.foldLeft(0) { (acc, _) => acc + 1 } map (Right(_)) recover {
        case QueryCancelledException(_) => Left(executor.ticks)
      })
    }
  }

  // Cancels the job after `ticks` ticks.
  def cancel(jobId: Option[JobId], ticks: Int): Future[Boolean] = jobId map { jobId =>
    Thread.sleep(ticks * tickDuration)
    jobManager.cancel(jobId, "Yarrrr", yggConfig.clock.now()) map (_.fold(_ => false, _ => true))
  } getOrElse Future(false)

  step {
    startup().copoint
  }

  "A managed query" should {
    import JobState._

    "start in the start state" in {
      (for {
        (Some(jobId), _) <- execute(5)
        job <- jobManager.findJob(jobId)
      } yield job).copoint must beLike {
        case Some(Job(_, _, _, _, Started(_, NotStarted), _)) => ok
      }
    }

    "be in a finished state if it completes successfully" in {
      (for {
        (Some(jobId), _) <- execute(1)
        _ <- waitFor(5)
        job <- jobManager.findJob(jobId)
      } yield job).copoint must beLike {
        case Some(Job(_, _, _, _, Finished(None, _, _), _)) => ok
      }
    }

    "complete successfully if not cancelled" in {
      val ticks = for {
        (jobId, query) <- execute(10)
        ticks <- query
      } yield ticks

      ticks.copoint must_== Right(10)
    }

    "be cancellable" in {
      val ticks = for {
        (jobId, query) <- execute(10)
        cancelled <- cancel(jobId, 5)
        ticks <- query
      } yield ticks

      ticks.copoint must beLike {
        case Left(ticks) => ticks must be_<(10)
      }
    }

    "be in an aborted state if cancelled successfully" in {
      val job = for {
        (Some(jobId), query) <- execute(6)
        cancelled <- cancel(Some(jobId), 1)
        _ <- waitFor(8)
        job <- jobManager.findJob(jobId)
      } yield job

      job.copoint must beLike {
        case Some(Job(_, _, _, _, Aborted(_, _, Cancelled(_, _, _)), _)) => ok
      }
    }

    "cannot be cancelled after it has successfully completed" in {
      val ticks = for {
        (jobId, query) <- execute(10)
        cancelled <- cancel(jobId, 11)
        _ <- waitFor(12)
        ticks <- query
      } yield ticks

      ticks.copoint must_== Right(10)
    }
  }

  step {
    shutdown().copoint
  }
}

trait TestManagedQueryExecutorFactory extends QueryExecutorFactory[Future] with ManagedQueryModule {

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

  // We need to be able to access the JobId to cancel it!
  trait TestQueryExecutor[M[+_]] extends QueryExecutor[M] {
    def jobId: Option[JobId]
    def ticks: Int
  }

  def executorFor(apiKey: APIKey): Future[Validation[String, TestQueryExecutor[Future]]] = {
    (createJob(apiKey, "Test Shard Query")(executionContext) map { implicit M =>
      new TestQueryExecutor[Future] {
        def jobId = M.jobId
        var ticks: Int = 0

        def execute(apiKey: APIKey, query: String, prefix: Path, opts: QueryOptions) = {
          val numTicks = query.toInt
          val result = StreamT.unfoldM[ShardQuery, CharBuffer, Int](0) {
            case i if i < numTicks =>
              M.point {
                // Super crazy computation.
                ticks += 1
                Thread.sleep(tickDuration)
                Some((tick, i + 1))
              }

            case _ =>
              M.point { None }
          }

          Success(completeJob(result))
        }
      }
    }).validation
  }

  def browse(apiKey: APIKey, path: Path) = sys.error("No loitering, move along.")
  def structure(apiKey: APIKey, path: Path) = sys.error("I'm an amorphous blob you insensitive clod!")
  def status() = sys.error("The lowliest of the low :(")

  def startup = Future { true }
  def shutdown = Future { actorSystem.shutdown; true }
}

