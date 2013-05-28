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
import com.precog.common.accounts._

import com.precog.daze._
import com.precog.yggdrasil.table.Slice
import com.precog.yggdrasil.execution._

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
  val account = AccountDetails("test", "test@test.test", clock.now(), apiKey, Path.Root, AccountPlan.Free)
  val ticker = actorSystem.actorOf(Props(new Ticker(ticks)))

  def execute(numTicks: Int, ticksToTimeout: Option[Int] = None): Future[JobId] = {
    val timeout = ticksToTimeout map { t => Duration(clock.duration * t, TimeUnit.MILLISECONDS) }
    val executionResult = for {
      executor <- asyncExecutorFor(apiKey) leftMap { EvaluationError.invalidState }
      ctx = EvaluationContext(apiKey, account, Path("/\\\\/\\///\\/"), clock.now())
      result <- executor.execute(numTicks.toString, ctx, QueryOptions(timeout = timeout))
    } yield result 

    executionResult.valueOr(err => sys.error(err.toString))
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

trait TestManagedPlatform extends ManagedExecution with ManagedQueryModule with SchedulableFuturesModule { self =>
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

      def execute(query: String, ctx: EvaluationContext, opts: QueryOptions) = {
        val numTicks = query.toInt
        EitherT.right[JobQueryTF, EvaluationError, StreamT[JobQueryTF, Slice]] {
          schedule(0) {
            StreamT.unfoldM[JobQueryTF, Slice, Int](0) {
              case i if i < numTicks =>
                schedule(1) {
                  Some((Slice.fromJValues(Stream(JObject("value" -> JString(".")))), i + 1))
                }.liftM[JobQueryT]

              case _ =>
                shardQueryMonad.point { None }
            }
          }.liftM[JobQueryT]
        }
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

  def startup = Future { true }
  def shutdown = Future { actorSystem.shutdown; true }
}
