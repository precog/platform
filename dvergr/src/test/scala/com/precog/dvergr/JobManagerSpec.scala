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
package com.precog.dvergr

import com.precog.common.jobs._
import com.precog.common.security.APIKey
import com.precog.common.client._
import com.precog.util.IOUtils

import org.specs2.mutable._
import org.specs2.specification.{Fragments, Step}

import org.joda.time.DateTime

import blueeyes.core.http._
import blueeyes.core.service._
import blueeyes.core.data._

import blueeyes.bkka._
import blueeyes.json._
import blueeyes.persistence.mongo._

import akka.util.{ Timeout, Duration }
import akka.dispatch._
import akka.actor.ActorSystem

import org.streum.configrity.Configuration

import scalaz._
import scalaz.Id.Id

class InMemoryJobManagerSpec extends Specification {
  include(new JobManagerSpec[Id] {
    val validAPIKey = "Anything should work!"
    val jobs = new InMemoryJobManager[Id]
    val M: Monad[Id] with Comonad[Id] = implicitly
  })
}

class FileJobManagerSpec extends Specification {
  val tempDir = IOUtils.createTmpDir("FileJobManagerSpec").unsafePerformIO

  include(new JobManagerSpec[Id] {
    val validAPIKey = "Anything should work!"
    val jobs = FileJobManager[Id](tempDir, Id.id)
    val M: Monad[Id] with Comonad[Id] = implicitly
  })

  override def map(fs: => Fragments) = Step { IOUtils.recursiveDelete(tempDir).unsafePerformIO } ^ fs
}

class WebJobManagerSpec extends TestJobService { self =>
  include(new JobManagerSpec[Future] {
    val validAPIKey = self.validAPIKey

    implicit val executionContext = self.executionContext
    implicit val M: Monad[Future] with Comonad[Future] = new UnsafeFutureComonad(executionContext, Duration(5, "seconds"))

    val jobs = (new WebJobManager {
      val executionContext = self.executionContext
      val M = self.M
      protected def withRawClient[A](f: HttpClient[ByteChunk] => A): A = f(client.path("/jobs/v1/"))
    }).withM[Future](ResponseAsFuture(M), FutureAsResponse(M), Monad[Response], M)
  })
}

class MongoJobManagerSpec extends Specification with RealMongoSpecSupport { self =>
  var actorSystem: ActorSystem = _
  implicit def executionContext = actorSystem.dispatcher

  step {
    actorSystem = ActorSystem("mongo-job-manager-spec")
  }

  include(new JobManagerSpec[Future] {
    val validAPIKey = "Anything should work!"
    implicit lazy val M: Monad[Future] with Comonad[Future] = new UnsafeFutureComonad(executionContext, Duration(5, "seconds"))
    lazy val jobs = new MongoJobManager(mongo.database("jobs"), MongoJobManagerSettings.default, new InMemoryFileStorage[Future])
  })

  step {
    actorSystem.shutdown()
  }
}

trait JobManagerSpec[M[+_]] extends Specification {
  import JobState._

  import scalaz.syntax.monad._
  import scalaz.syntax.comonad._

  implicit def M: Monad[M] with Comonad[M]

  def validAPIKey: APIKey

  def jobs: JobManager[M]

  "job managers" should {
    "create jobs that aren't started" >> {
      val job = jobs.createJob(validAPIKey, "name", "job type", None, None).copoint
      job must beLike { case Job(_, _, "name", "job type", None, NotStarted) => ok }
    }

    "create started jobs" in {
      val t = new DateTime
      val job = jobs.createJob(validAPIKey, "name", "job type", None, Some(t)).copoint
      job must beLike { case Job(_, _, "name", "job type", None, Started(`t`, NotStarted)) => ok }
    }

    "find created jobs" in {
      val job = jobs.createJob(validAPIKey, "name", "job type", None, None).copoint
      val job2 = jobs.findJob(job.id).copoint
      job2 must_== Some(job)

      jobs.findJob("nonexistant").copoint must_== None
    }

    "create jobs with data" in {
      val data: JValue = JObject(List(
        JField("a", JArray(JString("a"), JNum(2))),
        JField("b", JNum(1.675))
      ))

      val job = jobs.createJob(validAPIKey, "name", "job type", Some(data), None).copoint
      val job2 = jobs.findJob(job.id).copoint
      job must beLike { case Job(_, _, "name", "job type", Some(`data`), NotStarted) => ok }
      job2 must beLike { case Some(Job(_, _, "name", "job type", Some(`data`), NotStarted)) => ok }
    }

    "always update a job's status when not given previous" in {
      val job = jobs.createJob(validAPIKey, "b", "c", None, Some(new DateTime)).copoint
      val jobId = job.id
      val status1 = jobs.updateStatus(job.id, None, "1", 0.0, "%", None).copoint
      val status2 = jobs.updateStatus(job.id, None, "2", 5.0, "%", Some(JString("..."))).copoint
      val (s0, s5) = (BigDecimal(0), BigDecimal(5))
      status1 must beLike { case Right(Status(`jobId`, _, "1", `s0`, "%", None)) => ok }
      status2 must beLike { case Right(Status(`jobId`, _, "2", `s5`, "%", Some(JString("...")))) => ok }
    }

    "update a job's status if given correct previous status" in {
      val job = jobs.createJob(validAPIKey, "b", "c", None, Some(new DateTime)).copoint
      val jobId = job.id
      val status1 = jobs.updateStatus(job.id, None, "1", 0.0, "%", None).copoint.right.toOption
      val status1x = jobs.getStatus(job.id).copoint

      status1 must_== status1x

      val (s0, s5) = (BigDecimal(0), BigDecimal(5))

      status1 must beLike {
        case Some(Status(`jobId`, id, "1", `s0`, "%", None)) =>
          val status2 = jobs.updateStatus(job.id, Some(id), "2", 5.0, "%", Some(JString("..."))).copoint.right.toOption
          val status2x = jobs.getStatus(job.id).copoint
          status2 must_== status2x
          status2 must beLike { case Some(Status(`jobId`, _, "2", `s5`, "%", Some(JString("...")))) => ok }
      }
    }

    "refuse to update a job's status if given incorrect previous status" in {
      val jobId = jobs.createJob(validAPIKey, "b", "c", None, Some(new DateTime)).copoint.id
      val status1 = jobs.updateStatus(jobId, None, "1", 0.0, "%", None).copoint.right.toOption
      val status1x = jobs.getStatus(jobId).copoint

      status1 must_== status1x
      val s0 = BigDecimal(0)

      status1 must beLike {
        case Some(Status(`jobId`, id, "1", `s0`, "%", None)) =>
          val status2 = jobs.updateStatus(jobId, Some(id + 1), "2", 5.0, "%", Some(JString("..."))).copoint
          val status2x = jobs.getStatus(jobId).copoint
          status2x must_== status1
          status2 must beLike { case Left(_) => ok }
      }
    }

    "put job statuses in 'status' message channel" in {
      val job = jobs.createJob(validAPIKey, "b", "c", None, Some(new DateTime)).copoint
      val status1 = Status.toMessage(jobs.updateStatus(job.id, None, "1", 0.0, "%", None).copoint.right getOrElse sys.error("..."))
      val status2 = Status.toMessage(jobs.updateStatus(job.id, None, "2", 5.0, "%", Some(JString("..."))).copoint.right getOrElse sys.error("..."))
      jobs.listChannels(job.id).copoint must contain(JobManager.channels.Status)
      val statuses = jobs.listMessages(job.id, JobManager.channels.Status, None).copoint
      statuses must contain(status1, status2).inOrder
    }

    "add arbitrary messages in any channel" in {
      val job1 = jobs.createJob(validAPIKey, "b", "c", None, Some(new DateTime)).copoint
      val job2 = jobs.createJob(validAPIKey, "b", "c", None, Some(new DateTime)).copoint
      val m1 = jobs.addMessage(job1.id, "abc", JString("Hello, world!")).copoint
      val m2 = jobs.addMessage(job1.id, "group therapy", JString("My name is: Bob")).copoint
      val m3 = jobs.addMessage(job1.id, "group therapy", JString("Hi Bob!")).copoint
      val m4 = jobs.addMessage(job2.id, "cba", JString("Goodbye, cruel world!")).copoint

      val abc = jobs.listMessages(job1.id, "abc", None).copoint.toList
      val therapy = jobs.listMessages(job1.id, "group therapy", None).copoint.toList
      val cba = jobs.listMessages(job2.id, "cba", None).copoint.toList

      abc must_== List(m1)
      therapy must_== List(m2, m3)
      cba must_== List(m4)
    }

    "return empty seq for non-existant channel" in {
      val job = jobs.createJob(validAPIKey, "b", "c", None, Some(new DateTime)).copoint
      jobs.listMessages(job.id, "zzz", None).copoint.toList must_== Nil
    }

    "return latest results when previous message provided" in {
      val job = jobs.createJob(validAPIKey, "b", "c", None, Some(new DateTime)).copoint

      def say(name: String, message: String): JValue = JObject(List(
        JField("name", JString(name)), JField("message", JString(message))
      ))

      val m1 = jobs.addMessage(job.id, "chat", say("Tom", "Hello")).copoint
      val m2 = jobs.addMessage(job.id, "chat", say("Bob", "Hi")).copoint
      val m3 = jobs.addMessage(job.id, "chat", say("Tom", "How are you?")).copoint
      val m4 = jobs.addMessage(job.id, "chat", say("Bob", "Not so great.")).copoint
      val m5 = jobs.addMessage(job.id, "chat", say("Tom", "Why?")).copoint
      val m6 = jobs.addMessage(job.id, "chat", say("Bob", "Because I'm writing tests.")).copoint
      val m7 = jobs.addMessage(job.id, "chat", say("Tom", "That sucks.")).copoint

      jobs.listMessages(job.id, "chat", Some(m1.id)).copoint.toList must_== List(m2, m3, m4, m5, m6, m7)
      jobs.listMessages(job.id, "chat", Some(m4.id)).copoint.toList must_== List(m5, m6, m7)
      jobs.listMessages(job.id, "chat", Some(m6.id)).copoint.toList must_== List(m7)
      jobs.listMessages(job.id, "chat", Some(m7.id)).copoint.toList must_== Nil
      jobs.listMessages(job.id, "chat", None).copoint.toList must_== List(m1, m2, m3, m4, m5, m6, m7)
    }

    "list channels that have been posted to" in {
      val job = jobs.createJob(validAPIKey, "b", "c", None, Some(new DateTime)).copoint
      jobs.addMessage(job.id, "a", JString("a")).copoint
      jobs.addMessage(job.id, "b", JString("a")).copoint
      jobs.addMessage(job.id, "c", JString("a")).copoint
      jobs.listChannels(job.id).copoint must contain("a", "b", "c")
    }

    "allow jobs to be cancelled" in {
      val job = jobs.createJob(validAPIKey, "b", "c", None, Some(new DateTime)).copoint
      val state = job.state
      val jobId = job.id

      jobs.cancel(job.id, "I didn't like the way it looked at me.").copoint must beLike {
        case Right(Job(`jobId`, _, _, _, _, Cancelled("I didn't like the way it looked at me.", _, `state`))) => ok
      }

      jobs.findJob(jobId).copoint must beLike {
        case Some(Job(`jobId`, _, _, _, _, Cancelled(_, _, `state`))) => ok
      }
    }

    "not allow a double cancellation" in {
      val job = jobs.createJob(validAPIKey, "b", "c", None, Some(new DateTime)).copoint
      jobs.cancel(job.id, "It was redundant.").copoint must beLike { case Right(_) => ok }
      jobs.cancel(job.id, "It was redundant.").copoint must beLike { case Left(_) => ok }
    }

    "allow aborts from non-terminal state" in {
      val job1 = jobs.createJob(validAPIKey, "b", "c", None, Some(new DateTime)).copoint
      val job1Id = job1.id
      val job1State = job1.state

      jobs.abort(job1Id, "The mission was compromised.").copoint must beLike {
        case Right(Job(`job1Id`, _, _, _, _, Aborted("The mission was compromised.", _, `job1State`))) => ok
      }

      val job2Id = jobs.createJob(validAPIKey, "b", "c", None, Some(new DateTime)).copoint.id
      jobs.cancel(job2Id, "Muwahaha").copoint must beLike { case Right(job2) =>
        val job2State = job2.state

        jobs.abort(job2.id, "Blagawaga").copoint must beLike {
          case Right(Job(`job2Id`, _, _, _, _, Aborted("Blagawaga", _, `job2State`))) => ok
        }
      }
      
      val job3 = jobs.createJob(validAPIKey, "b", "c", None, None).copoint
      val job3Id = job3.id

      jobs.abort(job3Id, "Because I could.").copoint must beLike {
        case Right(Job(`job3Id`, _, _, _, _, Aborted("Because I could.", _, NotStarted))) => ok
      }
    }

    "allow a job to be started after it is created" in {
      val job = jobs.createJob(validAPIKey, "b", "c", None, None).copoint
      val jobId = job.id
      val dt = new DateTime
      jobs.start(job.id, dt).copoint must beLike {
        case Right(Job(`jobId`, _, _, _, _, Started(`dt`, NotStarted))) => ok
      }
    }

    "ensure jobs are only started once" in {
      val job = jobs.createJob(validAPIKey, "b", "c", None, None).copoint
      jobs.start(job.id).copoint
      jobs.start(job.id).copoint must beLike { case Left(_) => ok }

      val job2 = jobs.createJob(validAPIKey, "b", "c", None, Some(new DateTime)).copoint
      jobs.start(job2.id).copoint must beLike { case Left(_) => ok }
    }

    "finish jobs and preserve result" in {
      import MimeTypes._

      val job = jobs.createJob(validAPIKey, "b", "c", None, None).copoint
      jobs.start(job.id).copoint
      val result = JobResult(List(MimeTypes.text / plain), "Hello, world!".getBytes())
      jobs.finish(job.id).copoint must beLike {
        case Right(Job(_, _, _, _, _, Finished(_, _))) => ok
      }
      jobs.findJob(job.id).copoint must beLike {
        case Some(Job(_, _, _, _, _, Finished(_, _))) => ok
      }
    }
  }
}

