package com.precog.daze

import com.precog.common.jobs._

import blueeyes.util.Clock
import blueeyes.json._

import org.specs2.mutable.Specification

import scalaz._
import scalaz.syntax.monad._
import scalaz.syntax.copointed._

class JobQueryLoggerSpec extends Specification {
  import JobManager._
  import JobState._

  def withReport[A](f: JobQueryLogger[Need] => A): A = {
    f(new JobQueryLogger[Need] {
      val M = Need.need
      val clock = Clock.System
      val jobManager = new InMemoryJobManager[Need]
      val jobId = jobManager.createJob("password", "error-report-spec", "hard", None, Some(clock.now())).copoint.id
    })
  }

  def testChannel(channel: String)(f: (QueryLogger[Need], String) => Need[Unit]) = {
    withReport { report =>
      val messages = (for {
        _ <- f(report, "Hi there!")
        _ <- f(report, "Goodbye now.")
        messages <- report.jobManager.listMessages(report.jobId, channel, None)
      } yield messages).copoint.toList

      messages map { case Message(_, _, _, jobj) =>
        val JString(msg) = jobj \ "message"
        msg
      } must_== List("Hi there!", "Goodbye now.")
    }
  }

  "Job error report" should {
    "report info messages to the correct channel" in testChannel(channels.Info)(_ info _)
    "report warn messages to the correct channel" in testChannel(channels.Warning)(_ warn _)
    "report fatal messages to the correct channel" in testChannel(channels.Error)(_ fatal _)
    "cancel jobs on a fatal message" in {
      withReport { report =>
        val reason = "Arrrgggggggggggghhhhhhh....."
        (for {
          _ <- report.fatal(reason)
          job <- report.jobManager.findJob(report.jobId)
        } yield job).copoint must beLike {
          case Some(Job(_, _, _, _, _, Cancelled(_, _, _))) => ok
        }
      }
    }
  }
}
