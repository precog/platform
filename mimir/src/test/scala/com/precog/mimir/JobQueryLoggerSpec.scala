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
package com.precog.mimir

import com.precog.common.jobs._

import blueeyes.util.Clock
import blueeyes.json._
import blueeyes.json.serialization._

import org.specs2.mutable.Specification

import scalaz._
import scalaz.syntax.monad._
import scalaz.syntax.comonad._

class JobQueryLoggerSpec extends Specification {
  import JobManager._
  import JobState._

  def withReport[A](f: JobQueryLogger[Need, Unit] => A): A = {
    f(new JobQueryLogger[Need, Unit] with TimingQueryLogger[Need, Unit] {
      val M = Need.need
      val clock = Clock.System
      val jobManager = new InMemoryJobManager[Need]
      val jobId = jobManager.createJob("password", "error-report-spec", "hard", None, Some(clock.now())).copoint.id
      val decomposer = new Decomposer[Unit] {
        def decompose(u: Unit): JValue = JNull
      }
    })
  }

  def testChannel(channel: String)(f: (QueryLogger[Need, Unit], String) => Need[Unit]) = {
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
    "report info messages to the correct channel" in testChannel(channels.Info) { (report, msg) => report.info((), msg) }
    "report warn messages to the correct channel" in testChannel(channels.Warning) { (report, msg) => report.warn((), msg) }
    "report error messages to the correct channel" in testChannel(channels.Error) { (report, msg) => report.error((), msg) }
    "cancel jobs on a die" in {
      withReport { report =>
        val reason = "Arrrgggggggggggghhhhhhh....."
        (for {
          _ <- report.error((), reason)
          _ <- report.die()
          job <- report.jobManager.findJob(report.jobId)
        } yield job).copoint must beLike {
          case Some(Job(_, _, _, _, _, Cancelled(_, _, _))) => ok
        }
      }
    }
  }
}
