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
import com.precog.common.security._
import com.precog.common.JValueByteChunkTranscoders._

import akka.util.Duration
import akka.dispatch._

import blueeyes.bkka._
import blueeyes.core.service.test.BlueEyesServiceSpecification
import blueeyes.core.data._
import blueeyes.core.service._
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.http.MimeTypes._
import blueeyes.json._

import org.streum.configrity.Configuration

import org.joda.time.DateTime

import scalaz._

trait TestJobService extends BlueEyesServiceSpecification with JobService with AkkaDefaults {
  type JobResource = Unit

  override implicit val defaultFutureTimeouts: FutureTimeouts = FutureTimeouts(20, Duration(5, "seconds"))
  val validAPIKey = "secret"

  lazy val executionContext = defaultFutureDispatch
  implicit lazy val M: Monad[Future] with Comonad[Future] = new UnsafeFutureComonad(executionContext, Duration(10, "seconds"))

  lazy val shortFutureTimeouts = FutureTimeouts(5, Duration(50, "millis"))

  lazy val clock = blueeyes.util.Clock.System
  lazy val jobs = new InMemoryJobManager[Future]

  def jobManager(config: Configuration): (Unit, JobManager[Future]) = ((), jobs)


  def authService(config: Configuration): AuthService[Future] = TestAuthService[Future](Set(validAPIKey))

  def close(u: Unit): Future[Unit] = Future { u }
}

class JobServiceSpec extends TestJobService {
  import scalaz.syntax.comonad._
  import DefaultBijections._
  import blueeyes.json.serialization.DefaultSerialization.{ DateTimeDecomposer => _, DateTimeExtractor => _, _ }
  import JobState._

  val JSON = MimeTypes.application / MimeTypes.json

  val jobsClient = client.contentType[ByteChunk](JSON).path("/jobs/v1/")

  val simpleJob: JValue = JObject(List(
    JField("name", JString("abc")),
    JField("type", JString("cba"))
  ))

  val jobWithData: JValue = JObject(List(
    JField("name", JString("xyz")),
    JField("type", JString("zyx")),
    JField("data", JObject(JField("x", JNum(1))))
  ))

  def startJob(ts: Option[DateTime] = None): JValue = JObject(
    JField("state", JString("started")) ::
    (ts map { dt => JField("timestamp", dt.serialize) :: Nil } getOrElse Nil)
  )

  def postJob(job: JValue, apiKey: String = validAPIKey) = jobsClient.query("apiKey", apiKey).post[JValue]("/jobs/")(job)

  def postJobAndGetId(job: JValue, apiKey: String = validAPIKey) = for {
    res <- jobsClient.query("apiKey", apiKey).post[JValue]("/jobs/")(job)
    Some(JString(jobId)) = res.content map (_ \ "id")
  } yield jobId

  def getJob(jobId: String) = jobsClient.get[JValue]("/jobs/%s".format(jobId))

  def putState(jobId: String, state: JValue) = jobsClient.put[JValue]("/jobs/%s/state".format(jobId))(state)

  def getState(jobId: String) = jobsClient.get[JValue]("/jobs/%s/state".format(jobId))

  def postMessage(jobId: String, channel: String, msg: JValue) =
    jobsClient.post[JValue]("/jobs/%s/messages/%s" format (jobId, channel))(msg)

  def postMessageAndGetId(jobId: String, channel: String, msg: JValue) = for {
    res <- postMessage(jobId, channel, msg)
    Some(JNum(id)) = res.content map (_ \ "id")
  } yield id

  def getMessages(jobId: String, channel: String, after: Option[BigDecimal]) = {
    val client0 = after map { id => jobsClient.query("after", id.toInt.toString) } getOrElse jobsClient
    client0.contentType[ByteChunk](JSON).get[JValue]("/jobs/%s/messages/%s" format (jobId, channel))
  }

  def putStatusRaw(jobId: String, prev: Option[BigDecimal])(obj: JValue) = {
    val client0 = prev map { id => jobsClient.query("prevStatusId", id.toLong.toString) } getOrElse jobsClient
    client0.contentType[ByteChunk](JSON).put[JValue]("/jobs/%s/status".format(jobId))(obj)
  }

  def putStatus(jobId: String, message: String, progress: BigDecimal, unit: String, info: Option[JValue] = None, prev: Option[BigDecimal] = None) = {
    putStatusRaw(jobId, prev)(JObject(
      JField("message", JString(message)) ::
      JField("progress", JNum(progress)) ::
      JField("unit", JString(unit)) ::
      (info map (JField("info", _) :: Nil) getOrElse Nil)
    ))
  }

  def putStatusAndGetId(jobId: String, message: String, progress: BigDecimal, unit: String, info: Option[JValue] = None, prev: Option[BigDecimal] = None) = {
    for {
      res <- putStatus(jobId, message, progress, unit, info, prev)
      Some(JNum(id)) = res.content map (_ \ "id")
    } yield id
  }

  def getStatus(jobId: String) = jobsClient.get[JValue]("/jobs/%s/status".format(jobId))

  "job service" should {
    "allow job creation" in {
      postJob(simpleJob, validAPIKey).copoint must beLike {
        case HttpResponse(HttpStatus(Created, _), _, _, _) => ok
      }
    }

    "return not found for jobs that don't exist" in {
      getJob("super awesome job").copoint must beLike {
        case HttpResponse(HttpStatus(NotFound, _), _, _, _) => ok
      }
    }

    "forbid job creation if provided an invalid API key" in {
      postJob(simpleJob, "Completely wrong API key").copoint must beLike {
        case HttpResponse(HttpStatus(Forbidden, _), _, _, _) => ok
      }
    }

    "start job in unstarted state" in {
      postJob(simpleJob, validAPIKey).copoint must beLike {
        case HttpResponse(HttpStatus(Created, _), _, Some(obj), _) =>
          (obj \ "state").validated[JobState] must_== Success(JobState.NotStarted)
      }
    }

    "fetch created jobs" in {
      val obj = (for {
        res <- postJob(jobWithData, validAPIKey)
        Some(JString(jobId)) = res.content map (_ \ "id")
        HttpResponse(HttpStatus(OK, _), _, Some(obj), _) <- getJob(jobId)
      } yield obj).copoint

      
      val data = JObject(JField("x", JNum(1)) :: Nil)

      (obj \ "name", obj \ "type", obj \ "data") must beLike {
        case (JString("xyz"), JString("zyx"), `data`) => ok
      }
    }

    "start job and retrieve state" in {
      val (st1, st2, st3) = (for {
        res <- postJob(simpleJob, validAPIKey)
        Some(JString(jobId)) = res.content map (_ \ "id")
        HttpResponse(_, _, Some(st1), _) <- getState(jobId)
        HttpResponse(_, _, Some(st2), _) <- putState(jobId, startJob())
        HttpResponse(_, _, Some(st3), _) <- getState(jobId)
      } yield (st1, st2, st3)).copoint

      st1.validated[JobState] must_== Success(NotStarted)
      st2.validated[JobState] must beLike { case Success(Started(_, NotStarted)) => ok }
      st3.validated[JobState] must beLike { case Success(Started(_, NotStarted)) => ok }
    }

    "start jobs with explicit date time" in {
      val dt = new DateTime
      val (state, job) = (for {
        res <- postJob(simpleJob, validAPIKey)
        Some(JString(jobId)) = res.content map (_ \ "id")
        HttpResponse(HttpStatus(OK, _), _, Some(obj1), _) <- putState(jobId, startJob(Some(dt)))
        HttpResponse(_, _, Some(obj2), _) <- getJob(jobId)
      } yield (obj1, obj2)).copoint

      (state \ "state") must_== JString("started")
      (state \ "timestamp").validated[DateTime] must_== Success(dt)
      (job \ "state" \ "state") must_== JString("started")
      (job \ "state" \ "timestamp").validated[DateTime] must_== Success(dt)
    }

    "fail to cancel job without reason" in {
      (for {
        job <- postJob(simpleJob)
        Some(JString(jobId)) = job.content map (_ \ "id")
        _ <- putState(jobId, startJob())
        res <- putState(jobId, JObject(JField("state", JString("cancelled")) :: Nil))
      } yield res).copoint must beLike {
        case HttpResponse(HttpStatus(BadRequest, _), _, _, _) => ok
      }
    }

    val cancellation: JValue = JObject(
      JField("state", JString("cancelled")) ::
      JField("reason", JString("Because I said so.")) ::
      Nil)

    val abort: JValue = JObject(
      JField("state", JString("aborted")) ::
      JField("reason", JString("Yabba dabba doo!")) ::
      Nil)

    "cancel started job with reason" in {
      val st = (for {
        jobId <- postJobAndGetId(simpleJob)
        _ <- putState(jobId, startJob())
        HttpResponse(HttpStatus(OK, _), _, Some(st), _) <- putState(jobId, cancellation)
      } yield st).copoint
      
      st.validated[JobState] must beLike {
        case Success(Cancelled("Because I said so.", _, Started(_, NotStarted))) => ok
      }
    }

    "not allow jobs to be cancelled twice" in {
      (for {
        jobId <- postJobAndGetId(simpleJob)
        _ <- putState(jobId, startJob())
        _ <- putState(jobId, cancellation)
        res <- putState(jobId, cancellation)
      } yield res).copoint must beLike {
        case HttpResponse(HttpStatus(BadRequest, _), _, _, _) => ok
      }
    }

    "not allow jobs in a terminal state to be cancelled" in {
      (for {
        jobId <- postJobAndGetId(simpleJob)
        _ <- putState(jobId, startJob())
        _ <- putState(jobId, abort)
        res <- putState(jobId, cancellation)
      } yield res).copoint must beLike {
        case HttpResponse(HttpStatus(BadRequest, _), _, _, _) => ok
      }
    }

    "allow jobs that haven't been started to be aborted" in {
      (for {
        jobId <- postJobAndGetId(simpleJob)
        res <- putState(jobId, abort)
      } yield res).copoint must beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(obj), _) =>
          obj.validated[JobState] must beLike {
            case Success(Aborted("Yabba dabba doo!", _, NotStarted)) => ok
          }
      }
    }

    "allow started jobs to be aborted" in {
      (for {
        jobId <- postJobAndGetId(simpleJob)
        _ <- putState(jobId, startJob())
        res <- putState(jobId, abort)
      } yield res).copoint must beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(obj), _) =>
          obj.validated[JobState] must beLike {
            case Success(Aborted("Yabba dabba doo!", _, Started(_, NotStarted))) => ok
          }
      }
    }

    "allow cancelled jobs to be aborted" in {
      (for {
        jobId <- postJobAndGetId(simpleJob)
        _ <- putState(jobId, startJob())
        _ <- putState(jobId, cancellation)
        res <- putState(jobId, abort)
      } yield res).copoint must beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(obj), _) =>
          obj.validated[JobState] must beLike {
            case Success(Aborted("Yabba dabba doo!", _, Cancelled(_, _, Started(_, NotStarted)))) => ok
          }
      }
    }

    "not allow job to be aborted twice" in {
      (for {
        jobId <- postJobAndGetId(simpleJob)
        _ <- putState(jobId, startJob())
        _ <- putState(jobId, abort)
        res <- putState(jobId, abort)
      } yield res).copoint must beLike {
        case HttpResponse(HttpStatus(BadRequest, _), _, _, _) => ok
      }
    }

    def say(name: String, msg: String): JValue = JObject(JField("name", JString(name)) :: JField("msg", JString(msg)) :: Nil)

    "post a simple message to a job" in {
      (for {
        jobId <- postJobAndGetId(simpleJob)
        msg <- postMessage(jobId, "abc", say("Tom", "Hello!"))
      } yield (jobId, msg)).copoint must beLike {
        case (jobId, HttpResponse(HttpStatus(Created, _), _, Some(msg), _)) =>
          msg \ "channel" must_== JString("abc")
          msg \ "value" must_== say("Tom", "Hello!")
          msg \ "value" \ "name" must_== JString("Tom")
          msg \ "value" \ "msg" must_== JString("Hello!")
          msg \ "jobId" must_== JString(jobId)
      }
    }

    "post a series of messages and retrieve them" in {
      val m1 = say("Joan", "Hello!")
      val m2 = say("Bob", "Hi there!")
      val m3 = say("Joan", "Let's say 2 more things.")
      val m4 = say("Bob", "Righty-O!")
      val m5 = say("Joan", "OK")

      val (msgs, msgsAfterM3, msgsAfterM5) = (for {
        jobId <- postJobAndGetId(simpleJob)
        _ <- postMessageAndGetId(jobId, "abc", m1)
        _ <- postMessageAndGetId(jobId, "abc", m2)
        m3id <- postMessageAndGetId(jobId, "abc", m3)
        _ <- postMessageAndGetId(jobId, "abc", m4)
        m5id <- postMessageAndGetId(jobId, "abc", m5)
        msgs <- getMessages(jobId, "abc", None)
        msgsAfterM3 <- getMessages(jobId, "abc", Some(m3id))
        msgsAfterM5 <- getMessages(jobId, "abc", Some(m5id))
      } yield (msgs, msgsAfterM3, msgsAfterM5)).copoint
    
      msgs must beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(JArray(msgs)), _) =>
          msgs.map(_ \ "value").toList must_== List(m1, m2, m3, m4, m5)
      }

      msgsAfterM3 must beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(JArray(msgs)), _) =>
          msgs.map(_ \ "value").toList must_== List(m4, m5)
      }

      msgsAfterM5 must beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(JArray(msgs)), _) =>
          msgs.toList must_== Nil
      }
    }

    "post messages to different channels" in {
      val m1 = say("Anne", "I'm in abc!")
      val m2 = say("Bob", "Me too, Anne.")
      val m3 = say("Xavier", "I'm in xyz!")
      val m4 = say("Yosef", "Me too, Xavier.")

      val (abcMsgs, xyzMsgs, lastAbc) = (for {
        jobId <- postJobAndGetId(simpleJob)
        m1Id <- postMessageAndGetId(jobId, "abc", m1)
        _ <- postMessageAndGetId(jobId, "xyz", m3)
        _ <- postMessageAndGetId(jobId, "abc", m2)
        _ <- postMessageAndGetId(jobId, "xyz", m4)
        abcMsgs <- getMessages(jobId, "abc", None)
        xyzMsgs <- getMessages(jobId, "xyz", None)
        lastAbc <- getMessages(jobId, "abc", Some(m1Id))
      } yield (abcMsgs, xyzMsgs, lastAbc)).copoint

      abcMsgs must beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(JArray(msgs)), _) =>
          msgs.map(_ \ "value").toList must_== List(m1, m2)
      }

      xyzMsgs must beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(JArray(msgs)), _) =>
          msgs.map(_ \ "value").toList must_== List(m3, m4)
      }

      lastAbc must beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(JArray(msgs)), _) =>
          msgs.map(_ \ "value").toList must_== List(m2)
      }
    }

    "put simple status update and get it" in {
      (for {
        jobId <- postJobAndGetId(simpleJob)
        status1 <- putStatus(jobId, "Nearly there!", 99.999, "%")
        status2 <- getStatus(jobId)
      } yield (jobId, status1, status2)).copoint must beLike {
        case (jobId, status1 @ HttpResponse(HttpStatus(OK, _), _, Some(obj), _), status2) =>
          obj \ "jobId" must_== JString(jobId)
          obj \ "value" \ "message" must_== JString("Nearly there!")
          obj \ "value" \ "progress" must_== JNum(99.999)
          obj \ "value" \ "unit" must_== JString("%")
          status1 must_== status2
      }
    }

    "return 404 if no status was put" in {
      (for {
        jobId <- postJobAndGetId(simpleJob)
        res <- getStatus(jobId)
      } yield res).copoint must beLike {
        case HttpResponse(HttpStatus(NotFound, _), _, _, _) => ok
      }
    }

    "allow status updates to be conditional" in {
      (for {
        jobId <- postJobAndGetId(simpleJob)
        id1 <- putStatusAndGetId(jobId, "Nearly there!", 99.999, "%", None, None)
        id2 <- putStatusAndGetId(jobId, "Very nearly there!", 99.99999, "%", None, Some(id1))
        res <- putStatus(jobId, "Very nearly, almost there!", 99.9999999, "%", None, Some(id2))
      } yield res).copoint must beLike {
        case HttpResponse(HttpStatus(OK, _), _, Some(obj), _) =>
          obj \ "value" \ "message" must_== JString("Very nearly, almost there!")
      }
    }

    "notify of conflicting status updates when conditional" in {
      (for {
        jobId <- postJobAndGetId(simpleJob)
        id <- putStatusAndGetId(jobId, "Nearly there!", 99.999, "%", None, None)
        _ <- putStatusAndGetId(jobId, "Very nearly there!", 99.99999, "%", None, Some(id))
        res <- putStatus(jobId, "Very nearly there!", 99.99999, "%", None, Some(id))
      } yield res).copoint must beLike {
        case HttpResponse(HttpStatus(Conflict, _), _, _, _) => ok
      }
    }

    "require status updates to contain 'message, 'progress', and 'unit" in {
      val (res1, res2, res3, res4) = (for {
        jobId <- postJobAndGetId(simpleJob)
        res1 <- putStatusRaw(jobId, None)(JObject(Nil))
        res2 <- putStatusRaw(jobId, None)(JObject(JField("message", JString("a")) :: JField("unit", JString("%")) :: Nil))
        res3 <- putStatusRaw(jobId, None)(JObject(JField("message", JString("a")) :: JField("progress", JNum(99)) :: Nil))
        res4 <- putStatusRaw(jobId, None)(JObject(JField("progress", JNum(99)) :: JField("unit", JString("%")) :: Nil))
      } yield (res1, res2, res3, res4)).copoint

      def mustBeBad(res: HttpResponse[JValue]) = res must beLike {
        case HttpResponse(HttpStatus(BadRequest, _), _, _, _) => ok
      }

      mustBeBad(res1)
      mustBeBad(res2)
      mustBeBad(res3)
      mustBeBad(res4)
    }
  }
}

