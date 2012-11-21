package com.precog.heimdall

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

import scalaz._

trait TestJobService extends BlueEyesServiceSpecification with JobService with AkkaDefaults {
  val executionContext = defaultFutureDispatch

  override implicit val defaultFutureTimeouts: FutureTimeouts = FutureTimeouts(20, Duration(1, "second"))

  val shortFutureTimeouts = FutureTimeouts(5, Duration(50, "millis"))

  implicit val M = AkkaTypeClasses.futureApplicative(executionContext)

  val clock = blueeyes.util.Clock.System

  type Resource = Unit

  def jobManager(config: Configuration): (Unit, JobManager[Future]) = ((), new InMemoryJobManager[Future])

  def close(u: Unit): Future[Unit] = Future { u }
}

class JobServiceSpec extends TestJobService {

  import DefaultBijections._
  import blueeyes.json.serialization.DefaultSerialization._

  val validAPIKey = "xxx"

  val JSON = MimeTypes.application / MimeTypes.json

  "job service" should {
    "allow job creation" in {
      val body: JValue = JObject(List(
        JField("name", JString("abc")),
        JField("type", JString("abc"))
      ))
      client.contentType[ByteChunk](JSON)
             .query("apiKey", validAPIKey)
             .post[JValue]("/jobs")(body) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(Created, _), _, _, _) => ok
      } }
    }
  }
}

