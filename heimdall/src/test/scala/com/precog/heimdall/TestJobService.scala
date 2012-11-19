package com.precog.heimdall

import akka.util.Duration
import akka.dispatch._

import blueeyes.bkka._
import blueeyes.core.service.test.BlueEyesServiceSpecification
import blueeyes.core.data._
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.http.MimeTypes._
import blueeyes.json._

import org.streum.configrity.Configuration

import scalaz._

trait TestJobService extends BlueEyesServiceSpecification with JobService with AkkaDefaults {
  val asyncContext = defaultFutureDispatch

  override implicit val defaultFutureTimeouts: FutureTimeouts = FutureTimeouts(20, Duration(1, "second"))

  val shortFutureTimeouts = FutureTimeouts(5, Duration(50, "millis"))

  implicit val M = AkkaTypeClasses.futureApplicative(asyncContext)

  val clock = blueeyes.util.Clock.System

  def jobManager(config: Configuration): JobManager[Future] = new InMemoryJobManager[Future]

  def close(): Future[Unit] = Future { () }
}

class JobServiceSpec extends TestJobService {

  import BijectionsChunkJson._
  import BijectionsChunkString._
  import BijectionsByteArray._
  import BijectionsChunkByteArray._
  import BijectionsChunkFutureJson._

  val validAPIKey = "xxx"

  val JSON = MimeTypes.application / MimeTypes.json

  "job service" should {
    "allow job creation" in {
      val body: JValue = JObject(List(
        JField("name", JString("abc")),
        JField("type", JString("abc"))
      ))
      service.contentType[JValue](JSON)
             .query("apiKey", validAPIKey)
             .post("/jobs")(body) must whenDelivered { beLike {
        case HttpResponse(HttpStatus(Created, _), _, _, _) => ok
      } }
    }
  }
}

