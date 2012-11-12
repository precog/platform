package com.precog.ingest
package service

import kafka._

import com.precog.daze._
import com.precog.common.{ Path, Event }
import com.precog.common.security._

import org.specs2.mutable.Specification
import org.specs2.specification._
import org.scalacheck.Gen._

import akka.actor.ActorSystem
import akka.dispatch.Future
import akka.dispatch.ExecutionContext
import akka.util.Duration

import org.joda.time._

import org.streum.configrity.Configuration
import org.streum.configrity.io.BlockFormat

import scalaz.{Success, NonEmptyList}
import scalaz.Scalaz._

import blueeyes.concurrent.test._

import blueeyes.core.data._
import blueeyes.bkka.AkkaDefaults
import blueeyes.core.service.test.BlueEyesServiceSpecification
import blueeyes.core.http.HttpResponse
import blueeyes.core.http.HttpStatus
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.http.MimeTypes
import blueeyes.core.http.MimeTypes._

import blueeyes.json._

import blueeyes.util.Clock


class IngestServiceSpec extends TestIngestService with FutureMatchers {

  import BijectionsChunkJson._
  import BijectionsChunkString._
  import BijectionsByteArray._
  import BijectionsChunkByteArray._
  import BijectionsChunkFutureJson._

  val testValue: JValue = JObject(List(JField("testing", JNum(123))))

  val JSON = MimeTypes.application/MimeTypes.json
  val CSV = MimeTypes.text/MimeTypes.csv

  "Ingest service" should {
    "track event with valid API key" in {
      track(JSON)(testValue) must whenDelivered { beLike {
        case (HttpResponse(HttpStatus(OK, _), _, Some(_), _),
          Event(_, _, _, `testValue`, _) :: Nil) => ok
      } }
    }
    "track asynchronous event with valid API key" in {
      track(JSON, sync = false) {
        Chunk("""{ "testing": 123 }\n""".getBytes("UTF-8"),
          Some(Future { Chunk("""{ "testing": 321 }""".getBytes("UTF-8"), None) }))
      } must whenDelivered { beLike {
        case (HttpResponse(HttpStatus(Accepted, _), _, None, _), _) => ok
      } }
    }
    "track synchronous event with bad row" in {
      val msg = JParser.parse("""{
          "total": 2,
          "ingested": 1,
          "failed": 1,
          "skipped": 0,
          "errors": [ {
            "line": 0,
            "reason": "Parsing failed: expected whitespace got # (line 1, column 7)"
          } ]
        }""")

      track(JSON, sync = true) {
        Chunk("178234#!!@#$\n".getBytes("UTF-8"),
          Some(Future { Chunk("""{ "testing": 321 }""".getBytes("UTF-8"), None) }))
      } must whenDelivered {
        beLike {
          case (HttpResponse(HttpStatus(OK, _), _, Some(msg2), _), event) =>
            msg mustEqual msg2
            event map (_.data) mustEqual JParser.parse("""{ "testing": 321 }""") :: Nil
        }
      }
    }
    "track CSV batch ingest with valid API key" in {
      track(CSV, sync = true) {
        Chunk("a,b,c\n1,2,3\n4, ,a".getBytes("UTF-8"),
          Some(Future { Chunk("\n6,7,8".getBytes("UTF-8"), None) }))
      } must whenDelivered { beLike {
        case (HttpResponse(HttpStatus(OK, _), _, Some(_), _), event) =>
          event map (_.data) must_== List(
            JParser.parse("""{ "a": 1, "b": 2, "c": "3" }"""),
            JParser.parse("""{ "a": 4, "b": null, "c": "a" }"""),
            JParser.parse("""{ "a": 6, "b": 7, "c": "8" }"""))
      } }
    }
    "reject track request when API key not found" in {
      track(JSON, apiKey = Some("not gonna find it"))(testValue) must whenDelivered { beLike {
        case (HttpResponse(HttpStatus(BadRequest, _), _, Some(JString("The specified API key does not exist")), _), _) => ok 
      } }
    }
    "reject track request when no API key provided" in {
      track(JSON, apiKey = None)(testValue) must whenDelivered { beLike {
        case (HttpResponse(HttpStatus(BadRequest, _), _, _, _), _) => ok 
      }}
    }
    "reject track request when grant is expired" in {
      track(JSON, apiKey = Some(expiredAPIKey))(testValue) must whenDelivered { beLike {
        case (HttpResponse(HttpStatus(Unauthorized, _), _, Some(JString("Your API key does not have permissions to write at this location.")), _), _) => ok 
      }}
    }
    "reject track request when path is not accessible by API key" in {
      track(JSON, path = "")(testValue) must whenDelivered { beLike {
        case (HttpResponse(HttpStatus(Unauthorized, _), _, Some(JString("Your API key does not have permissions to write at this location.")), _), _) => ok 
      }}
    }
    "cap errors at 100" in {
      val data = Chunk((List.fill(500)("!@#$") mkString "\n").getBytes("UTF-8"), None)
      track(JSON)(data) must whenDelivered { beLike {
        case (HttpResponse(HttpStatus(OK, _), _, Some(msg), _), _) =>
          msg \ "total" must_== JNum(500)
          msg \ "ingested" must_== JNum(0)
          msg \ "failed" must_== JNum(100)
          msg \ "skipped" must_== JNum(400)
      }}
    }
  }
}
