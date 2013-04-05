package com.precog.ingest
package service

import kafka._

import com.precog.common._
import com.precog.common.security._
import com.precog.common.accounts._
import com.precog.common.ingest._
import com.precog.common.util._

import org.specs2.mutable.Specification
import org.specs2.specification._
import org.scalacheck.Gen._

import akka.actor.ActorSystem
import akka.dispatch.Future
import akka.dispatch.ExecutionContext

import org.joda.time._

import org.streum.configrity.Configuration
import org.streum.configrity.io.BlockFormat

import java.nio.ByteBuffer

import scalaz._
import scalaz.syntax.comonad._

import blueeyes.akka_testing._
import blueeyes.bkka.AkkaDefaults

import blueeyes.core.data._
import blueeyes.core.service._
import blueeyes.core.service.test.BlueEyesServiceSpecification
import blueeyes.core.http.HttpResponse
import blueeyes.core.http.HttpStatus
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.http.MimeTypes
import blueeyes.core.http.MimeTypes._

import blueeyes.json._
import blueeyes.json.serialization.DefaultSerialization._

import blueeyes.util.Clock

class EventServiceSpec extends TestEventService with AkkaConversions with com.precog.common.util.ArbitraryJValue {
  implicit def executionContext = defaultFutureDispatch

  import DefaultBijections._

  val testValue: JValue = JObject(List(JField("testing", JNum(123))))

  val JSON = MimeTypes.application/MimeTypes.json
  val CSV = MimeTypes.text/MimeTypes.csv

  def bb(s: String) = ByteBuffer.wrap(s.getBytes("UTF-8"))

  def chunk(strs: String*): ByteChunk =
    Right(strs.map(bb).foldRight(StreamT.empty[Future, ByteBuffer])(_ :: _))

  "Ingest service" should {
    "track event with valid API key" in {
      val result = track[JValue](JSON, Some(testAccount.apiKey), testAccount.rootPath, Some(testAccount.accountId), batch = false)(testValue)

      result.copoint must beLike {
        case (HttpResponse(HttpStatus(OK, _), _, Some(_), _), Ingest(_, _, _, values, _, _) :: Nil) =>
          values must contain(testValue).only
      }
    }

    "track asynchronous event with valid API key" in {
      val result = track(JSON, Some(testAccount.apiKey), testAccount.rootPath, Some(testAccount.accountId), sync = false, batch = true) {
        chunk("""{ "testing": 123 }""" + "\n", """{ "testing": 321 }""")
      }

      result.copoint must beLike {
        case (HttpResponse(HttpStatus(Accepted, _), _, Some(content), _), _) => (content.asInstanceOf[JObject] \? "ingestId") must not beEmpty
      }
    }

    "track synchronous batch event with bad row" in {
      val result = track(JSON, Some(testAccount.apiKey), testAccount.rootPath, Some(testAccount.accountId), sync = true, batch = true) {
        chunk("178234#!!@#$\n", """{ "testing": 321 }""")
      }

      result.copoint must beLike {
        case (HttpResponse(HttpStatus(OK, _), _, Some(msg), _), events) =>
          msg \ "total" mustEqual JNum(3)
          msg \ "ingested" mustEqual JNum(2)
          msg \ "failed" mustEqual JNum(1)
          msg \ "errors" mustEqual JArray(JObject(JField("line", JNum(1)), JField("reason", JString("expected json value got # (line 1, column 7)"))))

          events flatMap (_.data) mustEqual (JNum(178234) :: JParser.parseUnsafe("""{ "testing": 321 }""") :: Nil)
      }
    }

    "track CSV batch ingest with valid API key" in {
      val result = track(CSV, Some(testAccount.apiKey), testAccount.rootPath, Some(testAccount.accountId), sync = true, batch = true) {
        chunk("a,b,c\n1,2,3\n4, ,a", "\n6,7,8")
      }

      result.copoint must beLike {
        case (HttpResponse(HttpStatus(OK, _), _, Some(_), _), events) =>
          // render then parseUnsafe so that we get the same numeric representations
          events flatMap { _.data.map(v => JParser.parseUnsafe(v.renderCompact)) } must_== List(
            JParser.parseUnsafe("""{ "a": 1, "b": 2, "c": "3" }"""),
            JParser.parseUnsafe("""{ "a": 4, "b": null, "c": "a" }"""),
            JParser.parseUnsafe("""{ "a": 6, "b": 7, "c": "8" }"""))
      }
    }

    "handle CSVs with duplicate headers" in {
      val data = """URL,Title,Status,HubScore,Comments,24 Hours,7 Days,30 Days,Total,24 Hours,Total,Published Date,Edited Date,Featured
                   |http://alexk2009.hubpages.com/hub/Big-Birds-that-carry-off-children,Eagles carrying off children and babies,Published,91,21,11,98,2352,10856,0,252,11/05/11,12/19/12,yes
                   |http://alexk2009.hubpages.com/hub/Creating-Spirits,Creating Spirits and magical astral and physical thought forms,Published,88,0,1,15,58,1076,0,0,05/15/09,01/12/13,yes
                   |http://alexk2009.hubpages.com/hub/The-Illusion-of-Money-part-one,The illusion of money,Published,88,6,0,5,32,708,0,0,04/02/10,01/13/13,yes""".stripMargin

      val result = track(CSV, Some(testAccount.apiKey), testAccount.rootPath, Some(testAccount.accountId), sync = true, batch = true) {
        chunk(data)
      }

      result.copoint must beLike {
        case (HttpResponse(HttpStatus(OK, _), _, Some(_), _), events) =>
          events flatMap { _.data.map(v => JParser.parseUnsafe(v.renderCompact)) } must contain(
            JParser.parseUnsafe("""{
              "URL": "http://alexk2009.hubpages.com/hub/Big-Birds-that-carry-off-children",
              "Title": "Eagles carrying off children and babies", "Status": "Published",
              "24 Hours": [ 11, 0 ], "Total": [ 10856, 252 ],
              "HubScore": 91, "Comments": 21, "7 Days": 98, "30 Days": 2352,
              "Published Date": "11/05/11", "Edited Date": "12/19/12", "Featured": "yes"
            }""")
          )
      }
    }

    "reject track request when API key not found" in {
      val result = track(JSON, Some("not gonna find it"), testAccount.rootPath, Some(testAccount.accountId))(testValue)

      result.copoint must beLike {
        case (HttpResponse(HttpStatus(Forbidden, _), _, Some(JString("The specified API key does not exist: not gonna find it")), _), _) => ok
      }
    }

    "reject track request when no API key provided" in {
      val result = track(JSON, None, testAccount.rootPath, Some(testAccount.accountId))(testValue)

      result.copoint must beLike {
        case (HttpResponse(HttpStatus(BadRequest, _), _, _, _), _) => ok
      }
    }

    "reject track request when grant is expired" in {
      val result = track(JSON, Some(expiredAccount.apiKey), testAccount.rootPath, Some(testAccount.accountId))(testValue)

      result.copoint must beLike {
        case (HttpResponse(HttpStatus(Forbidden, _), _, Some(JString(_)), _), _) => ok
      }
    }

    "reject track request when path is not accessible by API key" in {
      val result = track(JSON, Some(testAccount.apiKey), Path("/"), Some(testAccount.accountId))(testValue)
      result.copoint must beLike {
        case (HttpResponse(HttpStatus(Forbidden, _), _, Some(JString(_)), _), _) => ok
      }
    }

    "reject track request for json values that flatten to more than 1024 (default) primitive values" in {
      val result = track(JSON, Some(testAccount.apiKey), testAccount.rootPath, Some(testAccount.accountId), sync = true, batch = false) {
        genObject(1025).sample.get: JValue
      }

      result.copoint must beLike {
        case (HttpResponse(HttpStatus(BadRequest, _), _, Some(JObject(fields)), _), _) =>
          val JArray(errors) = fields("errors")
            errors.exists {
              case JString(msg) => msg.startsWith("Cannot ingest values with more than 1024 primitive fields.")
              case _ => false
            } must_== true
      }
    }

    // not sure if this restriction still makes sense
    "cap errors at 100" in {
      val data = chunk(List.fill(500)("!@#$") mkString "\n")
      val result = track(JSON, Some(testAccount.apiKey), testAccount.rootPath, Some(testAccount.accountId), batch = true, sync = true)(data)
      result.copoint must beLike {
        case (HttpResponse(HttpStatus(OK, _), _, Some(msg), _), _) =>
          msg \ "total" must_== JNum(500)
          msg \ "ingested" must_== JNum(0)
          msg \ "failed" must_== JNum(100)
          msg \ "skipped" must_== JNum(400)
      }
    }.pendingUntilFixed
  }
}
