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

import java.nio.ByteBuffer

import scalaz._
import scalaz.Scalaz._

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

import blueeyes.util.Clock

class EventServiceSpec extends TestEventService with FutureMatchers {

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
      val res = track[JValue](JSON, Some(testAPIKey), testPath, Some(testAccountId))(testValue)

      res must whenDelivered { beLike {
        case (HttpResponse(HttpStatus(OK, _), _, Some(_), _),
          Event(_, _, _, `testValue`, _) :: Nil) => ok
      } }
    }
    "track asynchronous event with valid API key" in {
      track(JSON, Some(testAPIKey), testPath, Some(testAccountId), sync = false) {
        chunk("""{ "testing": 123 }\n""", """{ "testing": 321 }""")
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

      track(JSON, Some(testAPIKey), testPath, Some(testAccountId), sync = true) {
        chunk("178234#!!@#$\n", """{ "testing": 321 }""")
      } must whenDelivered {
        beLike {
          case (HttpResponse(HttpStatus(OK, _), _, Some(msg2), _), event) =>
            msg mustEqual msg2
            event map (_.data) mustEqual JParser.parse("""{ "testing": 321 }""") :: Nil
        }
      }
    }
    "track CSV batch ingest with valid API key" in {
      track(CSV, Some(testAPIKey), testPath, Some(testAccountId), sync = true) {
        chunk("a,b,c\n1,2,3\n4, ,a", "\n6,7,8")
      } must whenDelivered { beLike {
        case (HttpResponse(HttpStatus(OK, _), _, Some(_), _), event) =>
          event map (_.data) must_== List(
            JParser.parse("""{ "a": 1, "b": 2, "c": "3" }"""),
            JParser.parse("""{ "a": 4, "b": null, "c": "a" }"""),
            JParser.parse("""{ "a": 6, "b": 7, "c": "8" }"""))
      } }
    }
    "reject track request when API key not found" in {
      track(JSON, Some("not gonna find it"), testPath, Some(testAccountId))(testValue) must whenDelivered { beLike {
        case (HttpResponse(HttpStatus(BadRequest, _), _, Some(JString("The specified API key does not exist: not gonna find it")), _), _) => ok 
      } }
    }
    "reject track request when no API key provided" in {
      track(JSON, None, testPath, Some(testAccountId))(testValue) must whenDelivered { beLike {
        case (HttpResponse(HttpStatus(BadRequest, _), _, _, _), _) => ok 
      }}
    }
    "reject track request when grant is expired" in {
      track(JSON, Some(expiredAPIKey), testPath, Some(testAccountId))(testValue) must whenDelivered { beLike {
        case (HttpResponse(HttpStatus(Unauthorized, _), _, Some(JString("Your API key does not have permissions to write at this location.")), _), _) => ok 
      }}
    }
    "reject track request when path is not accessible by API key" in {
      track(JSON, Some(testAPIKey), Path("/"), Some(testAccountId))(testValue) must whenDelivered { beLike {
        case (HttpResponse(HttpStatus(Unauthorized, _), _, Some(JString("Your API key does not have permissions to write at this location.")), _), _) => ok 
      }}
    }
    "cap errors at 100" in {
      val data = chunk(List.fill(500)("!@#$") mkString "\n")
      track(JSON, Some(testAPIKey), testPath, Some(testAccountId))(data) must whenDelivered { beLike {
        case (HttpResponse(HttpStatus(OK, _), _, Some(msg), _), _) =>
          msg \ "total" must_== JNum(500)
          msg \ "ingested" must_== JNum(0)
          msg \ "failed" must_== JNum(100)
          msg \ "skipped" must_== JNum(400)
      }}
    }
  }
}
