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
import akka.dispatch.Await

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

      Await.result(result, 5.seconds) must beLike {
        case (HttpResponse(HttpStatus(OK, _), _, Some(_), _), Ingest(_, _, _, values, _) :: Nil) => values must contain(testValue).only
      }
    }

    "track asynchronous event with valid API key" in {
      val result = track(JSON, Some(testAccount.apiKey), testAccount.rootPath, Some(testAccount.accountId), sync = false, batch = true) {
        chunk("""{ "testing": 123 }""" + "\n", """{ "testing": 321 }""")
      } 
      
      Await.result(result, 5.seconds) must beLike {
        case (HttpResponse(HttpStatus(Accepted, _), _, Some(content), _), _) => content.validated[Long]("content-length") must_== Success(37L)
      } 
    }

    "track synchronous batch event with bad row" in {
      val msg = JParser.parseUnsafe("""{
          "total": 2,
          "ingested": 1,
          "failed": 1,
          "skipped": 0,
          "errors": [ {
            "line": 0,
            "reason": "expected whitespace got # (line 1, column 7)"
          } ]
        }""")

      val result = track(JSON, Some(testAccount.apiKey), testAccount.rootPath, Some(testAccount.accountId), sync = true, batch = true) {
        chunk("178234#!!@#$\n", """{ "testing": 321 }""")
      } 

      Await.result(result, 5.seconds) must beLike {
        case (HttpResponse(HttpStatus(OK, _), _, Some(msg2), _), events) =>
          msg mustEqual msg2
          events flatMap (_.data) mustEqual JParser.parseUnsafe("""{ "testing": 321 }""") :: Nil
      }
    }
    
    "track CSV batch ingest with valid API key" in {
      val result = track(CSV, Some(testAccount.apiKey), testAccount.rootPath, Some(testAccount.accountId), sync = true, batch = true) {
        chunk("a,b,c\n1,2,3\n4, ,a", "\n6,7,8")
      } 

      Await.result(result, 5.seconds) must beLike {
        case (HttpResponse(HttpStatus(OK, _), _, Some(_), _), events) =>
          // render then parseUnsafe so that we get the same numeric representations
          events flatMap { _.data.map(v => JParser.parseUnsafe(v.renderCompact)) } must_== List(
            JParser.parseUnsafe("""{ "a": 1, "b": 2, "c": "3" }"""),
            JParser.parseUnsafe("""{ "a": 4, "b": null, "c": "a" }"""),
            JParser.parseUnsafe("""{ "a": 6, "b": 7, "c": "8" }"""))
      }
    }
    
    "reject track request when API key not found" in {
      val result = track(JSON, Some("not gonna find it"), testAccount.rootPath, Some(testAccount.accountId))(testValue) 

      Await.result(result, 5.seconds) must beLike {
        case (HttpResponse(HttpStatus(BadRequest, _), _, Some(JString("The specified API key does not exist: not gonna find it")), _), _) => ok 
      }
    }
    
    "reject track request when no API key provided" in {
      val result = track(JSON, None, testAccount.rootPath, Some(testAccount.accountId))(testValue) 

      Await.result(result, 5.seconds) must beLike {
        case (HttpResponse(HttpStatus(BadRequest, _), _, _, _), _) => ok 
      }
    }
    
    "reject track request when grant is expired" in {
      val result = track(JSON, Some(expiredAccount.apiKey), testAccount.rootPath, Some(testAccount.accountId))(testValue) 

      Await.result(result, 5.seconds) must beLike {
        case (HttpResponse(HttpStatus(Unauthorized, _), _, Some(JString("Your API key does not have permissions to write at this location.")), _), _) => ok 
      }
    }
    
    "reject track request when path is not accessible by API key" in {
      val result = track(JSON, Some(testAccount.apiKey), Path("/"), Some(testAccount.accountId))(testValue) 
      Await.result(result, 5.seconds) must beLike {
        case (HttpResponse(HttpStatus(Unauthorized, _), _, Some(JString("Your API key does not have permissions to write at this location.")), _), _) => ok 
      }
    }

    "reject track request for json values that flatten to more than 250 primitive values" in {
      val result = track(JSON, Some(testAccount.apiKey), testAccount.rootPath, Some(testAccount.accountId), sync = true, batch = false) { 
        genObject(251).sample.get: JValue 
      }

      Await.result(result, 5.seconds) must beLike {
        case (HttpResponse(HttpStatus(BadRequest, _), _, Some(JString(msg)), _), _) =>
          msg must startWith("Cannot ingest values with more than 250 primitive fields.")
      }
    }
    
    // not sure if this restriction still makes sense
    "cap errors at 100" in {
      val data = chunk(List.fill(500)("!@#$") mkString "\n")
      val result = track(JSON, Some(testAccount.apiKey), testAccount.rootPath, Some(testAccount.accountId), batch = true, sync = true)(data) 
      Await.result(result, 5.seconds) must beLike {
        case (HttpResponse(HttpStatus(OK, _), _, Some(msg), _), _) =>
          msg \ "total" must_== JNum(500)
          msg \ "ingested" must_== JNum(0)
          msg \ "failed" must_== JNum(100)
          msg \ "skipped" must_== JNum(400)
      }
    }.pendingUntilFixed
  }
}
