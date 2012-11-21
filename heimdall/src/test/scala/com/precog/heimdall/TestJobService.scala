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

