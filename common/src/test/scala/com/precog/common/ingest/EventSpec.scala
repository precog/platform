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
package com.precog.common
package ingest

import util._

import org.specs2.mutable._
import org.specs2.ScalaCheck

import org.scalacheck.{Arbitrary, Gen} 

import blueeyes.json._
import blueeyes.json.serialization.{ Extractor, Decomposer }
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.json.serialization.Extractor._

import scalaz._

class EventSpec extends Specification with ArbitraryEventMessage with ScalaCheck {
  implicit val arbEvent = Arbitrary(genRandomIngest)
  "serialization of an event" should {
    "read back the data that was written" in check { in: Ingest =>
      in.serialize.validated[Ingest] must beLike {
        case Success(out) => out must_== in
      }
    }
  }

  "Event serialization" should {
    "Handle V0 format" in {
      (JObject("tokenId" -> JString("1234"),
               "path"    -> JString("/test/"),
               "data"    -> JObject("test" -> JNum(1)))).validated[Ingest] must beLike {
        case Success(_) => ok
      }
    }

    "Handle V1 format" in {
      (JObject("apiKey" -> JString("1234"),
               "path"    -> JString("/test/"),
               "data"    -> JObject("test" -> JNum(1)),
               "metadata" -> JArray())).validated[Ingest] must beLike {
        case Success(_) => ok
        case Failure(Thrown(ex)) =>
          throw ex
      }
    }
  }

  "Archive serialization" should {
    "Handle V0 format" in {
      JObject("tokenId" -> JString("1234"),
              "path"    -> JString("/test/")).validated[Archive] must beLike {
        case Success(_) => ok
      }
    }

    "Handle V1 format" in {
      JObject("apiKey" -> JString("1234"),
              "path"   -> JString("/test/")).validated[Archive] must beLike {
        case Success(_) => ok
      }
    }
  }
}
