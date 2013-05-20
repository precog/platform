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
