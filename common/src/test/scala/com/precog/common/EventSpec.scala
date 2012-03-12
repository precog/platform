package com.precog.common

import util._

import org.specs2.mutable._
import org.specs2.ScalaCheck

import org.scalacheck.{Arbitrary, Gen} 

import blueeyes.json.xschema.{ ValidatedExtraction, Extractor, Decomposer }
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.Extractor._

import scalaz._

class EventSerializationSpec extends Specification with ArbitraryIngestMessage with ScalaCheck {
  implicit val arbEvent = Arbitrary(genRandomEvent)
  "serialization of an event" should {
    "read back the data that was written" in check { in: Event =>
      in.serialize.validated[Event] must beLike {
        case Success(out) => in must_== out
      }
    }
  }
}


// vim: set ts=4 sw=4 et:
