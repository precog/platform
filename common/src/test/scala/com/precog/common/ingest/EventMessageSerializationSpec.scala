package com.precog.common
package ingest

import kafka._
import java.nio.ByteBuffer

import org.specs2.ScalaCheck
import org.specs2.mutable._

import org.scalacheck._
import org.scalacheck.Gen._

import blueeyes.json._
import blueeyes.json.serialization._

import scalaz._

object EventMessageSerializationSpec extends Specification with ScalaCheck with ArbitraryEventMessage {
  implicit val arbMsg = Arbitrary(genRandomEventMessage)
  
  "Event message serialization " should {
    "maintain event content" in { check { (in: EventMessage) => 
      val buf = EventMessageEncoding.toMessageBytes(in)
      EventMessageEncoding.read(buf) must beLike {
        case Success(out) => out must_== in
        case Failure(Extractor.Thrown(ex)) => throw ex
      }
    }}
  }
}
