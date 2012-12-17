package com.precog.common
package ingest

import java.nio.ByteBuffer

import org.specs2.ScalaCheck
import org.specs2.mutable._

import org.scalacheck._
import org.scalacheck.Gen._

import blueeyes.json._

object EventMessageSerializationSpec extends Specification with ScalaCheck with ArbitraryEventMessage {
  implicit val arbRandomIngestMessage = Arbitrary(genRandomIngestMessage)
  
  "Event message serialization " should {
    "maintain event content" in { check { (in: IngestMessage) => 
      val buf = ByteBuffer.allocate(1024 * 1024)
      val ser = EventMessage

      ser.write(buf, in)

      buf.flip

      ser.readMessage(buf).toOption must beLike {
        case Some(out) => out must_== in
      }
    }}
  }
}
