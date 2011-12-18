package com.querio.ingest.api

import java.nio.ByteBuffer

import org.specs2.ScalaCheck
import org.specs2.mutable._

import org.scalacheck._
import org.scalacheck.Gen._

import com.querio.ingest.util.ArbitraryIngestMessage

object EventMessageSerializationSpec extends Specification with ScalaCheck with ArbitraryIngestMessage {
  
  "Event message serialization " should {

    implicit val arbRandomIngestMessage = Arbitrary(genRandomIngestMessage)

    "maintain event content" in { check { (in: IngestMessage) => 
      val buf = ByteBuffer.allocate(1024 * 1024)
      val ser = IngestMessageSerialization

      ser.write(buf, in)

      buf.flip

      val out = ser.readMessage(buf)

      out.toOption must beSome like {
        case Some(o) => o must_== in
      }
    }}
  }
}
