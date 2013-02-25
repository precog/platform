package com.precog.common.ingest

import org.scalacheck.Gen

import org.specs2.ScalaCheck
import org.specs2.mutable.Specification

object EventIdSpecs extends Specification with ScalaCheck {
  implicit val idRange = Gen.chooseNum[Int](0, Int.MaxValue)

  "EventId" should {
    "support round-trip encap/decap of producer/sequence ids" in check { (prod: Int, seq: Int) =>
      val uid = EventId(prod, seq).uid

      EventId.producerId(uid) mustEqual prod
      EventId.sequenceId(uid) mustEqual seq
    }
  }
}
