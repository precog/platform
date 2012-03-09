package com.precog.ingest.kafka

import org.specs2.mutable._

class SystemEventIdSequenceSpec extends Specification {
  "the system event id sequence" should {
    "reliably obtain a starting state" in todo
    "not allow one to obtain a sequence with an invalid starting state" in todo
    "persist and restore state correctly" in todo
  }
}

// vim: set ts=4 sw=4 et:
