package com.precog.yggdrasil.kafka

import org.specs2.mutable._

object ProjectionActorSpec extends Specification {
  "a projection actor" should {
    "not close a projection that has an outstanding reference count" in todo
    "correctly insert a batch of records" in todo
    "not attempt to insert into a closed projection" in todo
    "not return a reference to a closed projection" in todo
    "not allow the reference count to be incremented once a stop has been requested" in todo
    "have configurable stop rescheduling" in todo
  }
}

// vim: set ts=4 sw=4 et:
