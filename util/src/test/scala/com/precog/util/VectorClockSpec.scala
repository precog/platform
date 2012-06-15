package com.precog.util

import org.specs2.mutable._

object VectorClockSpec extends Specification {
  "vector clock" should {
    "update when key not already present" in {
      val vc = VectorClock.empty
      vc.update(1,1) must_== VectorClock(Map((1 -> 1)))
    }
    "update when key is present and new value is greater" in {
      val vc = VectorClock(Map((1 -> 0)))
      vc.update(1,1) must_== VectorClock(Map((1 -> 1)))
    }
    "not update when key is present and new value is lesser" in {
      val vc = VectorClock(Map((1 -> 2)))
      vc.update(1,1) must_== vc 
    }
    "evaluate lower bound for single key" in {
      val vc1 = VectorClock(Map(0 -> 0))
      val vc2 = VectorClock(Map(0 -> 1))
      val vc3 = VectorClock(Map(0 -> 2))

      vc2.isDominatedBy(vc1) must beFalse
      vc2.isDominatedBy(vc2) must beTrue
      vc2.isDominatedBy(vc3) must beTrue
    }
    "evaluate lower bound for multiple single keys" in {
      val vc1 = VectorClock(Map(0 -> 0, 1->1))
      val vc2 = VectorClock(Map(0 -> 1, 1->2))
      val vc3 = VectorClock(Map(0 -> 2, 1->0))

      vc2.isDominatedBy(vc1) must beFalse
      vc2.isDominatedBy(vc2) must beTrue
      vc2.isDominatedBy(vc3) must beFalse
    }
    "evaluate lower bound with missing keys" in {
      val vc1 = VectorClock(Map((0->1),(1->0)))
      val vc2 = VectorClock(Map((1->0),(2->1)))
      val vc3 = VectorClock(Map((1->1),(3->1)))

      vc1.isDominatedBy(vc2) must beTrue
      vc2.isDominatedBy(vc1) must beTrue
      vc3.isDominatedBy(vc1) must beFalse
    }
    "evaluate lower bound with respect to empty" in {
      val vc = VectorClock(Map(0 -> 0, 1->1))

      VectorClock.empty.isDominatedBy(vc) must beTrue
      vc.isDominatedBy(VectorClock.empty) must beTrue
    }
  }
}
