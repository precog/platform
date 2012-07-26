package com.precog.ragnarock

import org.specs2.mutable.Specification

import scalaz._


class PerfTestRunnerSpec extends Specification {
  "sequential tests" should {
    "run in order, with disjoint timespans" in {
      import Id._

      val t = SequenceTests(TimeQuery(".") :: TimeQuery(".") :: Nil)
      val r = new MockPerfTestRunner[Id](50)
      val s = r.TimeSpanSemigroup

      r.run(t) must beLike { case GroupedResult(_, results) =>
        results must have size(2)
        (results sliding 2 map (_ map (_.timeOption(s))) forall {
          case Some((_, t1)) :: Some((t2, _)) :: Nil => t1 <= t2
          case _ => false
        }) must_== true
      }
    }
  }
}

