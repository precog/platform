package com.precog.ragnarok

import org.specs2.mutable.Specification

import scalaz._
import scalaz.std.option._


class PerfTestRunnerSpec extends Specification {
  "sequential tests" should {
    "run in order, with disjoint timespans" in {
      import Id._

      val t = Tree.node(RunSequential, Stream(
        Tree.leaf[PerfTest](RunQuery(".")),
        Tree.leaf[PerfTest](RunQuery("."))))
      val r = new MockPerfTestRunner[Id](50)

      import r.timer._
      // implicit val s = r.TimeSpanSemigroup

      r.runAll(t, 1)(identity) must beLike {
        case Tree.Node((RunSequential, _), results) =>
          results must have size(2)
          results map { case Tree.Node((_, time), _) => time } sliding 2 forall {
            case Stream(Some((_, t1)), Some((t2, _))) => t1 <= t2
            case _ => false
          } must_== true
      }
    }
  }
}

