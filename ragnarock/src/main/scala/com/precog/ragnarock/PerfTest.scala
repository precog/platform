package com.precog.ragnarock

import scalaz._


sealed trait PerfTest

/** A single query to be timed. */
case class TimeQuery(query: String) extends PerfTest

case class GroupedTest(name: String, test: PerfTest) extends PerfTest

/** Sequenced tests run one after the other. */
case class SequenceTests(tests: List[PerfTest]) extends PerfTest

/** Runs a set of tests in parallel. */
case class ConcurrentTests(tests: List[PerfTest]) extends PerfTest

object PerfTest {
  implicit def monoid = new Monoid[PerfTest] {
    def zero = ConcurrentTests(Nil)

    def append(a: PerfTest, b: => PerfTest) = (a, b) match {
      case (ConcurrentTests(as), ConcurrentTests(bs)) => ConcurrentTests(as ++ bs)
      case (ConcurrentTests(as), b) => ConcurrentTests(as :+ b)
      case (a, ConcurrentTests(bs)) => ConcurrentTests(a :: bs)
      case (a, b) => ConcurrentTests(a :: b :: Nil)
    }
  }
}


