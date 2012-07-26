/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
package com.precog.ragnarock

import scalaz._
import blueeyes.util.Clock


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


// TODO: At some point we'll need to join/multiply these results together to
// get statistics. We'll need to map the results from T to some type that
// gathers statistics, then we can sum them to get a final statistics set w/
// outliers, mean, variance, and all that.

// At runtime we'll know the results match up (hopefully), but what to do about
// the case where they don't? Should it just return the intersection? Or perhaps
// use the zero.

// So PerfTestResult needs to be a monoid if a monoid exists for T. It also
// needs to be a functor.


sealed trait PerfTestResult[T] {
  def time(implicit T: Monoid[T]): T
}

case class QueryResult[T](query: String, _time: T) extends PerfTestResult[T] {
  def time(implicit T: Monoid[T]): T = _time
}

case class GroupedResult[T](name: Option[String], children: List[PerfTestResult[T]]) extends PerfTestResult[T] {
  import scalaz.syntax.monoid._

  def time(implicit T: Monoid[T]): T = children.foldLeft(T.zero)(_ |+| _.time)
}


object PerfTestResult {
  implicit def functor = new Functor[PerfTestResult] {
    def map[A, B](fa: PerfTestResult[A])(f: A => B): PerfTestResult[B] = fa match {
      case QueryResult(q, a) =>
        QueryResult(q, f(a))

      case GroupedResult(name, as) =>
        GroupedResult(name, as map (map(_)(f)))
    }
  }

  // Adding together 2 perf test results. Need to match on names... So, really,
  // need to get rid of unnamed results.
}


trait PerfTestRunner[M[+_]] { self: Timer =>
  import scalaz.syntax.monad._
  import scalaz.syntax.copointed._

  implicit def M: Monad[M]

  /** Result type of running an eval. */
  type Result


  /** Evaluate a Quirrel query. */
  def eval(query: String): M[Result]


  def run(test: PerfTest)(implicit M: Copointed[M]) = runM(test).copoint

  // TODO: If M isn't async, then ConcurrentTests won't be either. Good? No?

  def runM(test: PerfTest): M[PerfTestResult[TimeSpan]] = test match {
    case TimeQuery(query) =>
      timeQuery(query) map { case (t, _) => QueryResult(query, t) }

    case GroupedTest(name, test) =>
      runM(test) map {
        case GroupedResult(None, rs) =>
          GroupedResult(Some(name), rs)

        case r @ (GroupedResult(_, _) | QueryResult(_, _)) =>
          GroupedResult(Some(name), r :: Nil)
      }

    case ConcurrentTests(tests) =>
      (tests map (runM(_))).foldLeft(GroupedResult[TimeSpan](None, Nil).pure[M]) { (acc, testRun) =>
        acc flatMap { case GroupedResult(_, rs) =>
          testRun map { r => GroupedResult(None, r :: rs) }
        }
      } map {
        case GroupedResult(_, rs) => GroupedResult(None, rs.reverse)
      }

    case SequenceTests(tests) =>
      tests.foldLeft(GroupedResult[TimeSpan](None, Nil).pure[M]) { (result, test) =>
        result flatMap { case GroupedResult(_, rs) =>
          runM(test) map { r => GroupedResult(None, r :: rs) }
        }
      } map {
        case GroupedResult(_, rs) => GroupedResult(None, rs.reverse)
      }
  }


  private def time[A](f: => A): (TimeSpan, A) = {
    val start = now()
    val result = f
    duration(start, now()) -> result
  }

  private def timeQuery(q: String): M[(TimeSpan, Result)] = {
    val start = now()
    eval(q) map (duration(start, now()) -> _)
  }
}


class MockPerfTestRunner[M[+_]](evalTime: => Int)(implicit val M: Monad[M]) extends PerfTestRunner[M] with SimpleTimer {
  import scalaz.syntax.monad._

  type Result = Unit

  def eval(query: String): M[Result] = {
    Thread.sleep(evalTime)
    (()).pure[M]
  }
}


// 

