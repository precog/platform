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
// import blueeyes.util.Clock


trait PerfTestRunner[M[+_]] { self: Timer =>
  import scalaz.syntax.monad._
  import scalaz.syntax.copointed._

  implicit def M: Monad[M]

  /** Result type of running an eval. */
  type Result


  /** Evaluate a Quirrel query. */
  def eval(query: String): M[Result]


  def run(test: PerfTest)(implicit M: Copointed[M]) = runM(test).copoint

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
