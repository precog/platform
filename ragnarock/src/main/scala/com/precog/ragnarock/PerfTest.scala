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


