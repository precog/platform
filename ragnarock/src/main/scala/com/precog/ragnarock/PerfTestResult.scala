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
  import scalaz.syntax.functor._
  import scalaz.std.option._

  def lift: PerfTestResult[Option[T]] = {
    import PerfTestResult._
    this map (Some(_))
  }

  def timeOption(implicit T: Semigroup[T]) = this.lift.time

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
  implicit def functor: Functor[PerfTestResult] = new Functor[PerfTestResult] {
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



