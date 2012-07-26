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

