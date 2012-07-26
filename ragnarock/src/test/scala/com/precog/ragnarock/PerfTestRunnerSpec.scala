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
import scalaz.std.option._


class PerfTestRunnerSpec extends Specification {
  "sequential tests" should {
    "run in order, with disjoint timespans" in {
      import Id._

      val t = Tree.node(RunSequential, Stream(
        Tree.leaf[PerfTest](RunQuery(".")),
        Tree.leaf[PerfTest](RunQuery("."))))
      val r = new MockPerfTestRunner[Id](50)

      implicit val s = r.TimeSpanSemigroup

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

