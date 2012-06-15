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
