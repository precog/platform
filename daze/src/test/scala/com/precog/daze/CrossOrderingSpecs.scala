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
package com.precog.daze

import org.specs2.mutable._

object CrossOrderingSpecs extends Specification with CrossOrdering {
  import instructions._
  import dag._
  
  "cross ordering" should {
    "order in the appropriate direction when one side is singleton" >> {
      "left" >> {
        val line = Line(0, "")
        
        val left = dag.LoadLocal(line, None, Root(line, PushString("/foo")), Het)
        val right = Root(line, PushNum("42"))
        
        val input = Join(line, Map2Cross(Eq), left, right)
        val expected = Join(line, Map2CrossLeft(Eq), left, right)
        
        orderCrosses(input) mustEqual expected
      }
      
      "right" >> {
        val line = Line(0, "")
        
        val left = Root(line, PushNum("42"))
        val right = dag.LoadLocal(line, None, Root(line, PushString("/foo")), Het)
        
        val input = Join(line, Map2Cross(Eq), left, right)
        val expected = Join(line, Map2CrossRight(Eq), left, right)
        
        orderCrosses(input) mustEqual expected
      }
    }
    
    "insert index 0 sorts on dynamic matching filter" in {
      val line = Line(0, "")
      val split = dag.Split(line, Root(line, PushNum("42")), Root(line, PushNum("24")))

      val left = split
      val right = Join(line, Map2Cross(Eq), split, Root(line, PushNum("9")))

      val input = Filter(line, None, None, left, right)

      left.provenance must beLike { case Vector(DynamicProvenance(_)) => ok }
      val num = left.provenance.head.asInstanceOf[DynamicProvenance].id

      right.provenance mustEqual Vector(DynamicProvenance(num))
      input.provenance mustEqual Vector(DynamicProvenance(num))

      val expected = Filter(line, None, None,
        Sort(left, Vector(0)),
        Sort(Join(line, Map2CrossLeft(Eq), split, Root(line, PushNum("9"))), Vector(0)))

      orderCrosses(input) mustEqual expected
    }
  }
}
