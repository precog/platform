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
package com.precog
package daze

import bytecode._
import org.specs2.mutable._

object CrossOrderingSpecs extends Specification with CrossOrdering with RandomLibrary {
  import instructions._
  import dag._
  
  "cross ordering" should {
    "order in the appropriate direction when one side is singleton" >> {
      "left" >> {
        val line = Line(0, "")
        
        val left = dag.LoadLocal(line, Root(line, PushString("/foo")))
        val right = Root(line, PushNum("42"))
        
        val input = Join(line, Eq, CrossRightSort, left, right)
        val expected = Join(line, Eq, CrossLeftSort, left, right)
        
        orderCrosses(input) mustEqual expected
      }
      
      "right" >> {
        val line = Line(0, "")
        
        val left = Root(line, PushNum("42"))
        val right = dag.LoadLocal(line, Root(line, PushString("/foo")))
        
        val input = Join(line, Eq, CrossLeftSort, left, right)
        val expected = Join(line, Eq, CrossRightSort, left, right)
        
        orderCrosses(input) mustEqual expected
      }
    }
    
    "refrain from sorting when sets are already aligned in match" in {
      val line = Line(0, "")
      
      val left = dag.LoadLocal(line, Root(line, PushString("/foo")))
      val right = Root(line, PushNum("42"))
      
      val input = Join(line, Or, IdentitySort, Join(line, Eq, CrossRightSort, left, right), left)
      val expected = Join(line, Or, IdentitySort, Join(line, Eq, CrossLeftSort, left, right), left)
      
      orderCrosses(input) mustEqual expected
    }
    
    "refrain from sorting when sets are already aligned in filter" in {
      val line = Line(0, "")
      
      val left = dag.LoadLocal(line, Root(line, PushString("/foo")))
      val right = Root(line, PushNum("42"))
      
      val input = Filter(line, IdentitySort, Join(line, Eq, CrossRightSort, left, right), left)
      val expected = Filter(line, IdentitySort, Join(line, Eq, CrossLeftSort, left, right), left)
      
      orderCrosses(input) mustEqual expected
    }
    
    // this will eventually be a re-order cross test case
    "insert sorts for match on out-of-order operand set" >> {
      "left" >> {
        val line = Line(0, "")
        
        val left = dag.LoadLocal(line, Root(line, PushString("/foo")), JTextT)
        val right = Join(line, Add, CrossRightSort,
          dag.LoadLocal(line, Root(line, PushString("/bar")), JTextT),
          left)
        
        val input = Join(line, Or, IdentitySort, left, right)
        
        val expectedRight = Join(line, Add, CrossLeftSort,
          dag.LoadLocal(line, Root(line, PushString("/bar")), JTextT),
          Memoize(left, 100))

        val expected = Join(line, Or, IdentitySort, left, Sort(expectedRight, Vector(1)))
        
        orderCrosses(input) mustEqual expected
      }
      
      "right" >> {
        val line = Line(0, "")
        
        val right = dag.LoadLocal(line, Root(line, PushString("/foo")), JTextT)
        val left = Join(line, Add, CrossRightSort,
          dag.LoadLocal(line, Root(line, PushString("/bar")), JTextT),
          right)
        
        val input = Join(line, Or, IdentitySort, left, right)
        
        val expectedLeft = Join(line, Add, CrossLeftSort,
          dag.LoadLocal(line, Root(line, PushString("/bar")), JTextT),
          Memoize(right, 100))

        val expected = Join(line, Or, IdentitySort, Sort(expectedLeft, Vector(1)), right)
        
        orderCrosses(input) mustEqual expected
      }
      
      "both" >> {
        val line = Line(0, "")
        
        val foo = dag.LoadLocal(line, Root(line, PushString("/foo")), JTextT)
        val bar = dag.LoadLocal(line, Root(line, PushString("/bar")), JTextT)
        val baz = dag.LoadLocal(line, Root(line, PushString("/baz")), JTextT)
        
        val left = Join(line, Add, CrossRightSort, bar, foo)
        val right = Join(line, Add, CrossRightSort, baz, foo)
        
        val expectedLeft = Join(line, Add, CrossLeftSort,
          dag.LoadLocal(line, Root(line, PushString("/bar")), JTextT),
          Memoize(foo, 100))

        val expectedRight = Join(line, Add, CrossLeftSort,
          dag.LoadLocal(line, Root(line, PushString("/baz")), JTextT),
          Memoize(foo, 100))

        val input = Join(line, Or, IdentitySort, left, right)
        val expected = Join(line, Or, IdentitySort, Sort(expectedLeft, Vector(1)), Sort(expectedRight, Vector(1)))
        
        orderCrosses(input) mustEqual expected
      }
      
      "random-case-without-a-label" >> {
        val line = Line(0, "")
        
        val numbers = dag.LoadLocal(line, Root(line, PushString("/hom/numbers")), JTextT)
        val numbers3 = dag.LoadLocal(line, Root(line, PushString("/hom/numbers3")), JTextT)
        
        val input = Join(line, Add, IdentitySort,
          Join(line, Add, CrossRightSort,
            Join(line, Eq, IdentitySort, numbers, numbers),
            Join(line, Eq, IdentitySort, numbers3, numbers3)),
          Join(line, Eq, IdentitySort, numbers3, numbers3))
        
        val expected = Join(line, Add, IdentitySort,
          Sort(
            Join(line, Add, CrossLeftSort,
              Join(line, Eq, IdentitySort, numbers, numbers),
              Memoize(Join(line, Eq, IdentitySort, numbers3, numbers3), 100)),
            Vector(1)),
          Join(line, Eq, IdentitySort, numbers3, numbers3))
            
        orderCrosses(input) mustEqual expected
      }
    }

    // this will eventually be a re-order cross test case
    "insert sorts for filter on out-of-order operand set" >> {
      "left" >> {
        val line = Line(0, "")
        
        val left = dag.LoadLocal(line, Root(line, PushString("/foo")), JTextT)
        val right = Join(line, Add, CrossRightSort,
          dag.LoadLocal(line, Root(line, PushString("/bar")), JTextT),
          left)
        
        val input = Filter(line, IdentitySort, left, right)
        
        val expectedRight = Join(line, Add, CrossLeftSort,
          dag.LoadLocal(line, Root(line, PushString("/bar")), JTextT),
          Memoize(left, 100))

        val expected = Filter(line, IdentitySort, left, Sort(expectedRight, Vector(1)))
        
        orderCrosses(input) mustEqual expected
      }
      
      "right" >> {
        val line = Line(0, "")
        
        val right = dag.LoadLocal(line, Root(line, PushString("/foo")), JTextT)
        val left = Join(line, Add, CrossRightSort,
          dag.LoadLocal(line, Root(line, PushString("/bar")), JTextT),
          right)
        
        val input = Filter(line, IdentitySort, left, right)
        
        val expectedLeft = Join(line, Add, CrossLeftSort,
          dag.LoadLocal(line, Root(line, PushString("/bar")), JTextT),
          Memoize(right, 100))
        
        val expected = Filter(line, IdentitySort, Sort(expectedLeft, Vector(1)), right)
        
        orderCrosses(input) mustEqual expected
      }
      
      "both" >> {
        val line = Line(0, "")
        
        val foo = dag.LoadLocal(line, Root(line, PushString("/foo")), JTextT)
        val bar = dag.LoadLocal(line, Root(line, PushString("/bar")), JTextT)
        val baz = dag.LoadLocal(line, Root(line, PushString("/baz")), JTextT)
        
        val left = Join(line, Add, CrossRightSort, bar, foo)
        val right = Join(line, Add, CrossRightSort, baz, foo)
        
        val expectedLeft = Join(line, Add, CrossLeftSort,
          dag.LoadLocal(line, Root(line, PushString("/bar")), JTextT),
          Memoize(foo, 100))

        val expectedRight = Join(line, Add, CrossLeftSort,
          dag.LoadLocal(line, Root(line, PushString("/baz")), JTextT),
          Memoize(foo, 100))

        val input = Filter(line, IdentitySort, left, right)
        val expected = Filter(line, IdentitySort, Sort(expectedLeft, Vector(1)), Sort(expectedRight, Vector(1)))
        
        orderCrosses(input) mustEqual expected
      }
    }
  }
}
