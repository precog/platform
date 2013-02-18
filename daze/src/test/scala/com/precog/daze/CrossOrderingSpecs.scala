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

import com.precog.common._
import bytecode._
import org.specs2.mutable._
import com.precog.yggdrasil._

object CrossOrderingSpecs extends Specification with CrossOrdering with FNDummyModule {
  import instructions._
  import dag._

  type Lib = RandomLibrary
  object library extends RandomLibrary
  
  "cross ordering" should {
    "order in the appropriate direction when one side is singleton" >> {
      "left" >> {
        val line = Line(1, 1, "")
        
        val left = dag.LoadLocal(Const(CString("/foo"))(line))(line)
        val right = Const(CLong(42))(line)
        
        val input = Join(Eq, CrossRightSort, left, right)(line)
        val expected = Join(Eq, CrossLeftSort, left, right)(line)
        
        orderCrosses(input) mustEqual expected
      }
      
      "right" >> {
        val line = Line(1, 1, "")
        
        val left = Const(CLong(42))(line)
        val right = dag.LoadLocal(Const(CString("/foo"))(line))(line)
        
        val input = Join(Eq, CrossLeftSort, left, right)(line)
        val expected = Join(Eq, CrossRightSort, left, right)(line)
        
        orderCrosses(input) mustEqual expected
      }
    }
    
    "refrain from sorting when sets are already aligned in match" in {
      val line = Line(1, 1, "")
      
      val left = dag.LoadLocal(Const(CString("/foo"))(line))(line)
      val right = Const(CLong(42))(line)
      
      val input = Join(Or, IdentitySort, Join(Eq, CrossRightSort, left, right)(line), left)(line)
      val expected = Join(Or, IdentitySort, Join(Eq, CrossLeftSort, left, right)(line), left)(line)
      
      orderCrosses(input) mustEqual expected
    }
    
    "refrain from sorting when sets are already aligned in filter" in {
      val line = Line(1, 1, "")
      
      val left = dag.LoadLocal(Const(CString("/foo"))(line))(line)
      val right = Const(CLong(42))(line)
      
      val input = Filter(IdentitySort, Join(Eq, CrossRightSort, left, right)(line), left)(line)
      val expected = Filter(IdentitySort, Join(Eq, CrossLeftSort, left, right)(line), left)(line)
      
      orderCrosses(input) mustEqual expected
    }

    // this will eventually be a re-order cross test case
    "insert sorts for match on out-of-order operand set" >> {
      "left" >> {
        val line = Line(1, 1, "")
        
        val left = dag.LoadLocal(Const(CString("/foo"))(line), JTextT)(line)
        val right = Join(Add, CrossRightSort,
          dag.LoadLocal(Const(CString("/bar"))(line), JTextT)(line),
          left)(line)
        
        val input = Join(Or, IdentitySort, left, right)(line)
        
        val expectedRight = Join(Add, CrossLeftSort,
          dag.LoadLocal(Const(CString("/bar"))(line), JTextT)(line),
          left)(line)

        val expected = Join(Or, IdentitySort, left, Sort(expectedRight, Vector(1)))(line)
        
        orderCrosses(input) mustEqual expected
      }
      
      "right" >> {
        val line = Line(1, 1, "")
        
        val right = dag.LoadLocal(Const(CString("/foo"))(line), JTextT)(line)
        val left = Join(Add, CrossRightSort,
          dag.LoadLocal(Const(CString("/bar"))(line), JTextT)(line),
          right)(line)
        
        val input = Join(Or, IdentitySort, left, right)(line)
        
        val expectedLeft = Join(Add, CrossLeftSort,
          dag.LoadLocal(Const(CString("/bar"))(line), JTextT)(line),
          right)(line)

        val expected = Join(Or, IdentitySort, Sort(expectedLeft, Vector(1)), right)(line)
        
        orderCrosses(input) mustEqual expected
      }
      
      "both" >> {
        val line = Line(1, 1, "")
        
        val foo = dag.LoadLocal(Const(CString("/foo"))(line), JTextT)(line)
        val bar = dag.LoadLocal(Const(CString("/bar"))(line), JTextT)(line)
        val baz = dag.LoadLocal(Const(CString("/baz"))(line), JTextT)(line)
        
        val left = Join(Add, CrossRightSort, bar, foo)(line)
        val right = Join(Add, CrossRightSort, baz, foo)(line)
        
        val expectedLeft = Join(Add, CrossLeftSort,
          dag.LoadLocal(Const(CString("/bar"))(line), JTextT)(line),
          foo)(line)

        val expectedRight = Join(Add, CrossLeftSort,
          dag.LoadLocal(Const(CString("/baz"))(line), JTextT)(line),
          foo)(line)

        val input = Join(Or, IdentitySort, left, right)(line)
        val expected = Join(Or, IdentitySort, Sort(expectedLeft, Vector(1)), Sort(expectedRight, Vector(1)))(line)
        
        orderCrosses(input) mustEqual expected
      }
      
      "random-case-without-a-label" >> {
        val line = Line(1, 1, "")
        
        val numbers = dag.LoadLocal(Const(CString("/hom/numbers"))(line), JTextT)(line)
        val numbers3 = dag.LoadLocal(Const(CString("/hom/numbers3"))(line), JTextT)(line)
        
        val input = Join(Add, IdentitySort,
          Join(Add, CrossRightSort,
            Join(Eq, IdentitySort, numbers, numbers)(line),
            Join(Eq, IdentitySort, numbers3, numbers3)(line))(line),
          Join(Eq, IdentitySort, numbers3, numbers3)(line))(line)
        
        val expected = Join(Add, IdentitySort,
          Sort(
            Join(Add, CrossLeftSort,
              Join(Eq, IdentitySort, numbers, numbers)(line),
              Memoize(Join(Eq, IdentitySort, numbers3, numbers3)(line), 100))(line),
            Vector(1)),
          Join(Eq, IdentitySort, numbers3, numbers3)(line))(line)
            
        orderCrosses(input) mustEqual expected
      }
    }

    // this will eventually be a re-order cross test case
    "insert sorts for filter on out-of-order operand set" >> {
      "left" >> {
        val line = Line(1, 1, "")
        
        val left = dag.LoadLocal(Const(CString("/foo"))(line), JTextT)(line)
        val right = Join(Add, CrossRightSort,
          dag.LoadLocal(Const(CString("/bar"))(line), JTextT)(line),
          left)(line)
        
        val input = Filter(IdentitySort, left, right)(line)
        
        val expectedRight = Join(Add, CrossLeftSort,
          dag.LoadLocal(Const(CString("/bar"))(line), JTextT)(line),
          left)(line)

        val expected = Filter(IdentitySort, left, Sort(expectedRight, Vector(1)))(line)
        
        orderCrosses(input) mustEqual expected
      }
      
      "right" >> {
        val line = Line(1, 1, "")
        
        val right = dag.LoadLocal(Const(CString("/foo"))(line), JTextT)(line)
        val left = Join(Add, CrossRightSort,
          dag.LoadLocal(Const(CString("/bar"))(line), JTextT)(line),
          right)(line)
        
        val input = Filter(IdentitySort, left, right)(line)
        
        val expectedLeft = Join(Add, CrossLeftSort,
          dag.LoadLocal(Const(CString("/bar"))(line), JTextT)(line),
          right)(line)
        
        val expected = Filter(IdentitySort, Sort(expectedLeft, Vector(1)), right)(line)
        
        orderCrosses(input) mustEqual expected
      }
      
      "both" >> {
        val line = Line(1, 1, "")
        
        val foo = dag.LoadLocal(Const(CString("/foo"))(line), JTextT)(line)
        val bar = dag.LoadLocal(Const(CString("/bar"))(line), JTextT)(line)
        val baz = dag.LoadLocal(Const(CString("/baz"))(line), JTextT)(line)
        
        val left = Join(Add, CrossRightSort, bar, foo)(line)
        val right = Join(Add, CrossRightSort, baz, foo)(line)
        
        val expectedLeft = Join(Add, CrossLeftSort,
          dag.LoadLocal(Const(CString("/bar"))(line), JTextT)(line),
          foo)(line)

        val expectedRight = Join(Add, CrossLeftSort,
          dag.LoadLocal(Const(CString("/baz"))(line), JTextT)(line),
          foo)(line)

        val input = Filter(IdentitySort, left, right)(line)
        val expected = Filter(IdentitySort, Sort(expectedLeft, Vector(1)), Sort(expectedRight, Vector(1)))(line)
        
        orderCrosses(input) mustEqual expected
      }
    }
    
    "memoize RHS of cross when it is not a forcing point" in {
      val line = Line(1, 1, "")
      
      val foo = dag.LoadLocal(Const(CString("/foo"))(line), JTextT)(line)
      val bar = dag.LoadLocal(Const(CString("/bar"))(line), JTextT)(line)
      
      val barAdd = Join(Add, IdentitySort, bar, bar)(line)
      
      val input = Join(Add, CrossLeftSort, foo, barAdd)(line)
      
      val expected = Join(Add, CrossLeftSort, foo, Memoize(barAdd, 100))(line)
      
      orderCrosses(input) mustEqual expected
    }
    
    "refrain from memoizing RHS of cross when it is a forcing point" in {
      val line = Line(1, 1, "")
      
      val foo = dag.LoadLocal(Const(CString("/foo"))(line), JTextT)(line)
      val bar = dag.LoadLocal(Const(CString("/bar"))(line), JTextT)(line)
      
      val input = Join(Add, CrossLeftSort, foo, bar)(line)
      
      orderCrosses(input) mustEqual input
    }
    
    "refrain from resorting by identity when cogrouping after an ordered cross" in {
      val line = Line(1, 1, "")
      
      val foo = dag.LoadLocal(Const(CString("/foo"))(line), JTextT)(line)
      
      val input =
        Join(Add, IdentitySort,
          Join(Add, CrossLeftSort,
            foo,
            Const(CLong(42))(line))(line),
          foo)(line)
          
      orderCrosses(input) mustEqual input
    }
    
    "refrain from resorting by value when cogrouping after an ordered cross" in {
      val line = Line(1, 1, "")
      
      val foo = dag.LoadLocal(Const(CString("/foo"))(line), JTextT)(line)
      
      val input =
        Join(Add, ValueSort(0),
          Join(Add, CrossLeftSort,
            SortBy(foo, "a", "b", 0),
            Const(CLong(42))(line))(line),
          SortBy(foo, "a", "b", 0))(line)
          
      orderCrosses(input) mustEqual input
    }
  }
}
