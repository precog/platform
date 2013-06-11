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
  import TableModule.CrossOrder._

  type Lib = RandomLibrary
  object library extends RandomLibrary
  
  "cross ordering" should {
    "order in the appropriate direction when one side is singleton" >> {
      "left" >> {
        val line = Line(1, 1, "")
        
        val left = dag.AbsoluteLoad(Const(CString("/foo"))(line))(line)
        val right = Const(CLong(42))(line)
        
        val input = Join(Eq, Cross(None), left, right)(line)
        val expected = Join(Eq, Cross(Some(CrossLeft)), left, right)(line)
        
        orderCrosses(input) mustEqual expected
      }
      
      "right" >> {
        val line = Line(1, 1, "")
        
        val left = Const(CLong(42))(line)
        val right = dag.AbsoluteLoad(Const(CString("/foo"))(line))(line)
        
        val input = Join(Eq, Cross(None), left, right)(line)
        val expected = Join(Eq, Cross(Some(CrossRight)), left, right)(line)
        
        orderCrosses(input) mustEqual expected
      }
    }
    
    "refrain from sorting when sets are already aligned in match" in {
      val line = Line(1, 1, "")
      
      val left = dag.AbsoluteLoad(Const(CString("/foo"))(line))(line)
      val right = Const(CLong(42))(line)
      
      val input = Join(Or, IdentitySort, Join(Eq, Cross(None), left, right)(line), left)(line)
      val expected = Join(Or, IdentitySort, Join(Eq, Cross(Some(CrossLeft)), left, right)(line), left)(line)
      
      orderCrosses(input) mustEqual expected
    }
    
    "refrain from sorting when sets are already aligned in filter" in {
      val line = Line(1, 1, "")
      
      val left = dag.AbsoluteLoad(Const(CString("/foo"))(line))(line)
      val right = Const(CLong(42))(line)
      
      val input = Filter(IdentitySort, Join(Eq, Cross(None), left, right)(line), left)(line)
      val expected = Filter(IdentitySort, Join(Eq, Cross(Some(CrossLeft)), left, right)(line), left)(line)
      
      orderCrosses(input) mustEqual expected
    }

    "memoize RHS of cross when it is not a forcing point" in {
      val line = Line(1, 1, "")
      
      val foo = dag.AbsoluteLoad(Const(CString("/foo"))(line), JTextT)(line)
      val bar = dag.AbsoluteLoad(Const(CString("/bar"))(line), JTextT)(line)
      
      val barAdd = Join(Add, IdentitySort, bar, bar)(line)
      
      val input = Join(Add, Cross(None), foo, barAdd)(line)
      
      val expected = Join(Add, Cross(None), foo, Memoize(barAdd, 100))(line)
      
      orderCrosses(input) mustEqual expected
    }
    
    "refrain from memoizing RHS of cross when it is a forcing point" in {
      val line = Line(1, 1, "")
      
      val foo = dag.AbsoluteLoad(Const(CString("/foo"))(line), JTextT)(line)
      val bar = dag.AbsoluteLoad(Const(CString("/bar"))(line), JTextT)(line)
      
      val input = Join(Add, Cross(None), foo, bar)(line)
      
      orderCrosses(input) mustEqual input
    }
    
    "refrain from resorting by identity when cogrouping after an ordered cross" in {
      val line = Line(1, 1, "")
      
      val foo = dag.AbsoluteLoad(Const(CString("/foo"))(line), JTextT)(line)
      
      val input =
        Join(Add, IdentitySort,
          Join(Add, Cross(Some(CrossLeft)),
            foo,
            Const(CLong(42))(line))(line),
          foo)(line)
          
      orderCrosses(input) mustEqual input
    }
    
    "refrain from resorting by value when cogrouping after an ordered cross" in {
      val line = Line(1, 1, "")
      
      val foo = dag.AbsoluteLoad(Const(CString("/foo"))(line), JTextT)(line)
      
      val input =
        Join(Add, ValueSort(0),
          Join(Add, Cross(Some(CrossLeft)),
            AddSortKey(foo, "a", "b", 0),
            Const(CLong(42))(line))(line),
          AddSortKey(foo, "a", "b", 0))(line)
          
      orderCrosses(input) mustEqual input
    }
  }
}
