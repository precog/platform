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
import com.precog.util.Identifier
import com.precog.bytecode.StaticLibrary
import com.precog.yggdrasil._
import org.specs2.mutable._

object MemoizerSpecs extends Specification with Memoizer with FNDummyModule {
  import instructions._
  import dag._

  type Lib = StaticLibrary
  val library = new StaticLibrary {}
  import library._

  "dag memoization" should {
    "not memoize a sub-graph of non-forcing operations" in {
      val line = Line(1, 1, "")
      
      val clicks = dag.AbsoluteLoad(Const(CString("/clicks"))(line))(line)
      
      val input =
        Join(Add, IdentitySort,
          clicks,
          Operate(Neg,
            Join(Mul, Cross(None),
              clicks,
              Const(CLong(42))(line))(line))(line))(line)
          
      memoize(input) mustEqual input
    }
    
    "insert memoization nodes for morph1 referenced by morph1 and cross" in {
      val line = Line(1, 1, "")
      
      val clicks = 
        dag.Morph1(libMorphism1.head, dag.AbsoluteLoad(Const(CString("/clicks"))(line))(line))(line)
      
      val input =
        Join(Add, IdentitySort,
          dag.Morph1(libMorphism1.head, clicks)(line),
          Join(Mul, Cross(None),
            clicks,
            clicks)(line))(line)
            
      val memoClicks = Memoize(clicks, 3)
      
      val expected =
        Join(Add, IdentitySort,
          dag.Morph1(libMorphism1.head, memoClicks)(line),
          Join(Mul, Cross(None),
            memoClicks,
            memoClicks)(line))(line)
            
      memoize(input) mustEqual expected
    }
    
    "insert memoization nodes for split referenced by morph1 and cross" in {
      val line = Line(1, 1, "")
      
      val id = new Identifier
      
      val clicks = 
        dag.Morph1(libMorphism1.head, dag.AbsoluteLoad(Const(CString("/clicks"))(line))(line))(line)
      
      val split = dag.Split(
        dag.Group(0, clicks, UnfixedSolution(1, clicks)),
        SplitParam(1, id)(line), id)(line)
      
      val input =
        Join(Add, IdentitySort,
          dag.Morph1(libMorphism1.head, split)(line),
          Join(Mul, Cross(None),
            split,
            split)(line))(line)
            
      val memoSplit = Memoize(split, 3)
      
      val expected =
        Join(Add, IdentitySort,
          dag.Morph1(libMorphism1.head, memoSplit)(line),
          Join(Mul, Cross(None),
            memoSplit,
            memoSplit)(line))(line)
            
      memoize(input) mustEqual expected
    }
    
    "insert memoization nodes for reduce parenting a split" in {
      val line = Line(1, 1, "")
      
      val id = new Identifier
      
      val clicks = 
        dag.Morph1(libMorphism1.head, dag.AbsoluteLoad(Const(CString("/clicks"))(line))(line))(line)
      
      val join =
        Join(Add, IdentitySort,
          dag.Morph1(libMorphism1.head, clicks)(line),
          Join(Mul, Cross(None),
            clicks,
            clicks)(line))(line)
            
      val split = dag.Split(
        dag.Group(0, join, UnfixedSolution(1, join)),
        SplitParam(1, id)(line), id)(line)
        
      val memoClicks = Memoize(clicks, 3)
      
      val expectedJoin =
        Join(Add, IdentitySort,
          dag.Morph1(libMorphism1.head, memoClicks)(line),
          Join(Mul, Cross(None),
            memoClicks,
            memoClicks)(line))(line)
            
      val expectedSplit = dag.Split(
        dag.Group(0, expectedJoin, UnfixedSolution(1, expectedJoin)),
        SplitParam(1, id)(line), id)(line)
        
      memoize(split) mustEqual expectedSplit
    }
  }
}
