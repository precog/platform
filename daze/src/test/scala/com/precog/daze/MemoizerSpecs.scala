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

import bytecode.RandomLibrary

import org.specs2.mutable._

object MemoizerSpecs extends Specification with Memoizer with RandomLibrary {
  import instructions._
  import dag._
  
  "dag memoization" should {
    "not memoize a sub-graph of non-forcing operations" in {
      val line = Line(0, "")
      
      val clicks = dag.LoadLocal(line, Root(line, PushString("/clicks")))
      
      val input =
        Join(line, Add, IdentitySort,
          clicks,
          Operate(line, Neg,
            Join(line, Mul, CrossLeftSort,
              clicks,
              Root(line, PushNum("42")))))
          
      memoize(input) mustEqual input
    }
    
    "insert memoization nodes for forcing points referenced by morph1 and cross" in {
      val line = Line(0, "")
      
      val clicks = dag.LoadLocal(line, Root(line, PushString("/clicks")))
      
      val input =
        Join(line, Add, IdentitySort,
          dag.Morph1(line, libMorphism1.head, clicks),
          Join(line, Mul, CrossLeftSort,
            clicks,
            clicks))
            
      val memoClicks = Memoize(clicks, 3)
      
      val expected =
        Join(line, Add, IdentitySort,
          dag.Morph1(line, libMorphism1.head, memoClicks),
          Join(line, Mul, CrossLeftSort,
            memoClicks,
            memoClicks))
            
      memoize(input) mustEqual expected
    }
    
    "insert memoization nodes for split referenced by morph1 and cross" in {
      val line = Line(0, "")
      
      val clicks = 
        dag.Morph1(line, libMorphism1.head, dag.LoadLocal(line, Root(line, PushString("/clicks"))))
      
      lazy val split: dag.Split = dag.Split(line,
        dag.Group(0, clicks, UnfixedSolution(1, clicks)),
        SplitParam(line, 1)(split))
      
      val input =
        Join(line, Add, IdentitySort,
          dag.Morph1(line, libMorphism1.head, split),
          Join(line, Mul, CrossLeftSort,
            split,
            split))
            
      val memoSplit = Memoize(split, 2)
      
      val expected =
        Join(line, Add, IdentitySort,
          dag.Morph1(line, libMorphism1.head, memoSplit),
          Join(line, Mul, CrossLeftSort,
            memoSplit,
            memoSplit))
            
      memoize(input) mustEqual expected
    }
    
    "insert memoization nodes for reduce parenting a split" in {
      val line = Line(0, "")
      
      val clicks = 
        dag.Morph1(line, libMorphism1.head, dag.LoadLocal(line, Root(line, PushString("/clicks"))))
      
      val join =
        Join(line, Add, IdentitySort,
          dag.Morph1(line, libMorphism1.head, clicks),
          Join(line, Mul, CrossLeftSort,
            clicks,
            clicks))
            
      lazy val split: dag.Split = dag.Split(line, 
        dag.Group(0, join, UnfixedSolution(1, join)),
        SplitParam(line, 1)(split))
            
      val memoClicks = Memoize(clicks, 2)
      
      val expectedJoin =
        Join(line, Add, IdentitySort,
          dag.Morph1(line, libMorphism1.head, memoClicks),
          Join(line, Mul, CrossLeftSort,
            memoClicks,
            memoClicks))
            
      lazy val expectedSplit: dag.Split = dag.Split(line, 
        dag.Group(0, expectedJoin, UnfixedSolution(1, expectedJoin)),
        SplitParam(line, 1)(expectedSplit))
            
      memoize(split) mustEqual expectedSplit
    }
    
    // TODO check memoize rewrite above a Split
  }
}
