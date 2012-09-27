package com.precog
package daze

import bytecode.StaticLibrary

import org.specs2.mutable._

object MemoizerSpecs extends Specification with Memoizer with StaticLibrary {
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
    
    "insert memoization nodes for morph1 referenced by morph1 and cross" in {
      val line = Line(0, "")
      
      val clicks = 
        dag.Morph1(line, libMorphism1.head, dag.LoadLocal(line, Root(line, PushString("/clicks"))))
      
      val input =
        Join(line, Add, IdentitySort,
          dag.Morph1(line, libMorphism1.head, clicks),
          Join(line, Mul, CrossLeftSort,
            clicks,
            clicks))
            
      val memoClicks = Memoize(clicks, 2)
      
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
  }
}
