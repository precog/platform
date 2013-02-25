package com.precog
package daze

import com.precog.common._
import com.precog.bytecode.StaticLibrary
import com.precog.yggdrasil._
import org.specs2.mutable._

object MemoizerSpecs extends Specification with Memoizer with FNDummyModule {
  import instructions._
  import dag._

  type Lib = StaticLibrary
  val library = new StaticLibrary{}
  import library._

  "dag memoization" should {
    "not memoize a sub-graph of non-forcing operations" in {
      val line = Line(1, 1, "")
      
      val clicks = dag.LoadLocal(Const(CString("/clicks"))(line))(line)
      
      val input =
        Join(Add, IdentitySort,
          clicks,
          Operate(Neg,
            Join(Mul, CrossLeftSort,
              clicks,
              Const(CLong(42))(line))(line))(line))(line)
          
      memoize(input) mustEqual input
    }
    
    "insert memoization nodes for morph1 referenced by morph1 and cross" in {
      val line = Line(1, 1, "")
      
      val clicks = 
        dag.Morph1(libMorphism1.head, dag.LoadLocal(Const(CString("/clicks"))(line))(line))(line)
      
      val input =
        Join(Add, IdentitySort,
          dag.Morph1(libMorphism1.head, clicks)(line),
          Join(Mul, CrossLeftSort,
            clicks,
            clicks)(line))(line)
            
      val memoClicks = Memoize(clicks, 3)
      
      val expected =
        Join(Add, IdentitySort,
          dag.Morph1(libMorphism1.head, memoClicks)(line),
          Join(Mul, CrossLeftSort,
            memoClicks,
            memoClicks)(line))(line)
            
      memoize(input) mustEqual expected
    }
    
    "insert memoization nodes for split referenced by morph1 and cross" in {
      val line = Line(1, 1, "")
      
      val clicks = 
        dag.Morph1(libMorphism1.head, dag.LoadLocal(Const(CString("/clicks"))(line))(line))(line)
      
      lazy val split: dag.Split = dag.Split(
        dag.Group(0, clicks, UnfixedSolution(1, clicks)),
        SplitParam(1)(split)(line))(line)
      
      val input =
        Join(Add, IdentitySort,
          dag.Morph1(libMorphism1.head, split)(line),
          Join(Mul, CrossLeftSort,
            split,
            split)(line))(line)
            
      val memoSplit = Memoize(split, 3)
      
      val expected =
        Join(Add, IdentitySort,
          dag.Morph1(libMorphism1.head, memoSplit)(line),
          Join(Mul, CrossLeftSort,
            memoSplit,
            memoSplit)(line))(line)
            
      memoize(input) mustEqual expected
    }
    
    "insert memoization nodes for reduce parenting a split" in {
      val line = Line(1, 1, "")
      
      val clicks = 
        dag.Morph1(libMorphism1.head, dag.LoadLocal(Const(CString("/clicks"))(line))(line))(line)
      
      val join =
        Join(Add, IdentitySort,
          dag.Morph1(libMorphism1.head, clicks)(line),
          Join(Mul, CrossLeftSort,
            clicks,
            clicks)(line))(line)
            
      lazy val split: dag.Split = dag.Split(
        dag.Group(0, join, UnfixedSolution(1, join)),
        SplitParam(1)(split)(line))(line)
            
      val memoClicks = Memoize(clicks, 3)
      
      val expectedJoin =
        Join(Add, IdentitySort,
          dag.Morph1(libMorphism1.head, memoClicks)(line),
          Join(Mul, CrossLeftSort,
            memoClicks,
            memoClicks)(line))(line)
            
      lazy val expectedSplit: dag.Split = dag.Split(
        dag.Group(0, expectedJoin, UnfixedSolution(1, expectedJoin)),
        SplitParam(1)(expectedSplit)(line))(line)
        
      memoize(split) mustEqual expectedSplit
    }
  }
}
