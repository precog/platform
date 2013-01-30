package com.precog
package daze

import org.specs2.mutable._

import yggdrasil._
import yggdrasil.test._

import blueeyes.json._

trait StaticInlinerSpecs[M[+_]] extends Specification
    with EvaluatorTestSupport[M] {
      
  import dag._
  import instructions._
  import library._
  
  "static inlining of Root computation" should {
    "detect and resolve addition" in {
      val line = Line(1, 1, "")
      
      val input = Join(Add, CrossLeftSort,
        Const(JNumLong(42))(line),
        Const(JNum(3.14))(line))(line)
        
      val expected = Const(JNum(45.14))(line)
      
      inlineStatics(input, defaultEvaluationContext, Set.empty) mustEqual expected
    }
    
    "detect and resolve operations at depth" in {
      val line = Line(1, 1, "")
      
      val input = Join(Add, CrossLeftSort,
        Const(JNumLong(42))(line),
        Join(Mul, CrossRightSort,
          Const(JNum(3.14))(line),
          Const(JNumLong(2))(line))(line))(line)
        
      val expected = Const(JNum(48.28))(line)
      
      inlineStatics(input, defaultEvaluationContext, Set.empty) mustEqual expected
    }
    
    "produce JUndefined in cases where the operation is undefined" in {
      val line = Line(1, 1, "")
      
      val input = Join(Div, CrossLeftSort,
        Const(JNumLong(42))(line),
        Const(JNumLong(0))(line))(line)
        
      val expected = Const(JUndefined)(line)
      
      inlineStatics(input, defaultEvaluationContext, Set.empty) mustEqual expected
    }
    
    "propagate through static computations JUndefined when produced at depth" in {
      val line = Line(1, 1, "")
      
      val input = Join(Add, CrossLeftSort,
        Const(JNumLong(42))(line),
        Join(Div, CrossRightSort,
          Const(JNum(3.14))(line),
          Const(JNumLong(0))(line))(line))(line)
        
      val expected = Const(JUndefined)(line)
      
      inlineStatics(input, defaultEvaluationContext, Set.empty) mustEqual expected
    }
    
    "propagate through non-singleton computations JUndefined when produced at depth" >> {
      val line = Line(1, 1, "")
      
      "left" >> {
        val input = Join(Add, CrossLeftSort,
          dag.LoadLocal(Const(JString("/foo"))(line))(line),
          Join(Div, CrossRightSort,
            Const(JNum(3.14))(line),
            Const(JNumLong(0))(line))(line))(line)
          
        val expected = Const(JUndefined)(line)
        
        inlineStatics(input, defaultEvaluationContext, Set.empty) mustEqual expected
      }
      
      "right" >> {
        val input = Join(Add, CrossLeftSort,
          Join(Div, CrossRightSort,
            Const(JNum(3.14))(line),
            Const(JNumLong(0))(line))(line),
          dag.LoadLocal(Const(JString("/foo"))(line))(line))(line)
          
        val expected = Const(JUndefined)(line)
        
        inlineStatics(input, defaultEvaluationContext, Set.empty) mustEqual expected
      }
    }
    
    "reduce filters with static RHS" >> {
      val line = Line(1, 1, "")
      
      "true" >> {
        val input = Filter(CrossLeftSort,
          dag.LoadLocal(Const(JString("/foo"))(line))(line),
          Const(JBool(true))(line))(line)
          
        inlineStatics(input, defaultEvaluationContext, Set.empty) mustEqual dag.LoadLocal(Const(JString("/foo"))(line))(line)
      }
      
      "false" >> {
        val input = Filter(CrossLeftSort,
          dag.LoadLocal(Const(JString("/foo"))(line))(line),
          Const(JBool(false))(line))(line)
          
        inlineStatics(input, defaultEvaluationContext, Set.empty) mustEqual Const(JUndefined)(line)
      }
    }
  }
}

object StaticInlinerSpecs extends StaticInlinerSpecs[YId] with test.YIdInstances
