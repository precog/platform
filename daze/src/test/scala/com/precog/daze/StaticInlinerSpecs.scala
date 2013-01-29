package com.precog
package daze

import org.specs2.mutable._

import yggdrasil._
import yggdrasil.test._

trait StaticInlinerSpecs[M[+_]] extends Specification
    with EvaluatorTestSupport[M] {
      
  import dag._
  import instructions._
  import library._
  
  "static inlining of Root computation" should {
    "detect and resolve addition" in {
      val line = Line(1, 1, "")
      
      val input = Join(Add, CrossLeftSort,
        Const(CLong(42))(line),
        Const(CDouble(3.14))(line))(line)
        
      val expected = Const(CNum(45.14))(line)
      
      inlineStatics(input, defaultEvaluationContext, Set.empty) mustEqual expected
    }
    
    "detect and resolve operations at depth" in {
      val line = Line(1, 1, "")
      
      val input = Join(Add, CrossLeftSort,
        Const(CLong(42))(line),
        Join(Mul, CrossRightSort,
          Const(CDouble(3.14))(line),
          Const(CLong(2))(line))(line))(line)
        
      val expected = Const(CNum(48.28))(line)
      
      inlineStatics(input, defaultEvaluationContext, Set.empty) mustEqual expected
    }
    
    "produce CUndefined in cases where the operation is undefined" in {
      val line = Line(1, 1, "")
      
      val input = Join(Div, CrossLeftSort,
        Const(CLong(42))(line),
        Const(CLong(0))(line))(line)
        
      val expected = Const(CUndefined)(line)
      
      inlineStatics(input, defaultEvaluationContext, Set.empty) mustEqual expected
    }
    
    "propagate through static computations CUndefined when produced at depth" in {
      val line = Line(1, 1, "")
      
      val input = Join(Add, CrossLeftSort,
        Const(CLong(42))(line),
        Join(Div, CrossRightSort,
          Const(CDouble(3.14))(line),
          Const(CLong(0))(line))(line))(line)
        
      val expected = Const(CUndefined)(line)
      
      inlineStatics(input, defaultEvaluationContext, Set.empty) mustEqual expected
    }
    
    "propagate through non-singleton computations CUndefined when produced at depth" >> {
      val line = Line(1, 1, "")
      
      "left" >> {
        val input = Join(Add, CrossLeftSort,
          dag.LoadLocal(Const(CString("/foo"))(line))(line),
          Join(Div, CrossRightSort,
            Const(CDouble(3.14))(line),
            Const(CLong(0))(line))(line))(line)
          
        val expected = Const(CUndefined)(line)
        
        inlineStatics(input, defaultEvaluationContext, Set.empty) mustEqual expected
      }
      
      "right" >> {
        val input = Join(Add, CrossLeftSort,
          Join(Div, CrossRightSort,
            Const(CDouble(3.14))(line),
            Const(CLong(0))(line))(line),
          dag.LoadLocal(Const(CString("/foo"))(line))(line))(line)
          
        val expected = Const(CUndefined)(line)
        
        inlineStatics(input, defaultEvaluationContext, Set.empty) mustEqual expected
      }
    }
    
    "reduce filters with static RHS" >> {
      val line = Line(1, 1, "")
      
      "true" >> {
        val input = Filter(CrossLeftSort,
          dag.LoadLocal(Const(CString("/foo"))(line))(line),
          Const(CBoolean(true))(line))(line)
          
        inlineStatics(input, defaultEvaluationContext, Set.empty) mustEqual dag.LoadLocal(Const(CString("/foo"))(line))(line)
      }
      
      "false" >> {
        val input = Filter(CrossLeftSort,
          dag.LoadLocal(Const(CString("/foo"))(line))(line),
          Const(CBoolean(false))(line))(line)
          
        inlineStatics(input, defaultEvaluationContext, Set.empty) mustEqual Const(CUndefined)(line)
      }
    }
  }
}

object StaticInlinerSpecs extends StaticInlinerSpecs[YId] with test.YIdInstances
