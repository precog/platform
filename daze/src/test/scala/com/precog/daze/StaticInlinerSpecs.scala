package com.precog
package daze

import org.specs2.mutable._

import yggdrasil._
import yggdrasil.test._

trait StaticInlinerSpecs[M[+_]] extends Specification
    with EvaluatorTestSupport[M]
    with StaticInliner[M] {
      
  import dag._
  import instructions._
  
  "static inlining of Root computation" should {
    "detect and resolve addition" in {
      val line = Line(0, "")
      
      val input = Join(line, Add, CrossLeftSort,
        Const(line, CLong(42)),
        Const(line, CDouble(3.14)))
        
      val expected = Const(line, CNum(45.14))
      
      inlineStatics(input, defaultEvaluationContext) mustEqual expected
    }
    
    "detect and resolve operations at depth" in {
      val line = Line(0, "")
      
      val input = Join(line, Add, CrossLeftSort,
        Const(line, CLong(42)),
        Join(line, Mul, CrossRightSort,
          Const(line, CDouble(3.14)),
          Const(line, CLong(2))))
        
      val expected = Const(line, CNum(48.28))
      
      inlineStatics(input, defaultEvaluationContext) mustEqual expected
    }
    
    "produce CUndefined in cases where the operation is undefined" in {
      val line = Line(0, "")
      
      val input = Join(line, Div, CrossLeftSort,
        Const(line, CLong(42)),
        Const(line, CLong(0)))
        
      val expected = Const(line, CUndefined)
      
      inlineStatics(input, defaultEvaluationContext) mustEqual expected
    }
    
    "propagate through static computations CUndefined when produced at depth" in {
      val line = Line(0, "")
      
      val input = Join(line, Add, CrossLeftSort,
        Const(line, CLong(42)),
        Join(line, Div, CrossRightSort,
          Const(line, CDouble(3.14)),
          Const(line, CLong(0))))
        
      val expected = Const(line, CUndefined)
      
      inlineStatics(input, defaultEvaluationContext) mustEqual expected
    }
    
    "propagate through non-singleton computations CUndefined when produced at depth" >> {
      val line = Line(0, "")
      
      "left" >> {
        val input = Join(line, Add, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("/foo"))),
          Join(line, Div, CrossRightSort,
            Const(line, CDouble(3.14)),
            Const(line, CLong(0))))
          
        val expected = Const(line, CUndefined)
        
        inlineStatics(input, defaultEvaluationContext) mustEqual expected
      }
      
      "right" >> {
        val input = Join(line, Add, CrossLeftSort,
          Join(line, Div, CrossRightSort,
            Const(line, CDouble(3.14)),
            Const(line, CLong(0))),
          dag.LoadLocal(line, Const(line, CString("/foo"))))
          
        val expected = Const(line, CUndefined)
        
        inlineStatics(input, defaultEvaluationContext) mustEqual expected
      }
    }
    
    "reduce filters with static RHS" >> {
      val line = Line(0, "")
      
      "true" >> {
        val input = Filter(line, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("/foo"))),
          Const(line, CBoolean(true)))
          
        inlineStatics(input, defaultEvaluationContext) mustEqual dag.LoadLocal(line, Const(line, CString("/foo")))
      }
      
      "false" >> {
        val input = Filter(line, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("/foo"))),
          Const(line, CBoolean(false)))
          
        inlineStatics(input, defaultEvaluationContext) mustEqual Const(line, CUndefined)
      }
    }
  }
}

object StaticInlinerSpecs extends StaticInlinerSpecs[YId] with test.YIdInstances
