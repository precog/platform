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
      
      inlineStatics(input, defaultEvaluationContext) mustEqual expected
    }
    
    "detect and resolve operations at depth" in {
      val line = Line(1, 1, "")
      
      val input = Join(Add, CrossLeftSort,
        Const(CLong(42))(line),
        Join(Mul, CrossRightSort,
          Const(CDouble(3.14))(line),
          Const(CLong(2))(line))(line))(line)
        
      val expected = Const(CNum(48.28))(line)
      
      inlineStatics(input, defaultEvaluationContext) mustEqual expected
    }
    
    "produce CUndefined in cases where the operation is undefined" in {
      val line = Line(1, 1, "")
      
      val input = Join(Div, CrossLeftSort,
        Const(CLong(42))(line),
        Const(CLong(0))(line))(line)
        
      val expected = Const(CUndefined)(line)
      
      inlineStatics(input, defaultEvaluationContext) mustEqual expected
    }
    
    "propagate through static computations CUndefined when produced at depth" in {
      val line = Line(1, 1, "")
      
      val input = Join(Add, CrossLeftSort,
        Const(CLong(42))(line),
        Join(Div, CrossRightSort,
          Const(CDouble(3.14))(line),
          Const(CLong(0))(line))(line))(line)
        
      val expected = Const(CUndefined)(line)
      
      inlineStatics(input, defaultEvaluationContext) mustEqual expected
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
        
        inlineStatics(input, defaultEvaluationContext) mustEqual expected
      }
      
      "right" >> {
        val input = Join(Add, CrossLeftSort,
          Join(Div, CrossRightSort,
            Const(CDouble(3.14))(line),
            Const(CLong(0))(line))(line),
          dag.LoadLocal(Const(CString("/foo"))(line))(line))(line)
          
        val expected = Const(CUndefined)(line)
        
        inlineStatics(input, defaultEvaluationContext) mustEqual expected
      }
    }
    
    "reduce filters with static RHS" >> {
      val line = Line(1, 1, "")
      
      "true" >> {
        val input = Filter(CrossLeftSort,
          dag.LoadLocal(Const(CString("/foo"))(line))(line),
          Const(CBoolean(true))(line))(line)
          
        inlineStatics(input, defaultEvaluationContext) mustEqual dag.LoadLocal(Const(CString("/foo"))(line))(line)
      }
      
      "false" >> {
        val input = Filter(CrossLeftSort,
          dag.LoadLocal(Const(CString("/foo"))(line))(line),
          Const(CBoolean(false))(line))(line)
          
        inlineStatics(input, defaultEvaluationContext) mustEqual Const(CUndefined)(line)
      }
    }
  }
}

object StaticInlinerSpecs extends StaticInlinerSpecs[YId] with test.YIdInstances
