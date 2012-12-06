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
      
      inlineStatics(input) mustEqual expected
    }
    
    "detect and resolve operations at depth" in {
      val line = Line(0, "")
      
      val input = Join(line, Add, CrossLeftSort,
        Const(line, CLong(42)),
        Join(line, Mul, CrossRightSort,
          Const(line, CDouble(3.14)),
          Const(line, CLong(2))))
        
      val expected = Const(line, CNum(48.28))
      
      inlineStatics(input) mustEqual expected
    }
    
    "produce CUndefined in cases where the operation is undefined" in {
      val line = Line(0, "")
      
      val input = Join(line, Div, CrossLeftSort,
        Const(line, CLong(42)),
        Const(line, CLong(0)))
        
      val expected = Const(line, CUndefined)
      
      inlineStatics(input) mustEqual expected
    }
    
    "propagate through static computations CUndefined when produced at depth" in {
      val line = Line(0, "")
      
      val input = Join(line, Add, CrossLeftSort,
        Const(line, CLong(42)),
        Join(line, Div, CrossRightSort,
          Const(line, CDouble(3.14)),
          Const(line, CLong(0))))
        
      val expected = Const(line, CUndefined)
      
      inlineStatics(input) mustEqual expected
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
        
        inlineStatics(input) mustEqual expected
      }
      
      "right" >> {
        val input = Join(line, Add, CrossLeftSort,
          Join(line, Div, CrossRightSort,
            Const(line, CDouble(3.14)),
            Const(line, CLong(0))),
          dag.LoadLocal(line, Const(line, CString("/foo"))))
          
        val expected = Const(line, CUndefined)
        
        inlineStatics(input) mustEqual expected
      }
    }
    
    "reduce filters with static RHS" >> {
      val line = Line(0, "")
      
      "true" >> {
        val input = Filter(line, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("/foo"))),
          Const(line, CBoolean(true)))
          
        inlineStatics(input) mustEqual dag.LoadLocal(line, Const(line, CString("/foo")))
      }
      
      "false" >> {
        val input = Filter(line, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("/foo"))),
          Const(line, CBoolean(false)))
          
        inlineStatics(input) mustEqual Const(line, CUndefined)
      }
    }
  }
}

object StaticInlinerSpecs extends StaticInlinerSpecs[YId] with test.YIdInstances
