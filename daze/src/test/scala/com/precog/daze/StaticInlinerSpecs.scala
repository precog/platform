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

    "detect and resolve array" >> {
      val line = Line(1, 1, "")

      "wrap" >> {
        val input = Operate(WrapArray, Const(JBool(false))(line))(line)

        inlineStatics(input, defaultEvaluationContext, Set.empty) mustEqual Const(JArray(JBool(false)))(line)
      }

      "deref" >> {
        val input = Join(DerefArray, CrossLeftSort, Const(JArray(JBool(false), JBool(true)))(line), Const(JNum(1))(line))(line)

        inlineStatics(input, defaultEvaluationContext, Set.empty) mustEqual Const(JBool(true))(line)
      }

      "join" >> {
        val input = Join(JoinArray, CrossLeftSort, Const(JArray(JBool(true)))(line), Const(JArray(JBool(false)))(line))(line)

        inlineStatics(input, defaultEvaluationContext, Set.empty) mustEqual Const(JArray(JBool(true), JBool(false)))(line)
      }

      "swap" >> {
        val input = Join(ArraySwap, CrossLeftSort, Const(JArray(JBool(true), JBool(false), JString("TEST")))(line), Const(JNum(1))(line))(line)

        inlineStatics(input, defaultEvaluationContext, Set.empty) mustEqual Const(JArray(JBool(false), JBool(true), JString("TEST")))(line)
      }
    }

    "detect and resolve object" >> {
      val line = Line(1, 1, "")

      "wrap" >> {
        val input = Join(WrapObject, CrossLeftSort, Const(JString("k"))(line), Const(JBool(true))(line))(line)

        inlineStatics(input, defaultEvaluationContext, Set.empty) mustEqual Const(JObject("k" -> JBool(true)))(line)
      }

      "deref" >> {
        val input = Join(DerefObject, CrossLeftSort, Const(JObject("k" -> JBool(false)))(line), Const(JString("k"))(line))(line)

        inlineStatics(input, defaultEvaluationContext, Set.empty) mustEqual Const(JBool(false))(line)
      }

      "join" >> {
        val input = Join(JoinObject, CrossLeftSort, Const(JObject("k" -> JBool(true)))(line), Const(JObject("l" -> JBool(false)))(line))(line)

        inlineStatics(input, defaultEvaluationContext, Set.empty) mustEqual Const(JObject("k" -> JBool(true), "l" -> JBool(false)))(line)
      }
    }
  }
}

object StaticInlinerSpecs extends StaticInlinerSpecs[YId] with test.YIdInstances
