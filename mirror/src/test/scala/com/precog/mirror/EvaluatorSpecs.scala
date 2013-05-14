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
package com.precog.mirror

import org.specs2.matcher._
import org.specs2.mutable._

import blueeyes.json._

object EvaluatorSpecs extends Specification with EvaluatorModule {
  import Function.const
  
  "mirror evaluator" should {
    "evaluate simple arithmetic expressions" >> {
      "add" >> {
        "6 + 7" must evalTo(JNum(13))
      }
      
      "sub" >> {
        "6 - 7" must evalTo(JNum(-1))
      }
      
      "mul" >> {
        "6 * 7" must evalTo(JNum(42))
      }
      
      "div" >> {
        "6 / 2" must evalTo(JNum(3))
      }
      
      "mod" >> {
        "6 % 2" must evalTo(JNum(0))
      }
      
      "pow" >> {
        "6 ^ 2" must evalTo(JNum(36))
      }
    }
    
    "evalute simple numeric comparisons" >> {
      "lt" >> {
        "6 < 7" must evalTo(JTrue)
        "7 < 6" must evalTo(JFalse)
        "6 < 6" must evalTo(JFalse)
      }
      
      "lteq" >> {
        "6 <= 7" must evalTo(JTrue)
        "7 <= 6" must evalTo(JFalse)
        "6 <= 6" must evalTo(JTrue)
      }
      
      "gt" >> {
        "6 > 7" must evalTo(JFalse)
        "7 > 6" must evalTo(JTrue)
        "6 > 6" must evalTo(JFalse)
      }
      
      "gteq" >> {
        "6 >= 7" must evalTo(JFalse)
        "7 >= 6" must evalTo(JTrue)
        "6 >= 6" must evalTo(JTrue)
      }
      
      "eq" >> {
        "6 = 7" must evalTo(JFalse)
        "7 = 6" must evalTo(JFalse)
        "6 = 6" must evalTo(JTrue)
      }
      
      "noteq" >> {
        "6 != 7" must evalTo(JTrue)
        "7 != 6" must evalTo(JTrue)
        "6 != 6" must evalTo(JFalse)
      }
      
      "neg" >> {
        "neg 5" must evalTo(JNum(-5))
      }
    }
    
    "evaluate the boolean combinators" >> {
      "and" >> {
        "true & true" must evalTo(JTrue)
        "true & false" must evalTo(JFalse)
        "false & true" must evalTo(JFalse)
        "false & false" must evalTo(JFalse)
      }
      
      "or" >> {
        "true | true" must evalTo(JTrue)
        "true | false" must evalTo(JTrue)
        "false | true" must evalTo(JTrue)
        "false | false" must evalTo(JFalse)
      }
      
      "comp" >> {
        "!true" must evalTo(JFalse)
        "!false" must evalTo(JTrue)
      }
    }
  }
  
  private def evalTo(expect: JValue*): Matcher[String] = {
    def doEval(q: String) =
      eval(compileSingle(q))(const(Seq.empty))
    
    def inner(q: String): Boolean =
      expect == doEval(q)
    
    def message(q: String): String = {
      val actual = eval(compileSingle(q))(const(Seq.empty))
      
      "evaluates to [%s], not [%s]".format(
        actual map { _.renderCompact } mkString ",",
        expect map { _.renderCompact } mkString ",")
    }
    
    (inner _, message _)
  }
  
  private def compileSingle(str: String): Expr = {
    val forest = compile(str) filter { _.errors.isEmpty }
    forest must haveSize(1)
    forest.head
  }
}
