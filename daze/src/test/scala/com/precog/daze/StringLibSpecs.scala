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
package com.precog.daze

import org.specs2.mutable._

import com.precog.common._
import com.precog.yggdrasil._
import com.precog.common.Path
import scalaz._
import scalaz.std.list._

import com.precog.util.IdGen

trait StringLibSpecs[M[+_]] extends Specification
    with EvaluatorTestSupport[M]
    with LongIdMemoryDatasetConsumer[M] { self =>
      
  import Function._
  
  import dag._
  import instructions._
  import library._

  private val line = Line(1, 1, "")
  private def homStrings(line: Line) = dag.LoadLocal(Const(CString("/hom/strings"))(line))(line)
  private def hetStrings(line: Line) = dag.LoadLocal(Const(CString("/het/strings"))(line))(line)

  val testAPIKey = "testAPIKey"

  def testEval(graph: DepGraph): Set[SEvent] = {
    consumeEval(testAPIKey, graph, Path.Root) match {
      case Success(results) => results
      case Failure(error) => throw error
    }
  }

  def op1Input(op: Op1, loadFrom: Line => dag.LoadLocal) = {
    dag.Operate(BuiltInFunction1Op(op), loadFrom(line))(line)
  }

  def op2Input(op: Op2, const: RValue, loadFrom: Line => dag.LoadLocal) = {
    Join(BuiltInFunction2Op(op), CrossLeftSort, loadFrom(line), Const(const)(line))(line)
  }
        
  "for homogeneous sets, the appropriate string function" should {
    "determine length" in {
      val input = op1Input(library.length, homStrings)
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(6, 12, 16, 26, 42, 0)
    }  
    "determine trim" in {
      val input = op1Input(trim, homStrings)
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("quirky", "solstice + 7", "Monkey: [Brains]", """("alpha", "beta", "gamma")""", "Whitespace       is   awesome  !!!1!!", "")
    }  
    "determine toUpperCase" in {
      val input = op1Input(toUpperCase, homStrings)
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("QUIRKY", "SOLSTICE + 7", "MONKEY: [BRAINS]", """("ALPHA", "BETA", "GAMMA")""", "  WHITESPACE       IS   AWESOME  !!!1!!   ", "")
    }  
    "determine toLowerCase" in {
      val input = op1Input(toLowerCase, homStrings)
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("quirky", "solstice + 7", "monkey: [brains]", """("alpha", "beta", "gamma")""", "  whitespace       is   awesome  !!!1!!   ", "")
    }  
    "determine isEmpty" in {
      val input = op1Input(isEmpty, homStrings)
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SBoolean(d)) if ids.length == 1 => d
      }
      
      result2 must contain(true, false)
    }  
    "determine intern" in {
      val input = op1Input(intern, homStrings)
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("quirky", "solstice + 7", "Monkey: [Brains]", """("alpha", "beta", "gamma")""", "  Whitespace       is   awesome  !!!1!!   ", "")
    }  

    "determine codePointAt with valid integer" in {
      val input = op2Input(codePointAt, CLong(7), homStrings)
        
      val result = testEval(input)
      
      result must haveSize(4)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(101, 32, 34, 115)
    }
    "determine codePointAt with invalid integer" in {
      val input = op2Input(codePointAt, CNum(7.5), homStrings)
        
      val result = testEval(input)
      
      result must haveSize(0)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain()
    } 
    "determine startsWith" in {
      val input = op2Input(startsWith, CString("s"), homStrings)
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SBoolean(d)) if ids.length == 1 => d
      }
      
      result2 must contain(true, false)
    }  
    "determine lastIndexOf" in {
      val input = op2Input(lastIndexOf, CString("s"), homStrings)
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(-1, 3, 14, 27)
    }
    "determine concat" in {
      val input = op2Input(concat, CString("7"), homStrings)
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("quirky7", "solstice + 77", "Monkey: [Brains]7", """("alpha", "beta", "gamma")7""", "  Whitespace       is   awesome  !!!1!!   7", "7")
    }
    "determine endsWith" in {
      val input = op2Input(endsWith, CString("y"), homStrings)
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SBoolean(d)) if ids.length == 1 => d
      }
      
      result2 must contain(true, false)
    }
    "determine codePointBefore with valid integer" in {
      val input = op2Input(codePointBefore, CLong(7), homStrings)
        
      val result = testEval(input)
      
      result must haveSize(4)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(99, 58, 97, 101)
    }
    "determine codePointBefore with invalid integer" in {
      val input = op2Input(codePointBefore, CNum(7.5), homStrings)
        
      val result = testEval(input)
      
      result must haveSize(0)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain()
    }
    "determine takeLeft with valid integer" in {
      val input = op2Input(takeLeft, CLong(8), homStrings)
        
      val result = testEval(input)
      
      result must haveSize(6)
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("", "quirky", "solstice", "Monkey: ", "(\"alpha\"", "  Whites").only
    }
    "determine takeRight with valid integer" in {
      val input = op2Input(takeRight, CLong(8), homStrings)
        
      val result = testEval(input)
      
      result must haveSize(6)
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("", "quirky", "tice + 7", "[Brains]", "\"gamma\")", "!!1!!   ").only
    }
    "determine dropLeft with valid integer" in {
      val input = op2Input(dropLeft, CLong(8), homStrings)

      val result = testEval(input)
      
      result must haveSize(6)
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("", " + 7", "[Brains]", ", \"beta\", \"gamma\")", "pace       is   awesome  !!!1!!   ").only
    }
    "determine dropRight with valid integer" in {
      val input = op2Input(dropRight, CLong(8), homStrings)
      val result = testEval(input)
      
      result must haveSize(6)
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("", "sols", "Monkey: ", "(\"alpha\", \"beta\", ", "  Whitespace       is   awesome  !").only
    }
    "determine takeLeft with invalid integer" in {
      val input = op2Input(takeLeft, CNum(7.5), homStrings)
      testEval(input) must haveSize(0)
    }
    "determine takeRight with invalid integer" in {
      val input = op2Input(takeRight, CNum(7.5), homStrings)
      testEval(input) must haveSize(0)
    }
    "determine dropLeft with invalid integer" in {
      val input = op2Input(dropLeft, CNum(7.5), homStrings)
      testEval(input) must haveSize(0)
    }
    "determine dropRight with invalid integer" in {
      val input = op2Input(dropRight, CNum(7.5), homStrings)
      testEval(input) must haveSize(0)
    }
    "determine matches" in {
      val input = op2Input(matches, CString("quirky"), homStrings) //todo put regex here!
      
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SBoolean(d)) if ids.length == 1 => d
      }
      
      result2 must contain(true, false)
    }
    "determine matches with capture" in {
      val input = op2Input(regexMatch, CString(""".*(e)[^a]*(a)?[^a]*.*"""), homStrings)
      
      val result = testEval(input)
      result must haveSize(4)
      
      val result2 = result collect {
        case (ids, SArray(vec)) if ids.length == 1 => vec
      }
      
      result2 must contain(Vector(SString("e"), SString("a")), Vector(SString("e"), SString("")))
    }
    "determine compareTo" in {
      val input = op2Input(compareTo, CString("quirky"), homStrings) //todo put regex here!
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(0, 2, -36, -73, -81, -6)
    }
    "determine compareToIgnoreCase" in {
      val input = Join(BuiltInFunction2Op(compareToIgnoreCase), CrossLeftSort,
        dag.LoadLocal(Const(CString("/hom/strings"))(line))(line),
        Const(CString("QUIRKY"))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(0, 2, -4, -73, -81, -6)
    }
    "determine compare" in {
      val input = op2Input(compare, CString("quirky"), homStrings) //todo put regex here!
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(0, 2, -36, -73, -81, -6)
    }
    "determine compareIgnoreCase" in {
      val input = Join(BuiltInFunction2Op(compareIgnoreCase), CrossLeftSort,
        dag.LoadLocal(Const(CString("/hom/strings"))(line))(line),
        Const(CString("QUIRKY"))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(0, 2, -4, -73, -81, -6)
    }
    "determine equals" in {
      val input = Join(BuiltInFunction2Op(library.equals), CrossLeftSort,
        dag.LoadLocal(Const(CString("/hom/strings"))(line))(line),
        Const(CString("quirky"))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SBoolean(d)) if ids.length == 1 => d
      }
      
      result2 must contain(true, false)
    }
    "determine indexOf" in {
      val input = Join(BuiltInFunction2Op(indexOf), CrossLeftSort,
        dag.LoadLocal(Const(CString("/hom/strings"))(line))(line),
        Const(CString("e"))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(-1, 7, 4, 12, 6)
    }
    "determine equalsIgnoreCase" in {
      val input = Join(BuiltInFunction2Op(equalsIgnoreCase), CrossLeftSort,
        dag.LoadLocal(Const(CString("/hom/strings"))(line))(line), 
        Const(CString("QUIRKY"))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SBoolean(d)) if ids.length == 1 => d
      }
      
      result2 must contain(true, false)
    }
  }

  "for heterogeneous sets, the appropriate string function" should {
    "determine length" in {
      val input = dag.Operate(BuiltInFunction1Op(library.length),
        dag.LoadLocal(Const(CString("/het/strings"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(6, 12, 16, 26, 42, 0)
    }  
    "determine trim" in {
      val input = dag.Operate(BuiltInFunction1Op(trim),
        dag.LoadLocal(Const(CString("/het/strings"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("quirky", "solstice + 7", "Monkey: [Brains]", """("alpha", "beta", "gamma")""", "Whitespace       is   awesome  !!!1!!", "")
    }  
    "determine toUpperCase" in {
      val input = dag.Operate(BuiltInFunction1Op(toUpperCase),
        dag.LoadLocal(Const(CString("/het/strings"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("QUIRKY", "SOLSTICE + 7", "MONKEY: [BRAINS]", """("ALPHA", "BETA", "GAMMA")""", "  WHITESPACE       IS   AWESOME  !!!1!!   ", "")
    }  
    "determine toLowerCase" in {
      val input = dag.Operate(BuiltInFunction1Op(toLowerCase),
        dag.LoadLocal(Const(CString("/het/strings"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("quirky", "solstice + 7", "monkey: [brains]", """("alpha", "beta", "gamma")""", "  whitespace       is   awesome  !!!1!!   ", "")
    }  
    "determine isEmpty" in {
      val input = dag.Operate(BuiltInFunction1Op(isEmpty),
        dag.LoadLocal(Const(CString("/het/strings"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SBoolean(d)) if ids.length == 1 => d
      }
      
      result2 must contain(true, false)
    }  
    "determine intern" in {
      val input = dag.Operate(BuiltInFunction1Op(intern),
        dag.LoadLocal(Const(CString("/het/strings"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("quirky", "solstice + 7", "Monkey: [Brains]", """("alpha", "beta", "gamma")""", "  Whitespace       is   awesome  !!!1!!   ", "")
    }  

    "determine codePointAt with valid integer" in {
      val input = Join(BuiltInFunction2Op(codePointAt), CrossLeftSort,
        dag.LoadLocal(Const(CString("/het/strings"))(line))(line),
        Const(CLong(7))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(4)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(101, 32, 34, 115)
    }
    "determine codePointAt with invalid integer" in {
      val input = Join(BuiltInFunction2Op(codePointAt), CrossLeftSort,
        dag.LoadLocal(Const(CString("/het/strings"))(line))(line),
        Const(CNum(7.5))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(0)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain()
    }
    "determine startsWith" in {
      val input = Join(BuiltInFunction2Op(startsWith), CrossLeftSort,
        dag.LoadLocal(Const(CString("/het/strings"))(line))(line),
        Const(CString("s"))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SBoolean(d)) if ids.length == 1 => d
      }
      
      result2 must contain(true, false)
    }
    "determine lastIndexOf" in {
      val input = Join(BuiltInFunction2Op(lastIndexOf), CrossLeftSort,
        dag.LoadLocal(Const(CString("/het/strings"))(line))(line),
        Const(CString("s"))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(-1, 3, 14, 27)
    }
    "determine concat" in {
      val input = Join(BuiltInFunction2Op(concat), CrossLeftSort,
        dag.LoadLocal(Const(CString("/het/strings"))(line))(line),
        Const(CString("7"))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("quirky7", "solstice + 77", "Monkey: [Brains]7", """("alpha", "beta", "gamma")7""", "  Whitespace       is   awesome  !!!1!!   7", "7")
    }
    "determine endsWith" in {
      val input = Join(BuiltInFunction2Op(endsWith), CrossLeftSort,
        dag.LoadLocal(Const(CString("/het/strings"))(line))(line),
        Const(CString("y"))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SBoolean(d)) if ids.length == 1 => d
      }
      
      result2 must contain(true, false)
    }
    "determine codePointBefore with valid integer" in {
      val input = Join(BuiltInFunction2Op(codePointBefore), CrossLeftSort,
        dag.LoadLocal(Const(CString("/het/strings"))(line))(line),
        Const(CLong(7))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(4)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(99, 58, 97, 101)
    }
    "determine codePointBefore with invalid integer" in {
      val input = Join(BuiltInFunction2Op(codePointBefore), CrossLeftSort,
        dag.LoadLocal(Const(CString("/het/strings"))(line))(line),
        Const(CNum(7.5))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(0)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain()
    }
    "determine takeLeft with valid integer" in {
      val input = Join(BuiltInFunction2Op(takeLeft), CrossLeftSort,
        dag.LoadLocal(Const(CString("/het/strings"))(line))(line),
        Const(CLong(8))(line))(line)

      val result = testEval(input)
      
      result must haveSize(6)
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("", "quirky", "solstice", "Monkey: ", "(\"alpha\"", "  Whites").only
    }
    "determine takeRight with valid integer" in {
      val input = Join(BuiltInFunction2Op(takeRight), CrossLeftSort,
        dag.LoadLocal(Const(CString("/het/strings"))(line))(line),
        Const(CLong(8))(line))(line)

      val result = testEval(input)
      
      result must haveSize(6)
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("", "quirky", "tice + 7", "[Brains]", "\"gamma\")", "!!1!!   ").only
    }
    "determine dropLeft with valid integer" in {
      val input = Join(BuiltInFunction2Op(dropLeft), CrossLeftSort,
        dag.LoadLocal(Const(CString("/het/strings"))(line))(line),
        Const(CLong(8))(line))(line)

      val result = testEval(input)
      
      result must haveSize(6)
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("", " + 7", "[Brains]", ", \"beta\", \"gamma\")", "pace       is   awesome  !!!1!!   ").only
    }
    "determine dropRight with valid integer" in {
      val input = Join(BuiltInFunction2Op(dropRight), CrossLeftSort,
        dag.LoadLocal(Const(CString("/het/strings"))(line))(line),
        Const(CLong(8))(line))(line)
      
      val result = testEval(input)
      
      result must haveSize(6)
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("", "sols", "Monkey: ", "(\"alpha\", \"beta\", ", "  Whitespace       is   awesome  !").only
    }
    "determine takeLeft with invalid integer" in {
      val input = op2Input(takeLeft, CNum(7.5), hetStrings)
      testEval(input) must haveSize(0)
    }
    "determine takeRight with invalid integer" in {
      val input = op2Input(takeRight, CNum(7.5), hetStrings)
      testEval(input) must haveSize(0)
    }
    "determine dropLeft with invalid integer" in {
      val input = op2Input(dropLeft, CNum(7.5), hetStrings)
      testEval(input) must haveSize(0)
    }
    "determine dropRight with invalid integer" in {
      val input = op2Input(dropRight, CNum(7.5), hetStrings)
      testEval(input) must haveSize(0)
    }
    "determine matches" in {
      val input = op2Input(matches, CString("quirky"), hetStrings) //todo put regex here!
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SBoolean(d)) if ids.length == 1 => d
      }
      
      result2 must contain(true, false)
    }
    "determine compareTo" in {
      val input = op2Input(compareTo, CString("quirky"), hetStrings) 
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(0, 2, -36, -73, -81, -6)
    }
    "determine compareToIgnoreCase" in {
      val input = op2Input(compareToIgnoreCase, CString("QUIRKY"), hetStrings) 
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(0, 2, -4, -73, -81, -6)
    }
    "determine compare" in {
      val input = op2Input(compare, CString("quirky"), hetStrings) 
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(0, 2, -36, -73, -81, -6)
    }
    "determine compareIgnoreCase" in {
      val input = op2Input(compareIgnoreCase, CString("QUIRKY"), hetStrings) 
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(0, 2, -4, -73, -81, -6)
    }
    "determine equals" in {
      val input = op2Input(library.equals, CString("quirky"), hetStrings) 
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SBoolean(d)) if ids.length == 1 => d
      }
      
      result2 must contain(true, false)
    }
    "determine indexOf" in {
      val input = Join(BuiltInFunction2Op(indexOf), CrossLeftSort,
        dag.LoadLocal(Const(CString("/het/strings"))(line))(line),
        Const(CString("e"))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(-1, 7, 4, 12, 6)
    }
    "determine equalsIgnoreCase" in {
      val input = Join(BuiltInFunction2Op(equalsIgnoreCase), CrossLeftSort,
        dag.LoadLocal(Const(CString("/het/strings"))(line))(line), 
        Const(CString("QUIRKY"))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SBoolean(d)) if ids.length == 1 => d
      }
      
      result2 must contain(true, false)
    }
  }

  "parseNum" should {
    "handle valid and invalid inputs" in {
      val input = dag.Operate(BuiltInFunction1Op(parseNum),
        dag.LoadLocal(Const(CString("/het/stringNums"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(8)

      val ns = result.toList.collect {
        case (_, SDecimal(n)) => n
      }

      ns must contain(
        BigDecimal("42"),
        BigDecimal("42.0"),
        BigDecimal("42.123"),
        BigDecimal("-666"),
        BigDecimal("2e3"),
        BigDecimal("0e9"),
        BigDecimal("2.23532235235235353252352343636953295923"),
        BigDecimal("1.2e3")
      ).only
    }
  }

  "toString" should {
    "convert values to strings" in {
      val input = dag.Operate(BuiltInFunction1Op(numToString),
        dag.LoadLocal(Const(CString("/het/random"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(6)

      val ss = result.toList.collect {
        case (_, SString(s)) => s
      }

      ss must contain(
        "4",
        "3",
        "4",
        "4",
        "3",
        "4"
      ).only
    }
    
    "trim the trailing '.0' in round double conversion" in {
      val input = dag.Operate(BuiltInFunction1Op(numToString),
        dag.Operate(BuiltInFunction1Op(round),
          Const(CDouble(3.14))(line))(line))(line)

      val resultE = testEval(input)
      resultE must haveSize(1)
      
      val result = resultE.collect {
        case (_, SString(s)) => s
      }
      
      result must contain("3")
    }
  }

  "split" should {

    val o = scala.math.Ordering.by[(Long, _), Long](_._1)

    def mogrify(result: Set[(Vector[Long], SValue)]): List[Vector[String]] =
      result.toList.map {
        case (Vector(n), SArray(elems)) => (n, elems)
      }.sorted(o).map(_._2.map { case SString(s) => s })

    def mktree(f: Op2, path: String, sep: String) =
      Join(BuiltInFunction2Op(f), CrossLeftSort,
        dag.LoadLocal(Const(CString(path))(line))(line),
        Const(CString(sep))(line))(line)

    def tester(f: Op2, path: String, sep: String) =
      mogrify(testEval(mktree(f, path, sep)))
    
    val commaSplitString2 = List(
      Vector("this", "is", "delimited"),
      Vector("this is a string"),
      Vector(""),
      Vector("also", "delmited"),
      Vector("", "starts", "with", "comma"),
      Vector("ends", "with", "comma", ""),
      Vector("lots", "", "", "", "of", "", "", "", "commas"),
      Vector("", "", "", "" , "", "", "", ""),
      Vector("", ""),
      Vector("", "", "crazy", "", ""),
      Vector("something", "basically", "reasonable")
    )

    "split heterogenous data on ," in {
      tester(split, "/het/strings2", ",") must_== commaSplitString2
    }

    "splitRegex heterogenous data on ," in {
      tester(splitRegex, "/het/strings2", ",") must_== commaSplitString2
    }

    "splitRegex heterogenous data on ,,+" in {
      tester(splitRegex, "/het/strings2", ",,+") must_== List(
        Vector("this,is,delimited"),
        Vector("this is a string"),
        Vector(""),
        Vector("also,delmited"),
        Vector(",starts,with,comma"),
        Vector("ends,with,comma,"),
        Vector("lots", "of", "commas"),
        Vector("", ""),
        Vector(","),
        Vector("", "crazy", ""),
        Vector("something,basically,reasonable")
      )
    }

    "splitRegex heterogenous data on invalid regex" in {
      tester(splitRegex, "/het/strings2", "(99") must_== List()
    }
  }
}

object StringLibSpecs extends StringLibSpecs[test.YId] with test.YIdInstances
