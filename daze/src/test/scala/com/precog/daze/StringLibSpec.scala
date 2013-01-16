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

import com.precog.yggdrasil._
import com.precog.common.Path
import scalaz._
import scalaz.std.list._

import com.precog.util.IdGen

trait StringLibSpec[M[+_]] extends Specification
    with EvaluatorTestSupport[M]
    with StringLib[M] 
    with LongIdMemoryDatasetConsumer[M] { self =>
      
  import Function._
  
  import dag._
  import instructions._

  private def line = Line(0, "")
  private def homStrings = dag.LoadLocal(line, Const(line, CString("/hom/strings")))
  private def hetStrings = dag.LoadLocal(line, Const(line, CString("/het/strings")))

  val testAPIKey = "testAPIKey"

  def testEval(graph: DepGraph): Set[SEvent] = {
    consumeEval(testAPIKey, graph, Path.Root) match {
      case Success(results) => results
      case Failure(error) => throw error
    }
  }

  "for homogeneous sets, the appropriate string function" should {
    "determine length" in {
      val input = dag.Operate(line, BuiltInFunction1Op(length), homStrings)
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(6, 12, 16, 26, 42, 0)
    }  
    "determine trim" in {
      val input = dag.Operate(line, BuiltInFunction1Op(trim), homStrings)
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("quirky", "solstice + 7", "Monkey: [Brains]", """("alpha", "beta", "gamma")""", "Whitespace       is   awesome  !!!1!!", "")
    }  
    "determine toUpperCase" in {
      val input = dag.Operate(line, BuiltInFunction1Op(toUpperCase), homStrings)
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("QUIRKY", "SOLSTICE + 7", "MONKEY: [BRAINS]", """("ALPHA", "BETA", "GAMMA")""", "  WHITESPACE       IS   AWESOME  !!!1!!   ", "")
    }  
    "determine toLowerCase" in {
      val input = dag.Operate(line, BuiltInFunction1Op(toLowerCase), homStrings)
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("quirky", "solstice + 7", "monkey: [brains]", """("alpha", "beta", "gamma")""", "  whitespace       is   awesome  !!!1!!   ", "")
    }  
    "determine isEmpty" in {
      val input = dag.Operate(line, BuiltInFunction1Op(isEmpty), homStrings)
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SBoolean(d)) if ids.length == 1 => d
      }
      
      result2 must contain(true, false)
    }  
    "determine intern" in {
      val input = dag.Operate(line, BuiltInFunction1Op(intern), homStrings)
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("quirky", "solstice + 7", "Monkey: [Brains]", """("alpha", "beta", "gamma")""", "  Whitespace       is   awesome  !!!1!!   ", "")
    }  

    "determine codePointAt with valid integer" in {
      val input = Join(line, BuiltInFunction2Op(codePointAt), CrossLeftSort, homStrings, Const(line, CLong(7)))
      val result = testEval(input)
      
      result must haveSize(4)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(101, 32, 34, 115)
    }
    "determine codePointAt with invalid integer" in {
      val input = Join(line, BuiltInFunction2Op(codePointAt), CrossLeftSort, homStrings, Const(line, CDouble(7.5)))
      val result = testEval(input)
      
      result must haveSize(0)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain()
    } 
    "determine startsWith" in {
      val input = Join(line, BuiltInFunction2Op(startsWith), CrossLeftSort, homStrings, Const(line, CString("s")))
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SBoolean(d)) if ids.length == 1 => d
      }
      
      result2 must contain(true, false)
    }  
    "determine lastIndexOf" in {
      val input = Join(line, BuiltInFunction2Op(lastIndexOf), CrossLeftSort, homStrings, Const(line, CString("s")))
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(-1, 3, 14, 27)
    }
    "determine concat" in {
      val input = Join(line, BuiltInFunction2Op(concat), CrossLeftSort, homStrings, Const(line, CString("7")))
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("quirky7", "solstice + 77", "Monkey: [Brains]7", """("alpha", "beta", "gamma")7""", "  Whitespace       is   awesome  !!!1!!   7", "7")
    }
    "determine endsWith" in {
      val input = Join(line, BuiltInFunction2Op(endsWith), CrossLeftSort, homStrings, Const(line, CString("y")))
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SBoolean(d)) if ids.length == 1 => d
      }
      
      result2 must contain(true, false)
    }
    "determine codePointBefore with valid integer" in {
      val input = Join(line, BuiltInFunction2Op(codePointBefore), CrossLeftSort, homStrings, Const(line, CLong(7)))
      val result = testEval(input)
      
      result must haveSize(4)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(99, 58, 97, 101)
    }
    "determine codePointBefore with invalid integer" in {
      val input = Join(line, BuiltInFunction2Op(codePointBefore), CrossLeftSort, homStrings, Const(line, CDouble(7.5)))
      val result = testEval(input)
      
      result must haveSize(0)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain()
    }
    "determine takeLeft with valid integer" in {
      val input = Join(line, BuiltInFunction2Op(takeLeft), CrossLeftSort, homStrings, Const(line, CLong(8)))
      val result = testEval(input)
      
      result must haveSize(6)
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("", "quirky", "solstice", "Monkey: ", "(\"alpha\"", "  Whites").only
    }
    "determine takeRight with valid integer" in {
      val input = Join(line, BuiltInFunction2Op(takeRight), CrossLeftSort, homStrings, Const(line, CLong(8)))
      val result = testEval(input)
      
      result must haveSize(6)
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("", "quirky", "tice + 7", "[Brains]", "\"gamma\")", "!!1!!   ").only
    }
    "determine dropLeft with valid integer" in {
      val input = Join(line, BuiltInFunction2Op(dropLeft), CrossLeftSort, homStrings, Const(line, CLong(8)))
      val result = testEval(input)
      
      result must haveSize(6)
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("", " + 7", "[Brains]", ", \"beta\", \"gamma\")", "pace       is   awesome  !!!1!!   ").only
    }
    "determine dropRight with valid integer" in {
      val input = Join(line, BuiltInFunction2Op(dropRight), CrossLeftSort, homStrings, Const(line, CLong(8)))
      val result = testEval(input)
      
      result must haveSize(6)
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("", "sols", "Monkey: ", "(\"alpha\", \"beta\", ", "  Whitespace       is   awesome  !").only
    }
    "determine takeLeft with invalid integer" in {
      val input = Join(line, BuiltInFunction2Op(takeLeft), CrossLeftSort, homStrings, Const(line, CDouble(7.5)))
      testEval(input) must haveSize(0)
    }
    "determine takeRight with invalid integer" in {
      val input = Join(line, BuiltInFunction2Op(takeRight), CrossLeftSort, homStrings, Const(line, CDouble(7.5)))
      testEval(input) must haveSize(0)
    }
    "determine dropLeft with invalid integer" in {
      val input = Join(line, BuiltInFunction2Op(dropLeft), CrossLeftSort, homStrings, Const(line, CDouble(7.5)))
      testEval(input) must haveSize(0)
    }
    "determine dropRight with invalid integer" in {
      val input = Join(line, BuiltInFunction2Op(dropRight), CrossLeftSort, homStrings, Const(line, CDouble(7.5)))
      testEval(input) must haveSize(0)
    }
    "determine matches" in {
      val input = Join(line, BuiltInFunction2Op(matches), CrossLeftSort, homStrings, Const(line, CString("quirky"))) //todo put regex here!
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SBoolean(d)) if ids.length == 1 => d
      }
      
      result2 must contain(true, false)
    }
    "determine compareTo" in {
      val input = Join(line, BuiltInFunction2Op(compareTo), CrossLeftSort, homStrings, Const(line, CString("quirky")))
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(0, 2, -36, -73, -81, -6)
    }
    "determine compareToIgnoreCase" in {
      val input = Join(line, BuiltInFunction2Op(compareToIgnoreCase), CrossLeftSort, homStrings, Const(line, CString("QUIRKY")))
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(0, 2, -4, -73, -81, -6)
    }
    "determine equals" in {
      val input = Join(line, BuiltInFunction2Op(equals), CrossLeftSort, homStrings, Const(line, CString("quirky")))
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SBoolean(d)) if ids.length == 1 => d
      }
      
      result2 must contain(true, false)
    }
    "determine indexOf" in {
      val input = Join(line, BuiltInFunction2Op(indexOf), CrossLeftSort, homStrings, Const(line, CString("e")))
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(-1, 7, 4, 12, 6)
    }
    "determine equalsIgnoreCase" in {
      val input = Join(line, BuiltInFunction2Op(equalsIgnoreCase), CrossLeftSort, homStrings, Const(line, CString("QUIRKY")))
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
      val input = dag.Operate(line, BuiltInFunction1Op(length), hetStrings)
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(6, 12, 16, 26, 42, 0)
    }  
    "determine trim" in {
      val input = dag.Operate(line, BuiltInFunction1Op(trim), hetStrings)
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("quirky", "solstice + 7", "Monkey: [Brains]", """("alpha", "beta", "gamma")""", "Whitespace       is   awesome  !!!1!!", "")
    }  
    "determine toUpperCase" in {
      val input = dag.Operate(line, BuiltInFunction1Op(toUpperCase), hetStrings)
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("QUIRKY", "SOLSTICE + 7", "MONKEY: [BRAINS]", """("ALPHA", "BETA", "GAMMA")""", "  WHITESPACE       IS   AWESOME  !!!1!!   ", "")
    }  
    "determine toLowerCase" in {
      val input = dag.Operate(line, BuiltInFunction1Op(toLowerCase), hetStrings)
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("quirky", "solstice + 7", "monkey: [brains]", """("alpha", "beta", "gamma")""", "  whitespace       is   awesome  !!!1!!   ", "")
    }  
    "determine isEmpty" in {
      val input = dag.Operate(line, BuiltInFunction1Op(isEmpty), hetStrings)
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SBoolean(d)) if ids.length == 1 => d
      }
      
      result2 must contain(true, false)
    }  
    "determine intern" in {
      val input = dag.Operate(line, BuiltInFunction1Op(intern), hetStrings)
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("quirky", "solstice + 7", "Monkey: [Brains]", """("alpha", "beta", "gamma")""", "  Whitespace       is   awesome  !!!1!!   ", "")
    }  

    "determine codePointAt with valid integer" in {
      val input = Join(line, BuiltInFunction2Op(codePointAt), CrossLeftSort, hetStrings, Const(line, CLong(7)))
      val result = testEval(input)
      
      result must haveSize(4)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(101, 32, 34, 115)
    }
    "determine codePointAt with invalid integer" in {
      val input = Join(line, BuiltInFunction2Op(codePointAt), CrossLeftSort, hetStrings, Const(line, CDouble(7.5)))
      val result = testEval(input)
      
      result must haveSize(0)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain()
    }
    "determine startsWith" in {
      val input = Join(line, BuiltInFunction2Op(startsWith), CrossLeftSort, hetStrings, Const(line, CString("s")))
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SBoolean(d)) if ids.length == 1 => d
      }
      
      result2 must contain(true, false)
    }
    "determine lastIndexOf" in {
      val input = Join(line, BuiltInFunction2Op(lastIndexOf), CrossLeftSort, hetStrings, Const(line, CString("s")))
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(-1, 3, 14, 27)
    }
    "determine concat" in {
      val input = Join(line, BuiltInFunction2Op(concat), CrossLeftSort, hetStrings, Const(line, CString("7")))
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("quirky7", "solstice + 77", "Monkey: [Brains]7", """("alpha", "beta", "gamma")7""", "  Whitespace       is   awesome  !!!1!!   7", "7")
    }
    "determine endsWith" in {
      val input = Join(line, BuiltInFunction2Op(endsWith), CrossLeftSort, hetStrings, Const(line, CString("y")))
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SBoolean(d)) if ids.length == 1 => d
      }
      
      result2 must contain(true, false)
    }
    "determine codePointBefore with valid integer" in {
      val input = Join(line, BuiltInFunction2Op(codePointBefore), CrossLeftSort, hetStrings, Const(line, CLong(7)))
      val result = testEval(input)
      
      result must haveSize(4)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(99, 58, 97, 101)
    }
    "determine codePointBefore with invalid integer" in {
      val input = Join(line, BuiltInFunction2Op(codePointBefore), CrossLeftSort, hetStrings, Const(line, CDouble(7.5)))
      val result = testEval(input)
      
      result must haveSize(0)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain()
    }
    "determine takeLeft with valid integer" in {
      val input = Join(line, BuiltInFunction2Op(takeLeft), CrossLeftSort, hetStrings, Const(line, CLong(8)))
      val result = testEval(input)
      
      result must haveSize(6)
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("", "quirky", "solstice", "Monkey: ", "(\"alpha\"", "  Whites").only
    }
    "determine takeRight with valid integer" in {
      val input = Join(line, BuiltInFunction2Op(takeRight), CrossLeftSort, hetStrings, Const(line, CLong(8)))
      val result = testEval(input)
      
      result must haveSize(6)
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("", "quirky", "tice + 7", "[Brains]", "\"gamma\")", "!!1!!   ").only
    }
    "determine dropLeft with valid integer" in {
      val input = Join(line, BuiltInFunction2Op(dropLeft), CrossLeftSort, hetStrings, Const(line, CLong(8)))
      val result = testEval(input)
      
      result must haveSize(6)
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("", " + 7", "[Brains]", ", \"beta\", \"gamma\")", "pace       is   awesome  !!!1!!   ").only
    }
    "determine dropRight with valid integer" in {
      val input = Join(line, BuiltInFunction2Op(dropRight), CrossLeftSort, hetStrings, Const(line, CLong(8)))
      val result = testEval(input)
      
      result must haveSize(6)
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("", "sols", "Monkey: ", "(\"alpha\", \"beta\", ", "  Whitespace       is   awesome  !").only
    }
    "determine takeLeft with invalid integer" in {
      val input = Join(line, BuiltInFunction2Op(takeLeft), CrossLeftSort, hetStrings, Const(line, CDouble(7.5)))
      testEval(input) must haveSize(0)
    }
    "determine takeRight with invalid integer" in {
      val input = Join(line, BuiltInFunction2Op(takeRight), CrossLeftSort, hetStrings, Const(line, CDouble(7.5)))
      testEval(input) must haveSize(0)
    }
    "determine dropLeft with invalid integer" in {
      val input = Join(line, BuiltInFunction2Op(dropLeft), CrossLeftSort, hetStrings, Const(line, CDouble(7.5)))
      testEval(input) must haveSize(0)
    }
    "determine dropRight with invalid integer" in {
      val input = Join(line, BuiltInFunction2Op(dropRight), CrossLeftSort, hetStrings, Const(line, CDouble(7.5)))
      testEval(input) must haveSize(0)
    }
    "determine matches" in {
      val input = Join(line, BuiltInFunction2Op(matches), CrossLeftSort, hetStrings, Const(line, CString("quirky"))) //todo put regex here!
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SBoolean(d)) if ids.length == 1 => d
      }
      
      result2 must contain(true, false)
    }
    "determine compareTo" in {
      val input = Join(line, BuiltInFunction2Op(compareTo), CrossLeftSort, hetStrings, Const(line, CString("quirky")))
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(0, 2, -36, -73, -81, -6)
    }
    "determine compareToIgnoreCase" in {
      val input = Join(line, BuiltInFunction2Op(compareToIgnoreCase), CrossLeftSort, hetStrings, Const(line, CString("QUIRKY")))
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(0, 2, -4, -73, -81, -6)
    }
    "determine equals" in {
      val input = Join(line, BuiltInFunction2Op(equals), CrossLeftSort, hetStrings, Const(line, CString("quirky")))
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SBoolean(d)) if ids.length == 1 => d
      }
      
      result2 must contain(true, false)
    }
    "determine indexOf" in {
      val input = Join(line, BuiltInFunction2Op(indexOf), CrossLeftSort, hetStrings, Const(line, CString("e")))
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(-1, 7, 4, 12, 6)
    }
    "determine equalsIgnoreCase" in {
      val input = Join(line, BuiltInFunction2Op(equalsIgnoreCase), CrossLeftSort, hetStrings, Const(line, CString("QUIRKY")))
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
      val input = dag.Operate(line, BuiltInFunction1Op(parseNum),
        dag.LoadLocal(line, Const(line, CString("/het/stringNums"))))

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
      val input = dag.Operate(line, BuiltInFunction1Op(numToString),
        dag.LoadLocal(line, Const(line, CString("/het/random"))))

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
  }
}

object StringLibSpec extends StringLibSpec[test.YId] with test.YIdInstances
