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
    with MemoryDatasetConsumer[M] { self =>
      
  import Function._
  
  import dag._
  import instructions._

  val testAPIKey = "testAPIKey"

  def testEval(graph: DepGraph): Set[SEvent] = withContext { ctx =>
    consumeEval(testAPIKey, graph, ctx,Path.Root) match {
      case Success(results) => results
      case Failure(error) => throw error
    }
  }

  "for homogeneous sets, the appropriate string function" should {
    "determine length" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(length),
        dag.LoadLocal(line, Const(line, CString("/hom/strings"))))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(6, 12, 16, 26, 42, 0)
    }  
    "determine trim" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(trim),
        dag.LoadLocal(line, Const(line, CString("/hom/strings"))))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("quirky", "solstice + 7", "Monkey: [Brains]", """("alpha", "beta", "gamma")""", "Whitespace       is   awesome  !!!1!!", "")
    }  
    "determine toUpperCase" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(toUpperCase),
        dag.LoadLocal(line, Const(line, CString("/hom/strings"))))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("QUIRKY", "SOLSTICE + 7", "MONKEY: [BRAINS]", """("ALPHA", "BETA", "GAMMA")""", "  WHITESPACE       IS   AWESOME  !!!1!!   ", "")
    }  
    "determine toLowerCase" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(toLowerCase),
        dag.LoadLocal(line, Const(line, CString("/hom/strings"))))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("quirky", "solstice + 7", "monkey: [brains]", """("alpha", "beta", "gamma")""", "  whitespace       is   awesome  !!!1!!   ", "")
    }  
    "determine isEmpty" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(isEmpty),
        dag.LoadLocal(line, Const(line, CString("/hom/strings"))))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SBoolean(d)) if ids.length == 1 => d
      }
      
      result2 must contain(true, false)
    }  
    "determine intern" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(intern),
        dag.LoadLocal(line, Const(line, CString("/hom/strings"))))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("quirky", "solstice + 7", "Monkey: [Brains]", """("alpha", "beta", "gamma")""", "  Whitespace       is   awesome  !!!1!!   ", "")
    }  

    "determine codePointAt with valid integer" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(codePointAt), CrossLeftSort,
        dag.LoadLocal(line, Const(line, CString("/hom/strings"))),
        Const(line, CLong(7)))
        
      val result = testEval(input)
      
      result must haveSize(4)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(101, 32, 34, 115)
    }
    "determine codePointAt with invalid integer" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(codePointAt), CrossLeftSort,
        dag.LoadLocal(line, Const(line, CString("/hom/strings"))),
        Const(line, CDouble(7.5)))
        
      val result = testEval(input)
      
      result must haveSize(0)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain()
    } 
    "determine startsWith" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(startsWith), CrossLeftSort,
        dag.LoadLocal(line, Const(line, CString("/hom/strings"))),
        Const(line, CString("s")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SBoolean(d)) if ids.length == 1 => d
      }
      
      result2 must contain(true, false)
    }  
    "determine lastIndexOf" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(lastIndexOf), CrossLeftSort,
        dag.LoadLocal(line, Const(line, CString("/hom/strings"))),
        Const(line, CString("s")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(-1, 3, 14, 27)
    }
    "determine concat" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(concat), CrossLeftSort,
        dag.LoadLocal(line, Const(line, CString("/hom/strings"))),
        Const(line, CString("7")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("quirky7", "solstice + 77", "Monkey: [Brains]7", """("alpha", "beta", "gamma")7""", "  Whitespace       is   awesome  !!!1!!   7", "7")
    }
    "determine endsWith" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(endsWith), CrossLeftSort,
        dag.LoadLocal(line, Const(line, CString("/hom/strings"))),
        Const(line, CString("y")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SBoolean(d)) if ids.length == 1 => d
      }
      
      result2 must contain(true, false)
    }
    "determine codePointBefore with valid integer" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(codePointBefore), CrossLeftSort,
        dag.LoadLocal(line, Const(line, CString("/hom/strings"))),
        Const(line, CLong(7)))
        
      val result = testEval(input)
      
      result must haveSize(4)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(99, 58, 97, 101)
    }
    "determine codePointBefore with invalid integer" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(codePointBefore), CrossLeftSort,
        dag.LoadLocal(line, Const(line, CString("/hom/strings"))),
        Const(line, CDouble(7.5)))
        
      val result = testEval(input)
      
      result must haveSize(0)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain()
    }
    "determine substring with valid integer" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(substring), CrossLeftSort,
        dag.LoadLocal(line, Const(line, CString("/hom/strings"))),
        Const(line, CLong(7)))
        
      val result = testEval(input)
      
      result must haveSize(4)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("e + 7", " [Brains]", """", "beta", "gamma")""", "space       is   awesome  !!!1!!   ")
    }
    "determine substring with invalid integer" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(substring), CrossLeftSort,
        dag.LoadLocal(line, Const(line, CString("/hom/strings"))),
        Const(line, CDouble(7.5)))
        
      val result = testEval(input)
      
      result must haveSize(0)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain()
    }
    "determine matches" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(matches), CrossLeftSort,
        dag.LoadLocal(line, Const(line, CString("/hom/strings"))),
        Const(line, CString("quirky"))) //todo put regex here!
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SBoolean(d)) if ids.length == 1 => d
      }
      
      result2 must contain(true, false)
    }
    "determine compareTo" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(compareTo), CrossLeftSort,
        dag.LoadLocal(line, Const(line, CString("/hom/strings"))),
        Const(line, CString("quirky")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(0, 2, -36, -73, -81, -6)
    }
    "determine compareToIgnoreCase" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(compareToIgnoreCase), CrossLeftSort,
        dag.LoadLocal(line, Const(line, CString("/hom/strings"))),
        Const(line, CString("QUIRKY")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(0, 2, -4, -73, -81, -6)
    }
    "determine equals" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(equals), CrossLeftSort,
        dag.LoadLocal(line, Const(line, CString("/hom/strings"))),
        Const(line, CString("quirky")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SBoolean(d)) if ids.length == 1 => d
      }
      
      result2 must contain(true, false)
    }
    "determine indexOf" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(indexOf), CrossLeftSort,
        dag.LoadLocal(line, Const(line, CString("/hom/strings"))),
        Const(line, CString("e")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(-1, 7, 4, 12, 6)
    }
    "determine equalsIgnoreCase" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(equalsIgnoreCase), CrossLeftSort,
        dag.LoadLocal(line, Const(line, CString("/hom/strings"))), 
        Const(line, CString("QUIRKY")))
        
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
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(length),
        dag.LoadLocal(line, Const(line, CString("/het/strings"))))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(6, 12, 16, 26, 42, 0)
    }  
    "determine trim" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(trim),
        dag.LoadLocal(line, Const(line, CString("/het/strings"))))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("quirky", "solstice + 7", "Monkey: [Brains]", """("alpha", "beta", "gamma")""", "Whitespace       is   awesome  !!!1!!", "")
    }  
    "determine toUpperCase" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(toUpperCase),
        dag.LoadLocal(line, Const(line, CString("/het/strings"))))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("QUIRKY", "SOLSTICE + 7", "MONKEY: [BRAINS]", """("ALPHA", "BETA", "GAMMA")""", "  WHITESPACE       IS   AWESOME  !!!1!!   ", "")
    }  
    "determine toLowerCase" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(toLowerCase),
        dag.LoadLocal(line, Const(line, CString("/het/strings"))))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("quirky", "solstice + 7", "monkey: [brains]", """("alpha", "beta", "gamma")""", "  whitespace       is   awesome  !!!1!!   ", "")
    }  
    "determine isEmpty" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(isEmpty),
        dag.LoadLocal(line, Const(line, CString("/het/strings"))))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SBoolean(d)) if ids.length == 1 => d
      }
      
      result2 must contain(true, false)
    }  
    "determine intern" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(intern),
        dag.LoadLocal(line, Const(line, CString("/het/strings"))))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("quirky", "solstice + 7", "Monkey: [Brains]", """("alpha", "beta", "gamma")""", "  Whitespace       is   awesome  !!!1!!   ", "")
    }  

    "determine codePointAt with valid integer" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(codePointAt), CrossLeftSort,
        dag.LoadLocal(line, Const(line, CString("/het/strings"))),
        Const(line, CLong(7)))
        
      val result = testEval(input)
      
      result must haveSize(4)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(101, 32, 34, 115)
    }
    "determine codePointAt with invalid integer" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(codePointAt), CrossLeftSort,
        dag.LoadLocal(line, Const(line, CString("/het/strings"))),
        Const(line, CDouble(7.5)))
        
      val result = testEval(input)
      
      result must haveSize(0)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain()
    }
    "determine startsWith" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(startsWith), CrossLeftSort,
        dag.LoadLocal(line, Const(line, CString("/het/strings"))),
        Const(line, CString("s")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SBoolean(d)) if ids.length == 1 => d
      }
      
      result2 must contain(true, false)
    }
    "determine lastIndexOf" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(lastIndexOf), CrossLeftSort,
        dag.LoadLocal(line, Const(line, CString("/het/strings"))),
        Const(line, CString("s")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(-1, 3, 14, 27)
    }
    "determine concat" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(concat), CrossLeftSort,
        dag.LoadLocal(line, Const(line, CString("/het/strings"))),
        Const(line, CString("7")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("quirky7", "solstice + 77", "Monkey: [Brains]7", """("alpha", "beta", "gamma")7""", "  Whitespace       is   awesome  !!!1!!   7", "7")
    }
    "determine endsWith" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(endsWith), CrossLeftSort,
        dag.LoadLocal(line, Const(line, CString("/het/strings"))),
        Const(line, CString("y")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SBoolean(d)) if ids.length == 1 => d
      }
      
      result2 must contain(true, false)
    }
    "determine codePointBefore with valid integer" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(codePointBefore), CrossLeftSort,
        dag.LoadLocal(line, Const(line, CString("/het/strings"))),
        Const(line, CLong(7)))
        
      val result = testEval(input)
      
      result must haveSize(4)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(99, 58, 97, 101)
    }
    "determine codePointBefore with invalid integer" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(codePointBefore), CrossLeftSort,
        dag.LoadLocal(line, Const(line, CString("/het/strings"))),
        Const(line, CDouble(7.5)))
        
      val result = testEval(input)
      
      result must haveSize(0)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain()
    }
    "determine substring with valid integer" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(substring), CrossLeftSort,
        dag.LoadLocal(line, Const(line, CString("/het/strings"))),
        Const(line, CLong(7)))
        
      val result = testEval(input)
      
      result must haveSize(4)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("e + 7", " [Brains]", """", "beta", "gamma")""", "space       is   awesome  !!!1!!   ")
    }
    "determine substring with invalid integer" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(substring), CrossLeftSort,
        dag.LoadLocal(line, Const(line, CString("/het/strings"))),
        Const(line, CDouble(7.5)))
        
      val result = testEval(input)
      
      result must haveSize(0)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
        ids.length must_== 1
      }
      
      result2 must contain()
    }
    "determine matches" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(matches), CrossLeftSort,
        dag.LoadLocal(line, Const(line, CString("/het/strings"))),
        Const(line, CString("quirky"))) //todo put regex here!
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SBoolean(d)) if ids.length == 1 => d
      }
      
      result2 must contain(true, false)
    }
    "determine compareTo" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(compareTo), CrossLeftSort,
        dag.LoadLocal(line, Const(line, CString("/het/strings"))),
        Const(line, CString("quirky")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(0, 2, -36, -73, -81, -6)
    }
    "determine compareToIgnoreCase" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(compareToIgnoreCase), CrossLeftSort,
        dag.LoadLocal(line, Const(line, CString("/het/strings"))),
        Const(line, CString("QUIRKY")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(0, 2, -4, -73, -81, -6)
    }
    "determine equals" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(equals), CrossLeftSort,
        dag.LoadLocal(line, Const(line, CString("/het/strings"))),
        Const(line, CString("quirky")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SBoolean(d)) if ids.length == 1 => d
      }
      
      result2 must contain(true, false)
    }
    "determine indexOf" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(indexOf), CrossLeftSort,
        dag.LoadLocal(line, Const(line, CString("/het/strings"))),
        Const(line, CString("e")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(-1, 7, 4, 12, 6)
    }
    "determine equalsIgnoreCase" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(equalsIgnoreCase), CrossLeftSort,
        dag.LoadLocal(line, Const(line, CString("/het/strings"))), 
        Const(line, CString("QUIRKY")))
        
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
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(parseNum),
        dag.LoadLocal(line, Const(line, CString("/het/stringNums"))))

      val result = testEval(input)

      result must haveSize(8)

      val ns = result.map {
        case (Vector(i:Long), SDecimal(n)) => (i, n)
      }.toList.sorted.map {
        case (i, n) => n
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
      )
    }
  }
}

object StringLibSpec extends StringLibSpec[test.YId] with test.YIdInstances
