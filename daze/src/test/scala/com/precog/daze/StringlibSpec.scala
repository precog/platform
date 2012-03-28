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

import memoization._
import com.precog.yggdrasil._

import scalaz._
import scalaz.effect._
import scalaz.iteratee._
import scalaz.std.list._
import Iteratee._

import com.precog.common.VectorCase
import com.precog.util.IdGen

class StringlibSpec extends Specification
  with Evaluator
  with StubOperationsAPI 
  with TestConfigComponent 
  with DiskIterableDatasetMemoizationComponent 
  with Stringlib 
  with MemoryDatasetConsumer { self =>
  override type Dataset[α] = IterableDataset[α]
    

  import Function._
  
  import dag._
  import instructions._

  object ops extends Ops 
  
  val testUID = "testUID"

  def testEval = consumeEval(testUID, _: DepGraph) match {
    case Success(results) => results
    case Failure(error) => throw error
  }

  "all string functions" should {
    "validate input" in todo
    "return failing validations for bad input" in todo
  }

  "for homogeneous sets, the appropriate string function" should {
    "determine length" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(length),
        dag.LoadLocal(line, None, Root(line, PushString("/hom/strings")), Het))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d
      }
      
      result2 must contain(6, 12, 16, 26, 42, 0)
    }  
    "determine trim" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(trim),
        dag.LoadLocal(line, None, Root(line, PushString("/hom/strings")), Het))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (VectorCase(_), SString(d)) => d
      }
      
      result2 must contain("quirky", "solstice + 7", "Monkey: [Brains]", """("alpha", "beta", "gamma")""", "Whitespace       is   awesome  !!!1!!", "")
    }  
    "determine toUpperCase" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(toUpperCase),
        dag.LoadLocal(line, None, Root(line, PushString("/hom/strings")), Het))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (VectorCase(_), SString(d)) => d
      }
      
      result2 must contain("QUIRKY", "SOLSTICE + 7", "MONKEY: [BRAINS]", """("ALPHA", "BETA", "GAMMA")""", "  WHITESPACE       IS   AWESOME  !!!1!!   ", "")
    }  
    "determine toLowerCase" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(toLowerCase),
        dag.LoadLocal(line, None, Root(line, PushString("/hom/strings")), Het))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (VectorCase(_), SString(d)) => d
      }
      
      result2 must contain("quirky", "solstice + 7", "monkey: [brains]", """("alpha", "beta", "gamma")""", "  whitespace       is   awesome  !!!1!!   ", "")
    }  
    "determine isEmpty" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(isEmpty),
        dag.LoadLocal(line, None, Root(line, PushString("/hom/strings")), Het))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (VectorCase(_), SBoolean(d)) => d
      }
      
      result2 must contain(true, false)
    }  
    "determine intern" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(intern),
        dag.LoadLocal(line, None, Root(line, PushString("/hom/strings")), Het))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (VectorCase(_), SString(d)) => d
      }
      
      result2 must contain("quirky", "solstice + 7", "Monkey: [Brains]", """("alpha", "beta", "gamma")""", "  Whitespace       is   awesome  !!!1!!   ", "")
    }  

    "determine codePointAt with valid integer" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(codePointAt)),
        dag.LoadLocal(line, None, Root(line, PushString("/hom/strings")), Het),
        Root(line, PushNum("7")))
        
      val result = testEval(input)
      
      result must haveSize(4)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d
      }
      
      result2 must contain(101, 32, 34, 115)
    }    
    "determine codePointAt with invalid integer" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(codePointAt)),
        dag.LoadLocal(line, None, Root(line, PushString("/hom/strings")), Het),
        Root(line, PushNum("7.5")))
        
      val result = testEval(input)
      
      result must haveSize(0)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d
      }
      
      result2 must contain()
    }  
    "determine startsWith" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(startsWith)),
        dag.LoadLocal(line, None, Root(line, PushString("/hom/strings")), Het),
        Root(line, PushString("s")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (VectorCase(_), SBoolean(d)) => d
      }
      
      result2 must contain(true, false)
    }  
    "determine lastIndexOf" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(lastIndexOf)),
        dag.LoadLocal(line, None, Root(line, PushString("/hom/strings")), Het),
        Root(line, PushString("s")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d
      }
      
      result2 must contain(-1, 3, 14, 27)
    }  
    "determine concat" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(concat)),
        dag.LoadLocal(line, None, Root(line, PushString("/hom/strings")), Het),
        Root(line, PushString("7")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (VectorCase(_), SString(d)) => d
      }
      
      result2 must contain("quirky7", "solstice + 77", "Monkey: [Brains]7", """("alpha", "beta", "gamma")7""", "  Whitespace       is   awesome  !!!1!!   7", "7")
    }  
    "determine endsWith" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(endsWith)),
        dag.LoadLocal(line, None, Root(line, PushString("/hom/strings")), Het),
        Root(line, PushString("y")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (VectorCase(_), SBoolean(d)) => d
      }
      
      result2 must contain(true, false)
    }  
    "determine codePointBefore with valid integer" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(codePointBefore)),
        dag.LoadLocal(line, None, Root(line, PushString("/hom/strings")), Het),
        Root(line, PushNum("7")))
        
      val result = testEval(input)
      
      result must haveSize(4)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d
      }
      
      result2 must contain(99, 58, 97, 101)
    }  
    "determine codePointBefore with invalid integer" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(codePointBefore)),
        dag.LoadLocal(line, None, Root(line, PushString("/hom/strings")), Het),
        Root(line, PushNum("7.5")))
        
      val result = testEval(input)
      
      result must haveSize(0)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d
      }
      
      result2 must contain()
    }  
    "determine substring with valid integer" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(substring)),
        dag.LoadLocal(line, None, Root(line, PushString("/hom/strings")), Het),
        Root(line, PushNum("7")))
        
      val result = testEval(input)
      
      result must haveSize(4)
      
      val result2 = result collect {
        case (VectorCase(_), SString(d)) => d
      }
      
      result2 must contain("e + 7", " [Brains]", """", "beta", "gamma")""", "space       is   awesome  !!!1!!   ")
    }  
    "determine substring with invalid integer" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(substring)),
        dag.LoadLocal(line, None, Root(line, PushString("/hom/strings")), Het),
        Root(line, PushNum("7.5")))
        
      val result = testEval(input)
      
      result must haveSize(0)
      
      val result2 = result collect {
        case (VectorCase(_), SString(d)) => d
      }
      
      result2 must contain()
    }  
    "determine matches" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(matches)),
        dag.LoadLocal(line, None, Root(line, PushString("/hom/strings")), Het),
        Root(line, PushString("quirky"))) //todo put regex here!
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (VectorCase(_), SBoolean(d)) => d
      }
      
      result2 must contain(true, false)
    }  
    "determine compareTo" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(compareTo)),
        dag.LoadLocal(line, None, Root(line, PushString("/hom/strings")), Het),
        Root(line, PushString("quirky")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d
      }
      
      result2 must contain(0, 2, -36, -73, -81, -6)
    }  
    "determine compareToIgnoreCase" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(compareToIgnoreCase)),
        dag.LoadLocal(line, None, Root(line, PushString("/hom/strings")), Het),
        Root(line, PushString("QUIRKY")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d
      }
      
      result2 must contain(0, 2, -4, -73, -81, -6)
    }  
    "determine equals" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(equals)),
        dag.LoadLocal(line, None, Root(line, PushString("/hom/strings")), Het),
        Root(line, PushString("quirky")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (VectorCase(_), SBoolean(d)) => d
      }
      
      result2 must contain(true, false)
    }  
    "determine indexOf" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(indexOf)),
        dag.LoadLocal(line, None, Root(line, PushString("/hom/strings")), Het),
        Root(line, PushString("e")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d
      }
      
      result2 must contain(-1, 7, 4, 12, 6)
    } 
    "determine equalsIgnoreCase" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(equalsIgnoreCase)),
        dag.LoadLocal(line, None, Root(line, PushString("/hom/strings")), Het), 
        Root(line, PushString("QUIRKY")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (VectorCase(_), SBoolean(d)) => d
      }
      
      result2 must contain(true, false)
    }    
  }

  "for heterogeneous sets, the appropriate string function" should {
    "determine length" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(length),
        dag.LoadLocal(line, None, Root(line, PushString("/het/strings")), Het))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d
      }
      
      result2 must contain(6, 12, 16, 26, 42, 0)
    }  
    "determine trim" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(trim),
        dag.LoadLocal(line, None, Root(line, PushString("/het/strings")), Het))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (VectorCase(_), SString(d)) => d
      }
      
      result2 must contain("quirky", "solstice + 7", "Monkey: [Brains]", """("alpha", "beta", "gamma")""", "Whitespace       is   awesome  !!!1!!", "")
    }  
    "determine toUpperCase" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(toUpperCase),
        dag.LoadLocal(line, None, Root(line, PushString("/het/strings")), Het))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (VectorCase(_), SString(d)) => d
      }
      
      result2 must contain("QUIRKY", "SOLSTICE + 7", "MONKEY: [BRAINS]", """("ALPHA", "BETA", "GAMMA")""", "  WHITESPACE       IS   AWESOME  !!!1!!   ", "")
    }  
    "determine toLowerCase" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(toLowerCase),
        dag.LoadLocal(line, None, Root(line, PushString("/het/strings")), Het))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (VectorCase(_), SString(d)) => d
      }
      
      result2 must contain("quirky", "solstice + 7", "monkey: [brains]", """("alpha", "beta", "gamma")""", "  whitespace       is   awesome  !!!1!!   ", "")
    }  
    "determine isEmpty" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(isEmpty),
        dag.LoadLocal(line, None, Root(line, PushString("/het/strings")), Het))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (VectorCase(_), SBoolean(d)) => d
      }
      
      result2 must contain(true, false)
    }  
    "determine intern" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(intern),
        dag.LoadLocal(line, None, Root(line, PushString("/het/strings")), Het))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (VectorCase(_), SString(d)) => d
      }
      
      result2 must contain("quirky", "solstice + 7", "Monkey: [Brains]", """("alpha", "beta", "gamma")""", "  Whitespace       is   awesome  !!!1!!   ", "")
    }  

    "determine codePointAt with valid integer" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(codePointAt)),
        dag.LoadLocal(line, None, Root(line, PushString("/het/strings")), Het),
        Root(line, PushNum("7")))
        
      val result = testEval(input)
      
      result must haveSize(4)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d
      }
      
      result2 must contain(101, 32, 34, 115)
    }    
    "determine codePointAt with invalid integer" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(codePointAt)),
        dag.LoadLocal(line, None, Root(line, PushString("/het/strings")), Het),
        Root(line, PushNum("7.5")))
        
      val result = testEval(input)
      
      result must haveSize(0)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d
      }
      
      result2 must contain()
    }  
    "determine startsWith" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(startsWith)),
        dag.LoadLocal(line, None, Root(line, PushString("/het/strings")), Het),
        Root(line, PushString("s")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (VectorCase(_), SBoolean(d)) => d
      }
      
      result2 must contain(true, false)
    }  
    "determine lastIndexOf" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(lastIndexOf)),
        dag.LoadLocal(line, None, Root(line, PushString("/het/strings")), Het),
        Root(line, PushString("s")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d
      }
      
      result2 must contain(-1, 3, 14, 27)
    }  
    "determine concat" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(concat)),
        dag.LoadLocal(line, None, Root(line, PushString("/het/strings")), Het),
        Root(line, PushString("7")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (VectorCase(_), SString(d)) => d
      }
      
      result2 must contain("quirky7", "solstice + 77", "Monkey: [Brains]7", """("alpha", "beta", "gamma")7""", "  Whitespace       is   awesome  !!!1!!   7", "7")
    }  
    "determine endsWith" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(endsWith)),
        dag.LoadLocal(line, None, Root(line, PushString("/het/strings")), Het),
        Root(line, PushString("y")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (VectorCase(_), SBoolean(d)) => d
      }
      
      result2 must contain(true, false)
    }  
    "determine codePointBefore with valid integer" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(codePointBefore)),
        dag.LoadLocal(line, None, Root(line, PushString("/het/strings")), Het),
        Root(line, PushNum("7")))
        
      val result = testEval(input)
      
      result must haveSize(4)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d
      }
      
      result2 must contain(99, 58, 97, 101)
    }  
    "determine codePointBefore with invalid integer" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(codePointBefore)),
        dag.LoadLocal(line, None, Root(line, PushString("/het/strings")), Het),
        Root(line, PushNum("7.5")))
        
      val result = testEval(input)
      
      result must haveSize(0)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d
      }
      
      result2 must contain()
    }  
    "determine substring with valid integer" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(substring)),
        dag.LoadLocal(line, None, Root(line, PushString("/het/strings")), Het),
        Root(line, PushNum("7")))
        
      val result = testEval(input)
      
      result must haveSize(4)
      
      val result2 = result collect {
        case (VectorCase(_), SString(d)) => d
      }
      
      result2 must contain("e + 7", " [Brains]", """", "beta", "gamma")""", "space       is   awesome  !!!1!!   ")
    }  
    "determine substring with invalid integer" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(substring)),
        dag.LoadLocal(line, None, Root(line, PushString("/het/strings")), Het),
        Root(line, PushNum("7.5")))
        
      val result = testEval(input)
      
      result must haveSize(0)
      
      val result2 = result collect {
        case (VectorCase(_), SString(d)) => d
      }
      
      result2 must contain()
    }  
    "determine matches" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(matches)),
        dag.LoadLocal(line, None, Root(line, PushString("/het/strings")), Het),
        Root(line, PushString("quirky"))) //todo put regex here!
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (VectorCase(_), SBoolean(d)) => d
      }
      
      result2 must contain(true, false)
    }  
    "determine compareTo" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(compareTo)),
        dag.LoadLocal(line, None, Root(line, PushString("/het/strings")), Het),
        Root(line, PushString("quirky")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d
      }
      
      result2 must contain(0, 2, -36, -73, -81, -6)
    }  
    "determine compareToIgnoreCase" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(compareToIgnoreCase)),
        dag.LoadLocal(line, None, Root(line, PushString("/het/strings")), Het),
        Root(line, PushString("QUIRKY")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d
      }
      
      result2 must contain(0, 2, -4, -73, -81, -6)
    }  
    "determine equals" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(equals)),
        dag.LoadLocal(line, None, Root(line, PushString("/het/strings")), Het),
        Root(line, PushString("quirky")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (VectorCase(_), SBoolean(d)) => d
      }
      
      result2 must contain(true, false)
    }  
    "determine indexOf" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(indexOf)),
        dag.LoadLocal(line, None, Root(line, PushString("/het/strings")), Het),
        Root(line, PushString("e")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d
      }
      
      result2 must contain(-1, 7, 4, 12, 6)
    } 
    "determine equalsIgnoreCase" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(equalsIgnoreCase)),
        dag.LoadLocal(line, None, Root(line, PushString("/het/strings")), Het), 
        Root(line, PushString("QUIRKY")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (VectorCase(_), SBoolean(d)) => d
      }
      
      result2 must contain(true, false)
    }    
  }


}
