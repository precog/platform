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

  val testAPIKey = "testAPIKey"

  def testEval(graph: DepGraph): Set[SEvent] = {
    consumeEval(testAPIKey, graph, Path.Root) match {
      case Success(results) => results
      case Failure(error) => throw error
    }
  }

  "for homogeneous sets, the appropriate string function" should {
    "determine length" in {
      val line = Line(0, "")
      
      val input = dag.Operate(BuiltInFunction1Op(length),
        dag.LoadLocal(Const(CString("/hom/strings"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(6, 12, 16, 26, 42, 0)
    }  
    "determine trim" in {
      val line = Line(0, "")
      
      val input = dag.Operate(BuiltInFunction1Op(trim),
        dag.LoadLocal(Const(CString("/hom/strings"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("quirky", "solstice + 7", "Monkey: [Brains]", """("alpha", "beta", "gamma")""", "Whitespace       is   awesome  !!!1!!", "")
    }  
    "determine toUpperCase" in {
      val line = Line(0, "")
      
      val input = dag.Operate(BuiltInFunction1Op(toUpperCase),
        dag.LoadLocal(Const(CString("/hom/strings"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("QUIRKY", "SOLSTICE + 7", "MONKEY: [BRAINS]", """("ALPHA", "BETA", "GAMMA")""", "  WHITESPACE       IS   AWESOME  !!!1!!   ", "")
    }  
    "determine toLowerCase" in {
      val line = Line(0, "")
      
      val input = dag.Operate(BuiltInFunction1Op(toLowerCase),
        dag.LoadLocal(Const(CString("/hom/strings"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("quirky", "solstice + 7", "monkey: [brains]", """("alpha", "beta", "gamma")""", "  whitespace       is   awesome  !!!1!!   ", "")
    }  
    "determine isEmpty" in {
      val line = Line(0, "")
      
      val input = dag.Operate(BuiltInFunction1Op(isEmpty),
        dag.LoadLocal(Const(CString("/hom/strings"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SBoolean(d)) if ids.length == 1 => d
      }
      
      result2 must contain(true, false)
    }  
    "determine intern" in {
      val line = Line(0, "")
      
      val input = dag.Operate(BuiltInFunction1Op(intern),
        dag.LoadLocal(Const(CString("/hom/strings"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("quirky", "solstice + 7", "Monkey: [Brains]", """("alpha", "beta", "gamma")""", "  Whitespace       is   awesome  !!!1!!   ", "")
    }  

    "determine codePointAt with valid integer" in {
      val line = Line(0, "")
      
      val input = Join(BuiltInFunction2Op(codePointAt), CrossLeftSort,
        dag.LoadLocal(Const(CString("/hom/strings"))(line))(line),
        Const(CLong(7))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(4)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(101, 32, 34, 115)
    }
    "determine codePointAt with invalid integer" in {
      val line = Line(0, "")
      
      val input = Join(BuiltInFunction2Op(codePointAt), CrossLeftSort,
        dag.LoadLocal(Const(CString("/hom/strings"))(line))(line),
        Const(CDouble(7.5))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(0)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain()
    } 
    "determine startsWith" in {
      val line = Line(0, "")
      
      val input = Join(BuiltInFunction2Op(startsWith), CrossLeftSort,
        dag.LoadLocal(Const(CString("/hom/strings"))(line))(line),
        Const(CString("s"))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SBoolean(d)) if ids.length == 1 => d
      }
      
      result2 must contain(true, false)
    }  
    "determine lastIndexOf" in {
      val line = Line(0, "")
      
      val input = Join(BuiltInFunction2Op(lastIndexOf), CrossLeftSort,
        dag.LoadLocal(Const(CString("/hom/strings"))(line))(line),
        Const(CString("s"))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(-1, 3, 14, 27)
    }
    "determine concat" in {
      val line = Line(0, "")
      
      val input = Join(BuiltInFunction2Op(concat), CrossLeftSort,
        dag.LoadLocal(Const(CString("/hom/strings"))(line))(line),
        Const(CString("7"))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("quirky7", "solstice + 77", "Monkey: [Brains]7", """("alpha", "beta", "gamma")7""", "  Whitespace       is   awesome  !!!1!!   7", "7")
    }
    "determine endsWith" in {
      val line = Line(0, "")
      
      val input = Join(BuiltInFunction2Op(endsWith), CrossLeftSort,
        dag.LoadLocal(Const(CString("/hom/strings"))(line))(line),
        Const(CString("y"))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SBoolean(d)) if ids.length == 1 => d
      }
      
      result2 must contain(true, false)
    }
    "determine codePointBefore with valid integer" in {
      val line = Line(0, "")
      
      val input = Join(BuiltInFunction2Op(codePointBefore), CrossLeftSort,
        dag.LoadLocal(Const(CString("/hom/strings"))(line))(line),
        Const(CLong(7))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(4)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(99, 58, 97, 101)
    }
    "determine codePointBefore with invalid integer" in {
      val line = Line(0, "")
      
      val input = Join(BuiltInFunction2Op(codePointBefore), CrossLeftSort,
        dag.LoadLocal(Const(CString("/hom/strings"))(line))(line),
        Const(CDouble(7.5))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(0)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain()
    }
    "determine substring with valid integer" in {
      val line = Line(0, "")
      
      val input = Join(BuiltInFunction2Op(substring), CrossLeftSort,
        dag.LoadLocal(Const(CString("/hom/strings"))(line))(line),
        Const(CLong(7))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(4)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("e + 7", " [Brains]", """", "beta", "gamma")""", "space       is   awesome  !!!1!!   ")
    }
    "determine substring with invalid integer" in {
      val line = Line(0, "")
      
      val input = Join(BuiltInFunction2Op(substring), CrossLeftSort,
        dag.LoadLocal(Const(CString("/hom/strings"))(line))(line),
        Const(CDouble(7.5))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(0)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain()
    }
    "determine matches" in {
      val line = Line(0, "")
      
      val input = Join(BuiltInFunction2Op(matches), CrossLeftSort,
        dag.LoadLocal(Const(CString("/hom/strings"))(line))(line),
        Const(CString("quirky"))(line))(line) //todo put regex here!
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SBoolean(d)) if ids.length == 1 => d
      }
      
      result2 must contain(true, false)
    }
    "determine compareTo" in {
      val line = Line(0, "")
      
      val input = Join(BuiltInFunction2Op(compareTo), CrossLeftSort,
        dag.LoadLocal(Const(CString("/hom/strings"))(line))(line),
        Const(CString("quirky"))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(0, 2, -36, -73, -81, -6)
    }
    "determine compareToIgnoreCase" in {
      val line = Line(0, "")
      
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
    "determine equals" in {
      val line = Line(0, "")
      
      val input = Join(BuiltInFunction2Op(equals), CrossLeftSort,
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
      val line = Line(0, "")
      
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
      val line = Line(0, "")
      
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
      val line = Line(0, "")
      
      val input = dag.Operate(BuiltInFunction1Op(length),
        dag.LoadLocal(Const(CString("/het/strings"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(6, 12, 16, 26, 42, 0)
    }  
    "determine trim" in {
      val line = Line(0, "")
      
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
      val line = Line(0, "")
      
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
      val line = Line(0, "")
      
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
      val line = Line(0, "")
      
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
      val line = Line(0, "")
      
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
      val line = Line(0, "")
      
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
      val line = Line(0, "")
      
      val input = Join(BuiltInFunction2Op(codePointAt), CrossLeftSort,
        dag.LoadLocal(Const(CString("/het/strings"))(line))(line),
        Const(CDouble(7.5))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(0)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain()
    }
    "determine startsWith" in {
      val line = Line(0, "")
      
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
      val line = Line(0, "")
      
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
      val line = Line(0, "")
      
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
      val line = Line(0, "")
      
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
      val line = Line(0, "")
      
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
      val line = Line(0, "")
      
      val input = Join(BuiltInFunction2Op(codePointBefore), CrossLeftSort,
        dag.LoadLocal(Const(CString("/het/strings"))(line))(line),
        Const(CDouble(7.5))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(0)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain()
    }
    "determine substring with valid integer" in {
      val line = Line(0, "")
      
      val input = Join(BuiltInFunction2Op(substring), CrossLeftSort,
        dag.LoadLocal(Const(CString("/het/strings"))(line))(line),
        Const(CLong(7))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(4)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("e + 7", " [Brains]", """", "beta", "gamma")""", "space       is   awesome  !!!1!!   ")
    }
    "determine substring with invalid integer" in {
      val line = Line(0, "")
      
      val input = Join(BuiltInFunction2Op(substring), CrossLeftSort,
        dag.LoadLocal(Const(CString("/het/strings"))(line))(line),
        Const(CDouble(7.5))(line))(line)
        
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
      
      val input = Join(BuiltInFunction2Op(matches), CrossLeftSort,
        dag.LoadLocal(Const(CString("/het/strings"))(line))(line),
        Const(CString("quirky"))(line))(line) //todo put regex here!
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SBoolean(d)) if ids.length == 1 => d
      }
      
      result2 must contain(true, false)
    }
    "determine compareTo" in {
      val line = Line(0, "")
      
      val input = Join(BuiltInFunction2Op(compareTo), CrossLeftSort,
        dag.LoadLocal(Const(CString("/het/strings"))(line))(line),
        Const(CString("quirky"))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(0, 2, -36, -73, -81, -6)
    }
    "determine compareToIgnoreCase" in {
      val line = Line(0, "")
      
      val input = Join(BuiltInFunction2Op(compareToIgnoreCase), CrossLeftSort,
        dag.LoadLocal(Const(CString("/het/strings"))(line))(line),
        Const(CString("QUIRKY"))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      result2 must contain(0, 2, -4, -73, -81, -6)
    }
    "determine equals" in {
      val line = Line(0, "")
      
      val input = Join(BuiltInFunction2Op(equals), CrossLeftSort,
        dag.LoadLocal(Const(CString("/het/strings"))(line))(line),
        Const(CString("quirky"))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (ids, SBoolean(d)) if ids.length == 1 => d
      }
      
      result2 must contain(true, false)
    }
    "determine indexOf" in {
      val line = Line(0, "")
      
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
      val line = Line(0, "")
      
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
      val line = Line(0, "")
      
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
      val line = Line(0, "")

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
  }
}

object StringLibSpec extends StringLibSpec[test.YId] with test.YIdInstances
