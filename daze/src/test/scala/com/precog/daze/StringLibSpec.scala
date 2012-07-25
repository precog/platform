package com.precog.daze

import org.specs2.mutable._

import com.precog.yggdrasil._
import com.precog.yggdrasil.memoization._

import scalaz._
import scalaz.effect._
import scalaz.iteratee._
import scalaz.std.list._
import Iteratee._

import com.precog.common.VectorCase
import com.precog.util.IdGen

trait StringLibSpec[M[+_]] extends Specification
    with Evaluator[M]
    with TestConfigComponent[M] 
    with StringLib[M] 
    with MemoryDatasetConsumer[M] { self =>
      
  import Function._
  
  import dag._
  import instructions._

  val testUID = "testUID"

  def testEval(graph: DepGraph): Set[SEvent] = withContext { ctx =>
    consumeEval(testUID, graph, ctx) match {
      case Success(results) => results
      case Failure(error) => throw error
    }
  }

  "all string functions" should {
    "validate input" in todo
    "return failing validations for bad input" in todo
  }

  "for homogeneous sets, the appropriate string function" should {
    "determine length" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(length),
        dag.LoadLocal(line, Root(line, PushString("/hom/strings"))))
        
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
        dag.LoadLocal(line, Root(line, PushString("/hom/strings"))))
        
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
        dag.LoadLocal(line, Root(line, PushString("/hom/strings"))))
        
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
        dag.LoadLocal(line, Root(line, PushString("/hom/strings"))))
        
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
        dag.LoadLocal(line, Root(line, PushString("/hom/strings"))))
        
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
        dag.LoadLocal(line, Root(line, PushString("/hom/strings"))))
        
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
        dag.LoadLocal(line, Root(line, PushString("/hom/strings"))),
        Root(line, PushNum("7")))
        
      val result = testEval(input)
      
      result must haveSize(4)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d
      }
      
      result2 must contain(101, 32, 34, 115)
    }.pendingUntilFixed    
    "determine codePointAt with invalid integer" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(codePointAt)),
        dag.LoadLocal(line, Root(line, PushString("/hom/strings"))),
        Root(line, PushNum("7.5")))
        
      val result = testEval(input)
      
      result must haveSize(0)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d
      }
      
      result2 must contain()
    }.pendingUntilFixed 
    "determine startsWith" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(startsWith)),
        dag.LoadLocal(line, Root(line, PushString("/hom/strings"))),
        Root(line, PushString("s")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (VectorCase(_), SBoolean(d)) => d
      }
      
      result2 must contain(true, false)
    }.pendingUntilFixed  
    "determine lastIndexOf" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(lastIndexOf)),
        dag.LoadLocal(line, Root(line, PushString("/hom/strings"))),
        Root(line, PushString("s")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d
      }
      
      result2 must contain(-1, 3, 14, 27)
    }.pendingUntilFixed
    "determine concat" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(concat)),
        dag.LoadLocal(line, Root(line, PushString("/hom/strings"))),
        Root(line, PushString("7")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (VectorCase(_), SString(d)) => d
      }
      
      result2 must contain("quirky7", "solstice + 77", "Monkey: [Brains]7", """("alpha", "beta", "gamma")7""", "  Whitespace       is   awesome  !!!1!!   7", "7")
    }.pendingUntilFixed
    "determine endsWith" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(endsWith)),
        dag.LoadLocal(line, Root(line, PushString("/hom/strings"))),
        Root(line, PushString("y")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (VectorCase(_), SBoolean(d)) => d
      }
      
      result2 must contain(true, false)
    }.pendingUntilFixed
    "determine codePointBefore with valid integer" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(codePointBefore)),
        dag.LoadLocal(line, Root(line, PushString("/hom/strings"))),
        Root(line, PushNum("7")))
        
      val result = testEval(input)
      
      result must haveSize(4)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d
      }
      
      result2 must contain(99, 58, 97, 101)
    }.pendingUntilFixed
    "determine codePointBefore with invalid integer" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(codePointBefore)),
        dag.LoadLocal(line, Root(line, PushString("/hom/strings"))),
        Root(line, PushNum("7.5")))
        
      val result = testEval(input)
      
      result must haveSize(0)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d
      }
      
      result2 must contain()
    }.pendingUntilFixed
    "determine substring with valid integer" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(substring)),
        dag.LoadLocal(line, Root(line, PushString("/hom/strings"))),
        Root(line, PushNum("7")))
        
      val result = testEval(input)
      
      result must haveSize(4)
      
      val result2 = result collect {
        case (VectorCase(_), SString(d)) => d
      }
      
      result2 must contain("e + 7", " [Brains]", """", "beta", "gamma")""", "space       is   awesome  !!!1!!   ")
    }.pendingUntilFixed
    "determine substring with invalid integer" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(substring)),
        dag.LoadLocal(line, Root(line, PushString("/hom/strings"))),
        Root(line, PushNum("7.5")))
        
      val result = testEval(input)
      
      result must haveSize(0)
      
      val result2 = result collect {
        case (VectorCase(_), SString(d)) => d
      }
      
      result2 must contain()
    }.pendingUntilFixed
    "determine matches" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(matches)),
        dag.LoadLocal(line, Root(line, PushString("/hom/strings"))),
        Root(line, PushString("quirky"))) //todo put regex here!
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (VectorCase(_), SBoolean(d)) => d
      }
      
      result2 must contain(true, false)
    }.pendingUntilFixed
    "determine compareTo" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(compareTo)),
        dag.LoadLocal(line, Root(line, PushString("/hom/strings"))),
        Root(line, PushString("quirky")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d
      }
      
      result2 must contain(0, 2, -36, -73, -81, -6)
    }.pendingUntilFixed
    "determine compareToIgnoreCase" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(compareToIgnoreCase)),
        dag.LoadLocal(line, Root(line, PushString("/hom/strings"))),
        Root(line, PushString("QUIRKY")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d
      }
      
      result2 must contain(0, 2, -4, -73, -81, -6)
    }.pendingUntilFixed
    "determine equals" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(equals)),
        dag.LoadLocal(line, Root(line, PushString("/hom/strings"))),
        Root(line, PushString("quirky")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (VectorCase(_), SBoolean(d)) => d
      }
      
      result2 must contain(true, false)
    }.pendingUntilFixed
    "determine indexOf" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(indexOf)),
        dag.LoadLocal(line, Root(line, PushString("/hom/strings"))),
        Root(line, PushString("e")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d
      }
      
      result2 must contain(-1, 7, 4, 12, 6)
    }.pendingUntilFixed
    "determine equalsIgnoreCase" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(equalsIgnoreCase)),
        dag.LoadLocal(line, Root(line, PushString("/hom/strings"))), 
        Root(line, PushString("QUIRKY")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (VectorCase(_), SBoolean(d)) => d
      }
      
      result2 must contain(true, false)
    }.pendingUntilFixed
  }

  "for heterogeneous sets, the appropriate string function" should {
    "determine length" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(length),
        dag.LoadLocal(line, Root(line, PushString("/het/strings"))))
        
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
        dag.LoadLocal(line, Root(line, PushString("/het/strings"))))
        
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
        dag.LoadLocal(line, Root(line, PushString("/het/strings"))))
        
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
        dag.LoadLocal(line, Root(line, PushString("/het/strings"))))
        
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
        dag.LoadLocal(line, Root(line, PushString("/het/strings"))))
        
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
        dag.LoadLocal(line, Root(line, PushString("/het/strings"))))
        
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
        dag.LoadLocal(line, Root(line, PushString("/het/strings"))),
        Root(line, PushNum("7")))
        
      val result = testEval(input)
      
      result must haveSize(4)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d
      }
      
      result2 must contain(101, 32, 34, 115)
    }.pendingUntilFixed
    "determine codePointAt with invalid integer" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(codePointAt)),
        dag.LoadLocal(line, Root(line, PushString("/het/strings"))),
        Root(line, PushNum("7.5")))
        
      val result = testEval(input)
      
      result must haveSize(0)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d
      }
      
      result2 must contain()
    }.pendingUntilFixed
    "determine startsWith" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(startsWith)),
        dag.LoadLocal(line, Root(line, PushString("/het/strings"))),
        Root(line, PushString("s")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (VectorCase(_), SBoolean(d)) => d
      }
      
      result2 must contain(true, false)
    }.pendingUntilFixed
    "determine lastIndexOf" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(lastIndexOf)),
        dag.LoadLocal(line, Root(line, PushString("/het/strings"))),
        Root(line, PushString("s")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d
      }
      
      result2 must contain(-1, 3, 14, 27)
    }.pendingUntilFixed
    "determine concat" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(concat)),
        dag.LoadLocal(line, Root(line, PushString("/het/strings"))),
        Root(line, PushString("7")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (VectorCase(_), SString(d)) => d
      }
      
      result2 must contain("quirky7", "solstice + 77", "Monkey: [Brains]7", """("alpha", "beta", "gamma")7""", "  Whitespace       is   awesome  !!!1!!   7", "7")
    }.pendingUntilFixed
    "determine endsWith" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(endsWith)),
        dag.LoadLocal(line, Root(line, PushString("/het/strings"))),
        Root(line, PushString("y")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (VectorCase(_), SBoolean(d)) => d
      }
      
      result2 must contain(true, false)
    }.pendingUntilFixed
    "determine codePointBefore with valid integer" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(codePointBefore)),
        dag.LoadLocal(line, Root(line, PushString("/het/strings"))),
        Root(line, PushNum("7")))
        
      val result = testEval(input)
      
      result must haveSize(4)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d
      }
      
      result2 must contain(99, 58, 97, 101)
    }.pendingUntilFixed
    "determine codePointBefore with invalid integer" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(codePointBefore)),
        dag.LoadLocal(line, Root(line, PushString("/het/strings"))),
        Root(line, PushNum("7.5")))
        
      val result = testEval(input)
      
      result must haveSize(0)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d
      }
      
      result2 must contain()
    }.pendingUntilFixed
    "determine substring with valid integer" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(substring)),
        dag.LoadLocal(line, Root(line, PushString("/het/strings"))),
        Root(line, PushNum("7")))
        
      val result = testEval(input)
      
      result must haveSize(4)
      
      val result2 = result collect {
        case (VectorCase(_), SString(d)) => d
      }
      
      result2 must contain("e + 7", " [Brains]", """", "beta", "gamma")""", "space       is   awesome  !!!1!!   ")
    }.pendingUntilFixed
    "determine substring with invalid integer" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(substring)),
        dag.LoadLocal(line, Root(line, PushString("/het/strings"))),
        Root(line, PushNum("7.5")))
        
      val result = testEval(input)
      
      result must haveSize(0)
      
      val result2 = result collect {
        case (VectorCase(_), SString(d)) => d
      }
      
      result2 must contain()
    }.pendingUntilFixed
    "determine matches" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(matches)),
        dag.LoadLocal(line, Root(line, PushString("/het/strings"))),
        Root(line, PushString("quirky"))) //todo put regex here!
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (VectorCase(_), SBoolean(d)) => d
      }
      
      result2 must contain(true, false)
    }.pendingUntilFixed
    "determine compareTo" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(compareTo)),
        dag.LoadLocal(line, Root(line, PushString("/het/strings"))),
        Root(line, PushString("quirky")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d
      }
      
      result2 must contain(0, 2, -36, -73, -81, -6)
    }.pendingUntilFixed
    "determine compareToIgnoreCase" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(compareToIgnoreCase)),
        dag.LoadLocal(line, Root(line, PushString("/het/strings"))),
        Root(line, PushString("QUIRKY")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d
      }
      
      result2 must contain(0, 2, -4, -73, -81, -6)
    }.pendingUntilFixed
    "determine equals" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(equals)),
        dag.LoadLocal(line, Root(line, PushString("/het/strings"))),
        Root(line, PushString("quirky")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (VectorCase(_), SBoolean(d)) => d
      }
      
      result2 must contain(true, false)
    }.pendingUntilFixed
    "determine indexOf" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(indexOf)),
        dag.LoadLocal(line, Root(line, PushString("/het/strings"))),
        Root(line, PushString("e")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d
      }
      
      result2 must contain(-1, 7, 4, 12, 6)
    }.pendingUntilFixed
    "determine equalsIgnoreCase" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(BuiltInFunction2Op(equalsIgnoreCase)),
        dag.LoadLocal(line, Root(line, PushString("/het/strings"))), 
        Root(line, PushString("QUIRKY")))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      val result2 = result collect {
        case (VectorCase(_), SBoolean(d)) => d
      }
      
      result2 must contain(true, false)
    }.pendingUntilFixed
  }
}

object StringLibSpec extends StringLibSpec[test.YId] with test.YIdInstances
