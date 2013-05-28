package com.precog.mirror

import org.specs2.matcher._
import org.specs2.mutable._

import blueeyes.json._

object EvaluatorSpecs extends Specification with EvaluatorModule {
  import Function.const
  
  "mirror evaluator" should {
    implicit val fs = FS(
      "/nums" -> Vector(JNum(1), JNum(2), JNum(3)),
      "/nums2" -> Vector(JNum(1), JNum(2), JNum(3)),
      "/nums3" -> Vector(JNum(1), JNum(2), JNum(3)))
    
    "evaluate basic literals" >> {
      "strings" >> {
        "\"foo\"" must evalTo(JString("foo"))
      }
      
      "numerics" >> {
        "42" must evalTo(JNum(42))
      }
      
      "booleans" >> {
        "true" must evalTo(JTrue)
        "false" must evalTo(JFalse)
      }
      
      "undefined" >> {
        "undefined" must evalTo()
      }
      
      "null" >> {
        "null" must evalTo(JNull)
      }
    }
    
    "evaluate compound literals" >> {
      "objects" >> {
        "{a:1, b:2}" must evalTo(JObject(Map("a" -> JNum(1), "b" -> JNum(2))))
      }
      
      "arrays" >> {
        "[1, 2]" must evalTo(JArray(JNum(1) :: JNum(2) :: Nil))
      }
    }
    
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
    
    "evaluate a simple object concatenation" in {
      "{a:1} with {b:2}" must evalTo(JObject(Map("a" -> JNum(1), "b" -> JNum(2))))
    }
    
    "evaluate a simple object deref" in {
      "{a:1}.a" must evalTo(JNum(1))
    }
    
    "evaluate a simple array deref" in {
      "([42])[0]" must evalTo(JNum(42))
    }
    
    "map constant addition over a set of numbers" in {
      "//nums + 5" must evalTo(JNum(6), JNum(7), JNum(8))
    }
    
    "self-join through the addition operator" in {
      "//nums + //nums" must evalTo(JNum(2), JNum(4), JNum(6))
    }
    
    "self-join a chain of operators" in {
      "//nums + //nums + //nums" must evalTo(JNum(3), JNum(6), JNum(9))
    }
    
    "filter a dataset" in {
      val input = """
        | nums := //nums
        | nums where nums < 2
        | """.stripMargin
        
      input must evalTo(JNum(1))
    }
    
    "evaluate a trivial assertion" >> {
      "success" >> {
        "assert true 42" must evalTo(JNum(42))
      }
      
      "failure" >> {
        "assert false 42" must evalAndThrow[RuntimeException]
      }
    }
    
    "evaluate a trivial conditional expression" in {
      "if true then 42 else 12" must evalTo(JNum(42))
    }
    
    "evaluate a simple union" in {
      "1 union 2" must evalTo(JNum(1), JNum(2))
    }
    
    "evaluate a simple intersect" in {
      "1 union 2 intersect 1" must evalTo(JNum(1))
    }
    
    "evaluate a simple difference" in {
      "1 union 2 difference 1" must evalTo(JNum(2))
    }
    
    "evaluate a sin function" in {
      "std::math::sin(42)" must evalTo(JNum(-0.9165215479156338))
    }
    
    "evaluate a roundTo function" in {
      "std::math::roundTo(3.14, 1)" must evalTo(JNum(3.1))
    }
    
    "evaluate a cross between unrelated sets" in {
      val input = """
        | n := //nums
        | n2 := //nums2
        |
        | n ~ n2
        |   n + n2
        | """.stripMargin
        
      input must evalTo(JNum(2), JNum(3), JNum(4), JNum(3), JNum(4), JNum(5), JNum(4), JNum(5), JNum(6))
    }
    
    "evaluate a join between related resultants" in {
      val input = """
        | n := //nums
        | n2 := //nums2
        | n3 := //nums3
        |
        | n ~ n2 ~ n3
        |   (n + n2) * (n3 + n)
        | """.stripMargin
        
      input must evalTo(JNum(4), JNum(9), JNum(16), JNum(9), JNum(16), JNum(25), JNum(16), JNum(25), JNum(36))
    }
    
    "restrict cartesians by filtered relation" in {
      val input = """
        | n := //nums
        | n2 := //nums2
        |
        | n' := n where n < 2
        |
        | n' ~ n2
        |   n + n2       -- no one uses this feature, hilariously
        | """.stripMargin
        
      input must evalTo(JNum(2), JNum(3), JNum(4))
    }
    
    "evaluate reductions" >> {
      "count" >> {
        "count(//nums)" must evalTo(JNum(3))
        "count(undefined)" must evalTo(JNum(0))
      }
    }
  }
  
  private def evalTo(expect: JValue*)(implicit fs: FS): Matcher[String] = {
    def doEval(q: String) =
      eval(compileSingle(q))(fs.map)
    
    def inner(q: String): Boolean = {
      val actual = doEval(q)
      
      expect.length == actual.length && (expect zip actual forall { case (a, b) => a == b })
    }
    
    def message(q: String): String = {
      val actual = doEval(q)
      
      "evaluates to [%s], not [%s]".format(
        actual map { _.renderCompact } mkString ",",
        expect map { _.renderCompact } mkString ",")
    }
    
    (inner _, message _)
  }
  
  private def evalAndThrow[E <: Throwable](implicit fs: FS, evidence: ClassManifest[E]): Matcher[String] = {
    def inner(q: String): Boolean = eval(compileSingle(q))(fs.map) must throwA[E]
    (inner _, "unused error message")
  }
  
  private def compileSingle(str: String): Expr = {
    val forest = compile(str) filter { _.errors must beEmpty }
    forest must haveSize(1)
    forest.head
  }
  
  private case class FS(files: (String, Seq[JValue])*) {
    val map = Map(files: _*)
  }
  
  private object FS {
    implicit val Empty: FS = FS()
  }
}
