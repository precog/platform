package com.precog.daze

import org.specs2.mutable._

import com.precog.yggdrasil._
import com.precog.common.Path

import scalaz._

trait TypeLibSpec[M[+_]] extends Specification
    with EvaluatorTestSupport[M]
    with TypeLib[M]
    with LongIdMemoryDatasetConsumer[M] { self =>

  import dag._
  import instructions._
  
  val testAPIKey = "testAPIKey"
  
  def testEval(graph: DepGraph): Set[SEvent] = {
    consumeEval(testAPIKey, graph, Path.Root) match {
      case Success(results) => results
      case Failure(error) => throw error
    }
  }

  "the type functions" should {
    "return correct booleans for isNumber" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(isNumber),
        dag.LoadLocal(line, Const(line, CString("/het/numbersAcrossSlices2"))))

      val result = testEval(input)

      result must haveSize(24)

      val result2 = result.toSeq collect {
        case (ids, SBoolean(b)) if ids.length == 1 => b
      }

      val (trues, falses) = result2 partition identity

      trues.length mustEqual 9
      falses.length mustEqual 15
    }
  
    "return correct booleans for isBoolean" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(isBoolean),
        dag.LoadLocal(line, Const(line, CString("/het/numbersAcrossSlices2"))))

      val result = testEval(input)

      result must haveSize(24)

      val result2 = result.toSeq collect {
        case (ids, SBoolean(b)) if ids.length == 1 => b
      }

      val (trues, falses) = result2 partition identity

      trues.length mustEqual 4
      falses.length mustEqual 20
    }
  
    "return correct booleans for isNull" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(isNull),
        dag.LoadLocal(line, Const(line, CString("/het/numbersAcrossSlices2"))))

      val result = testEval(input)

      result must haveSize(24)

      val result2 = result.toSeq collect {
        case (ids, SBoolean(b)) if ids.length == 1 => b
      }

      val (trues, falses) = result2 partition identity

      trues.length mustEqual 2
      falses.length mustEqual 22
    }
  
    "return correct booleans for isString" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(isString),
        dag.LoadLocal(line, Const(line, CString("/het/numbersAcrossSlices2"))))

      val result = testEval(input)

      result must haveSize(24)

      val result2 = result.toSeq collect {
        case (ids, SBoolean(b)) if ids.length == 1 => b
      }

      val (trues, falses) = result2 partition identity

      trues.length mustEqual 1
      falses.length mustEqual 23
    }
  
    "return correct booleans for isObject" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(isObject),
        dag.LoadLocal(line, Const(line, CString("/het/numbersAcrossSlices2"))))

      val result = testEval(input)

      result must haveSize(24)

      val result2 = result.toSeq collect {
        case (ids, SBoolean(b)) if ids.length == 1 => b
      }

      val (trues, falses) = result2 partition identity

      trues.length mustEqual 3
      falses.length mustEqual 21
    }
  
    "return correct booleans for isArray" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(isArray),
        dag.LoadLocal(line, Const(line, CString("/het/numbersAcrossSlices2"))))

      val result = testEval(input)

      result must haveSize(24)

      val result2 = result.toSeq collect {
        case (ids, SBoolean(b)) if ids.length == 1 => b
      }

      val (trues, falses) = result2 partition identity

      trues.length mustEqual 5
      falses.length mustEqual 19
    }
  }
}

object TypeLibSpec extends TypeLibSpec[test.YId] with test.YIdInstances