package com.precog.daze

import org.specs2.mutable._

import com.precog.common._
import com.precog.yggdrasil._

import scalaz._

trait TypeLibSpecs[M[+_]] extends Specification
    with EvaluatorTestSupport[M]
    with LongIdMemoryDatasetConsumer[M] { self =>

  import dag._
  import instructions._
  import library._
  
  def testEval(graph: DepGraph): Set[SEvent] = {
    consumeEval(graph, defaultEvaluationContext) match {
      case Success(results) => results
      case Failure(error) => throw error
    }
  }

  "the type functions" should {
    "return correct booleans for isNumber" in {
      val line = Line(1, 1, "")

      val input = dag.Operate(BuiltInFunction1Op(isNumber),
        dag.LoadLocal(Const(CString("/het/numbersAcrossSlices2"))(line))(line))(line)

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
      val line = Line(1, 1, "")

      val input = dag.Operate(BuiltInFunction1Op(isBoolean),
        dag.LoadLocal(Const(CString("/het/numbersAcrossSlices2"))(line))(line))(line)

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
      val line = Line(1, 1, "")

      val input = dag.Operate(BuiltInFunction1Op(isNull),
        dag.LoadLocal(Const(CString("/het/numbersAcrossSlices2"))(line))(line))(line)

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
      val line = Line(1, 1, "")

      val input = dag.Operate(BuiltInFunction1Op(isString),
        dag.LoadLocal(Const(CString("/het/numbersAcrossSlices2"))(line))(line))(line)

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
      val line = Line(1, 1, "")

      val input = dag.Operate(BuiltInFunction1Op(isObject),
        dag.LoadLocal(Const(CString("/het/numbersAcrossSlices2"))(line))(line))(line)

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
      val line = Line(1, 1, "")

      val input = dag.Operate(BuiltInFunction1Op(isArray),
        dag.LoadLocal(Const(CString("/het/numbersAcrossSlices2"))(line))(line))(line)

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

object TypeLibSpecs extends TypeLibSpecs[test.YId] with test.YIdInstances
