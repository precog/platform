package com.precog.daze

import org.specs2.mutable._

import com.precog.yggdrasil._
import com.precog.common._
import scalaz._
import scalaz.std.list._

import com.precog.util.IdGen

import org.joda.time._
import org.joda.time.format._

trait TimeMillisSpecs[M[+_]] extends Specification
    with EvaluatorTestSupport[M]
    with LongIdMemoryDatasetConsumer[M] { self =>
      
  import Function._
  
  import dag._
  import instructions._
  import library._

  val testAPIKey = "testAPIKey"

  val line = Line(1, 1, "")
  def inputOp1(op: Op1, loadFrom: String) = {
    dag.Operate(BuiltInFunction1Op(op),
      dag.LoadLocal(Const(CString(loadFrom))(line))(line))(line)
  }

  def testEval(graph: DepGraph): Set[SEvent] = {
    consumeEval(testAPIKey, graph, Path.Root) match {
      case Success(results) => results
      case Failure(error) => throw error
    }
  }

  "converting an ISO time string to a millis value (homogeneous case)" should {
    "return the correct millis value" in {
      val input = inputOp1(GetMillis, "/hom/iso8601")

      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toLong
      }
      
      result2 must contain(1272505072599L, 1315327492848L, 1328976693394L, 1356712699430L, 1298286599165L)
    }
  }

  "converting an ISO time string to a millis value (heterogeneous case)" should {
    "return the correct millis value" in {
      val input = inputOp1(GetMillis, "/het/iso8601")

      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toLong
      }
      
      result2 must contain(1272505072599L, 1315327492848L, 1328976693394L, 1356712699430L, 1298286599165L)
    }  
  }

 "converting a millis value to an ISO time string (homogeneous case)" should {
    "return the correct time string" in {
      val input = Join(BuiltInFunction2Op(MillisToISO), Cross(None),
        dag.LoadLocal(Const(CString("/hom/millisSinceEpoch"))(line))(line),
        Const(CString("-10:00"))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d.toString
      }
      
      result2 must contain("2012-02-28T06:44:52.420-10:00", "2012-02-18T06:44:52.780-10:00", "2012-02-21T08:28:42.774-10:00", "2012-02-25T08:01:27.710-10:00", "2012-02-18T06:44:52.854-10:00")      
    }

    "default to UTC if time zone is not specified" in todo

  }

  "converting a millis value to an ISO time string (heterogeneous set)" should {
    "return the correct time string" in {
      val input = Join(BuiltInFunction2Op(MillisToISO), Cross(None),
        dag.LoadLocal(Const(CString("/het/millisSinceEpoch"))(line))(line),
        Const(CString("-10:00"))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d.toString
      }
      
      result2 must contain("2012-02-28T06:44:52.420-10:00", "2012-02-18T06:44:52.780-10:00", "2012-02-21T08:28:42.774-10:00", "2012-02-25T08:01:27.710-10:00", "2012-02-18T06:44:52.854-10:00")      
    }

    "default to UTC if time zone is not specified" in todo

  }
}

object TimeMillisSpecs extends TimeMillisSpecs[test.YId] with test.YIdInstances
