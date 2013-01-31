package com.precog.daze

import org.specs2.mutable._

import com.precog.yggdrasil._
import com.precog.common.Path
import scalaz._
import scalaz.std.list._

import com.precog.util.IdGen

trait ArrayLibSpecs[M[+_]] extends Specification
    with EvaluatorTestSupport[M]
    with LongIdMemoryDatasetConsumer[M] { self =>
      
  import Function._
  
  import dag._
  import instructions._

  import library._

  val testAPIKey = "testAPIKey"

  def testEval(graph: DepGraph): Set[SEvent] = {
    consumeEval(testAPIKey, graph, Path.Root) match {
      case Success(results) => results
      case Failure(error) => throw error
    }
  }
  
  "array utilities" should {
    "flatten a homogeneous set" in {
      val line = Line(1, 1, "")
      
      val input = dag.Morph1(Flatten,
        dag.LoadLocal(Const(CString("/hom/arrays"))(line))(line))(line)
        
      val result = testEval(input)
      result must haveSize(25)
      
      val values = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d
      }
      
      values must contain(-9, -42, 42, 87, 4, 7, 6, 12, 0, 1024, 57, 77, 46.2,
        -100, 1, 19, 22, 11, 104, -27, 6, -2790111, 244, 13, 11)
    }
    
    "flatten a heterogeneous set" in {
      val line = Line(1, 1, "")
      
      val input = dag.Morph1(Flatten,
        dag.LoadLocal(Const(CString("/het/arrays"))(line))(line))(line)
        
      val result = testEval(input)
      result must haveSize(26)
      
      val values = result collect {
        case (ids, jv) if ids.length == 1 => jv
      }
      
      values must contain(SDecimal(-9), SDecimal(-42), SDecimal(42), SDecimal(87),
        SDecimal(4), SDecimal(7), SDecimal(6), SDecimal(12), SDecimal(0),
        SDecimal(1024), SDecimal(57), SDecimal(77), SDecimal(-100), SDecimal(1),
        SDecimal(19), SDecimal(22), SDecimal(11), SDecimal(104), SDecimal(-27),
        SDecimal(6), SDecimal(-2790111), SDecimal(244), SDecimal(13), SDecimal(11),
        SArray(Vector(SDecimal(-9), SDecimal(-42), SDecimal(42), SDecimal(87), SDecimal(4))))
    }
  }
}

object ArrayLibSpecs extends ArrayLibSpecs[test.YId] with test.YIdInstances