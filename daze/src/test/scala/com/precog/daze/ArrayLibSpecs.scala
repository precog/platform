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

import com.precog.common._
import com.precog.yggdrasil._
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
        case (ids, SDecimal(d)) if ids.length == 2 => d
      }
      
      values mustEqual Set(-9, -42, 42, 87, 4, 7, 6, 12, 0, 1024, 57, 77, 46.2,
        -100, 1, 19, 22, 11, 104, -27, 6, -2790111, 244, 13, 11)
    }
    
    "flatten a heterogeneous set" in {
      val line = Line(1, 1, "")
      
      val input = dag.Morph1(Flatten,
        dag.LoadLocal(Const(CString("/het/arrays"))(line))(line))(line)
        
      val result = testEval(input)
      result must haveSize(26)
      
      val values = result collect {
        case (ids, jv) if ids.length == 2 => jv
      }
      
      values mustEqual Set(SDecimal(-9), SDecimal(-42), SDecimal(42), SDecimal(87),
        SDecimal(4), SDecimal(7), SDecimal(6), SDecimal(12), SDecimal(0),
        SDecimal(1024), SDecimal(57), SDecimal(77), SDecimal(46.2), SDecimal(-100), SDecimal(1),
        SDecimal(19), SDecimal(22), SDecimal(11), SDecimal(104), SDecimal(-27),
        SDecimal(6), SDecimal(-2790111), SDecimal(244), SDecimal(13), SDecimal(11),
        SArray(Vector(SDecimal(-9), SDecimal(-42), SDecimal(42), SDecimal(87), SDecimal(4))))
    }
    
    "flattened set is related to original set" in {
      val line = Line(1, 1, "")
      
      val input = dag.Join(JoinObject, IdentitySort,
        dag.Join(WrapObject, Cross(None),
          Const(CString("arr"))(line),
          dag.LoadLocal(Const(CString("/het/arrays"))(line))(line))(line),
        dag.Join(WrapObject, Cross(None),
          Const(CString("val"))(line),
          dag.Morph1(Flatten,
            dag.LoadLocal(Const(CString("/het/arrays"))(line))(line))(line))(line))(line)
        
      val result = testEval(input)
      result must haveSize(26)
      
      val values = result collect {
        case (ids, jv) if ids.length == 2 => jv
      }
      values must haveAllElementsLike {
        case SObject(obj) =>
          obj.keySet mustEqual Set("arr", "val")
          val SArray(elems) = obj("arr")
          elems must contain(obj("val"))
        case _ => ko
      }
    }
    
    "flatten a non-array without exploding" in {
      val line = Line(1, 1, "")
      
      val input = dag.Morph1(Flatten,
        Const(CString("/het/arrays"))(line))(line)
        
      testEval(input) must haveSize(0)
    }
    
    "flatten an empty set without exploding" in {
      val line = Line(1, 1, "")
      
      val input = dag.Morph1(Flatten,
        Undefined(line))(line)
        
      testEval(input) must haveSize(0)
    }
  }
}

object ArrayLibSpecs extends ArrayLibSpecs[test.YId] with test.YIdInstances
