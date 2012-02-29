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
package com.precog
package daze

import akka.dispatch.Await
import akka.util.duration._
import com.precog.yggdrasil._
import org.specs2.mutable._

import java.io.File

import scalaz.effect._
import scalaz.iteratee._
import scalaz.std.list._
import Iteratee._

import com.precog.common.VectorCase
import com.precog.util.Identity

trait TestConfigComponent {
  lazy val yggConfig = new YggConfig

  class YggConfig extends YggEnumOpsConfig with DiskMemoizationConfig with EvaluatorConfig with DatasetConsumersConfig{
    def sortBufferSize = 1000
    def sortWorkDir: File = null //no filesystem storage in test!
    def chunkSerialization = SimpleProjectionSerialization
    def memoizationBufferSize = 1000
    def memoizationWorkDir: File = null //no filesystem storage in test!
    def flatMapTimeout = intToDurationInt(30).seconds
    def maxEvalDuration = intToDurationInt(30).seconds
  }
}

class EvaluatorSpecs extends Specification
    with Evaluator
    with StubOperationsAPI 
    with TestConfigComponent 
    with DiskMemoizationComponent { self =>
    

  import Function._
  
  import dag._
  import instructions._

  object ops extends Ops 
  
  val testUID = "testUID"

  def testEval = consumeEval(testUID, _: DepGraph)

  "evaluator" should {
    "evaluate simple two-value multiplication" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Cross(Mul),
        Root(line, PushNum("6")),
        Root(line, PushNum("7")))
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val result2 = result collect {
        case (VectorCase(), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(42)
    }
    
    "evaluate single value roots" >> {
      "push_string" >> {
        val line = Line(0, "")
        val input = Root(line, PushString("daniel"))
        val result = testEval(input)
        
        result must haveSize(1)
        
        val result2 = result collect {
          case (VectorCase(), SString(str)) => str
        }
        
        result2 must contain("daniel")
      }
      
      "push_num" >> {
        val line = Line(0, "")
        val input = Root(line, PushNum("42"))
        val result = testEval(input)
        
        result must haveSize(1)
        
        val result2 = result collect {
          case (VectorCase(), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(42)
      }
      
      "push_true" >> {
        val line = Line(0, "")
        val input = Root(line, PushTrue)
        val result = testEval(input)
        
        result must haveSize(1)
        
        val result2 = result collect {
          case (VectorCase(), SBoolean(b)) => b
        }
        
        result2 must contain(true)
      }
      
      "push_false" >> {
        val line = Line(0, "")
        val input = Root(line, PushFalse)
        val result = testEval(input)
        
        result must haveSize(1)
        
        val result2 = result collect {
          case (VectorCase(), SBoolean(b)) => b
        }
        
        result2 must contain(false)
      }
      
      "push_object" >> {
        val line = Line(0, "")
        val input = Root(line, PushObject)
        val result = testEval(input)
        
        result must haveSize(1)
        
        val result2 = result collect {
          case (VectorCase(), SObject(obj)) => obj
        }
        
        result2 must contain(Map())
      }
      
      "push_array" >> {
        val line = Line(0, "")
        val input = Root(line, PushArray)
        val result = testEval(input)
        
        result must haveSize(1)
        
        val result2 = result collect {
          case (VectorCase(), SArray(arr)) => arr
        }
        
        result2 must contain(Vector())
      }
    }
    
    "evaluate a load_local" in {
      val line = Line(0, "")
      val input = dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het)
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(42, 12, 77, 1, 13)
    }
    
    "evaluate a negation mapped over numbers" in {
      val line = Line(0, "")
      
      val input = Operate(line, Neg,
        dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(-42, -12, -77, -1, -13)
    }
    
    "evaluate a new mapped over numbers as no-op" in {
      val line = Line(0, "")
      
      val input = dag.New(line,
        dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(42, 12, 77, 1, 13)
    }

    "evaluate a binary non-numeric operation mapped over homogeneous set" >> { //todo also test for case when second parameter is not a singleton
      "changeTimeZone" >> {
        val line = Line(0, "")
        
        val input = Join(line, Map2Match(BuiltInFunction2(ChangeTimeZone)),
          dag.LoadLocal(line, None, Root(line, PushString("/hom/iso8601")), Het),
          Root(line, PushString("-10:00")))
          
        val result = testEval(input)
        
        result must haveSize(5)
        
        val result2 = result collect {
          case (VectorCase(_), SString(d)) => d.toString
        }
        
        result2 must contain("2011-02-21T01:09:59.165-10:00", "2012-02-11T06:11:33.394-10:00", "2011-09-06T06:44:52.848-10:00", "2010-04-28T15:37:52.599-10:00", "2012-12-28T06:38:19.430-10:00")
      }

      "epochToISO" >> {
        val line = Line(0, "")
        
        val input = Join(line, Map2Match(BuiltInFunction2(EpochToISO)),
          dag.LoadLocal(line, None, Root(line, PushString("/hom/millisSinceEpoch")), Het),
          Root(line, PushString("-10:00")))
          
        val result = testEval(input)
        
        result must haveSize(5)
        
        val result2 = result collect {
          case (VectorCase(_), SString(d)) => d.toString
        }
        
        result2 must contain("2012-02-28T06:44:52.420-10:00", "2012-02-18T06:44:52.780-10:00", "2012-02-21T08:28:42.774-10:00", "2012-02-25T08:01:27.710-10:00", "2012-02-18T06:44:52.854-10:00")      
      }
    }

    "evaluate a binary non-numeric operation mapped over heterogeneous set" >> { //todo also test for case when second parameter is not a singleton
      "changeTimeZone" >> {
        val line = Line(0, "")
        
        val input = Join(line, Map2Match(BuiltInFunction2(ChangeTimeZone)),
          dag.LoadLocal(line, None, Root(line, PushString("/hom/iso8601")), Het),
          Root(line, PushString("-10:00")))
          
        val result = testEval(input)
        
        result must haveSize(5)
        
        val result2 = result collect {
          case (VectorCase(_), SString(d)) => d.toString
        }
        
        result2 must contain("2011-02-21T01:09:59.165-10:00", "2012-02-11T06:11:33.394-10:00", "2011-09-06T06:44:52.848-10:00", "2010-04-28T15:37:52.599-10:00", "2012-12-28T06:38:19.430-10:00")
      }

      "epochToISO" >> {
        val line = Line(0, "")
        
        val input = Join(line, Map2Match(BuiltInFunction2(EpochToISO)),
          dag.LoadLocal(line, None, Root(line, PushString("/hom/millisSinceEpoch")), Het),
          Root(line, PushString("-10:00")))
          
        val result = testEval(input)
        
        result must haveSize(5)
        
        val result2 = result collect {
          case (VectorCase(_), SString(d)) => d.toString
        }
        
        result2 must contain("2012-02-28T06:44:52.420-10:00", "2012-02-18T06:44:52.780-10:00", "2012-02-21T08:28:42.774-10:00", "2012-02-25T08:01:27.710-10:00", "2012-02-18T06:44:52.854-10:00")      
      }
    }

    "evaluate a binary numeric operation mapped over homogeneous numeric set" >> {
      "addition" >> {
        val line = Line(0, "")
        
        val input = Join(line, Map2Cross(Add),
          dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
          Root(line, PushNum("5")))
          
        val result = testEval(input)
        
        result must haveSize(5)
        
        val result2 = result collect {
          case (VectorCase(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(47, 17, 82, 6, 18)
      }
      
      "subtraction" >> {
        val line = Line(0, "")
        
        val input = Join(line, Map2Cross(Sub),
          dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
          Root(line, PushNum("5")))
          
        val result = testEval(input)
        
        result must haveSize(5)
        
        val result2 = result collect {
          case (VectorCase(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(37, 7, 72, -4, 8)
      }
      
      "multiplication" >> {
        val line = Line(0, "")
        
        val input = Join(line, Map2Cross(Mul),
          dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
          Root(line, PushNum("5")))
          
        val result = testEval(input)
        
        result must haveSize(5)
        
        val result2 = result collect {
          case (VectorCase(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(210, 60, 385, 5, 65)
      }
      
      "division" >> {
        val line = Line(0, "")
        
        val input = Join(line, Map2Cross(Div),
          dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
          Root(line, PushNum("5")))
          
        val result = testEval(input)
        
        result must haveSize(5)
        
        val result2 = result collect {
          case (VectorCase(_), SDecimal(d)) => d.toDouble
        }
        
        result2 must contain(8.4, 2.4, 15.4, 0.2, 2.6)
      }
    }
    
    "evaluate a binary numeric operation mapped over heterogeneous numeric set" >> {
      "addition" >> {
        val line = Line(0, "")
        
        val input = Join(line, Map2Cross(Add),
          dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het),
          Root(line, PushNum("5")))
          
        val result = testEval(input)
        
        result must haveSize(5)
        
        val result2 = result collect {
          case (VectorCase(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(47, 17, 82, 6, 18)
      }
      
      "subtraction" >> {
        val line = Line(0, "")
        
        val input = Join(line, Map2Cross(Sub),
          dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het),
          Root(line, PushNum("5")))
          
        val result = testEval(input)
        
        result must haveSize(5)
        
        val result2 = result collect {
          case (VectorCase(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(37, 7, 72, -4, 8)
      }
      
      "multiplication" >> {
        val line = Line(0, "")
        
        val input = Join(line, Map2Cross(Mul),
          dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het),
          Root(line, PushNum("5")))
          
        val result = testEval(input)
        
        result must haveSize(5)
        
        val result2 = result collect {
          case (VectorCase(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(210, 60, 385, 5, 65)
      }
      
      "division" >> {
        val line = Line(0, "")
        
        val input = Join(line, Map2Cross(Div),
          dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het),
          Root(line, PushNum("5")))
          
        val result = testEval(input)
        
        result must haveSize(5)
        
        val result2 = result collect {
          case (VectorCase(_), SDecimal(d)) => d.toDouble
        }
        
        result2 must contain(8.4, 2.4, 15.4, 0.2, 2.6)
      }
    }
    
    "evaluate wrap_object on single values" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Cross(WrapObject),
        Root(line, PushString("answer")),
        Root(line, PushNum("42")))
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val optObj = result find {
        case (VectorCase(), SObject(_)) => true
        case _ => false
      } collect {
        case (_, SObject(obj)) => obj
      }
      
      optObj must beSome
      val obj = optObj.get
      
      obj must haveKey("answer")
      obj("answer") must beLike {
        case SDecimal(d) => d mustEqual 42
      }
    }
    
    "evaluate wrap_array on a single value" in {
      val line = Line(0, "")
      
      val input = Operate(line, WrapArray,
        Root(line, PushNum("42")))
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val optArr = result find {
        case (VectorCase(), SArray(_)) => true
        case _ => false
      } collect {
        case (_, SArray(arr)) => arr
      }
      
      optArr must beSome
      val arr = optArr.get
      
      arr must haveSize(1)
      arr.head must beLike {
        case SDecimal(d) => d mustEqual 42
      }
    }
    
    "evaluate join_object on single values" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Cross(JoinObject),
        Join(line, Map2Cross(WrapObject),
          Root(line, PushString("question")),
          Root(line, PushString("What is six times nine?"))),
        Join(line, Map2Cross(WrapObject),
          Root(line, PushString("answer")),
          Root(line, PushNum("42"))))
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val optObj = result find {
        case (VectorCase(), SObject(_)) => true
        case _ => false
      } collect {
        case (_, SObject(obj)) => obj
      }
      
      optObj must beSome
      val obj = optObj.get
      
      obj must haveKey("answer")
      obj("answer") must beLike {
        case SDecimal(d) => d mustEqual 42
      }
      
      obj must haveKey("question")
      obj("question") must beLike {
        case SString(str) => str mustEqual "What is six times nine?"
      }
    }
    
    "evaluate join_array on single values" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Cross(JoinArray),
        Operate(line, WrapArray,
          Root(line, PushNum("24"))),
        Operate(line, WrapArray,
          Root(line, PushNum("42"))))
        
      val result = testEval(input)
      
      result must haveSize(1)
      
      val optArr = result find {
        case (VectorCase(), SArray(_)) => true
        case _ => false
      } collect {
        case (_, SArray(arr)) => arr
      }
      
      optArr must beSome
      val arr = optArr.get
      
      arr must beLike {
        case Vector(SDecimal(d1), SDecimal(d2)) => {
          d1 mustEqual 24
          d2 mustEqual 42
        }
      }
    }
    
    "evaluate array_swap on single values" >> {
      "at start" >> {
        val line = Line(0, "")
        
        val input = Join(line, Map2Cross(ArraySwap),
          Join(line, Map2Cross(JoinArray),
            Operate(line, WrapArray,
              Root(line, PushNum("12"))),
            Join(line, Map2Cross(JoinArray),
              Operate(line, WrapArray,
                Root(line, PushNum("24"))),
              Operate(line, WrapArray,
                Root(line, PushNum("42"))))),
          Root(line, PushNum("1")))
          
        val result = testEval(input)
        
        result must haveSize(1)
        
        val optArr = result find {
          case (VectorCase(), SArray(_)) => true
          case _ => false
        } collect {
          case (_, SArray(arr)) => arr
        }
        
        optArr must beSome
        val arr = optArr.get
        
        arr must beLike {
          case Vector(SDecimal(d1), SDecimal(d2), SDecimal(d3)) => {
            d1 mustEqual 24
            d2 mustEqual 12
            d3 mustEqual 42
          }
        }
      }
      
      "at end" >> {
        val line = Line(0, "")
        
        val input = Join(line, Map2Cross(ArraySwap),
          Join(line, Map2Cross(JoinArray),
            Operate(line, WrapArray,
              Root(line, PushNum("12"))),
            Join(line, Map2Cross(JoinArray),
              Operate(line, WrapArray,
                Root(line, PushNum("24"))),
              Operate(line, WrapArray,
                Root(line, PushNum("42"))))),
          Root(line, PushNum("2")))
          
        val result = testEval(input)
        
        result must haveSize(1)
        
        val optArr = result find {
          case (VectorCase(), SArray(_)) => true
          case _ => false
        } collect {
          case (_, SArray(arr)) => arr
        }
        
        optArr must beSome
        val arr = optArr.get
        
        arr must beLike {
          case Vector(SDecimal(d1), SDecimal(d2), SDecimal(d3)) => {
            d1 mustEqual 12
            d2 mustEqual 42
            d3 mustEqual 24
          }
        }
      }
    }
    
    "evaluate descent on a homogeneous set" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Cross(DerefObject),
        dag.LoadLocal(line, None, Root(line, PushString("/hom/pairs")), Het),
        Root(line, PushString("first")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(42, 12, 77, 1, 13)
    }
    
    "evaluate descent on a heterogeneous set" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Cross(DerefObject),
        dag.LoadLocal(line, None, Root(line, PushString("/het/pairs")), Het),
        Root(line, PushString("first")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(42, 12, 77, 1, 13)
    }
    
    "evaluate descent producing a heterogeneous set" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Cross(DerefObject),
        dag.LoadLocal(line, None, Root(line, PushString("/het/het-pairs")), Het),
        Root(line, PushString("first")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
        case (VectorCase(_), SString(str)) => str
        case (VectorCase(_), SBoolean(b)) => b
      }
      
      result2 must contain(42, true, "daniel", 1, 13)
    }
    
    "evaluate array dereference on a homogeneous set" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Cross(DerefArray),
        dag.LoadLocal(line, None, Root(line, PushString("/hom/arrays")), Het),
        Root(line, PushNum("2")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(42, 12, 77, 1, 13)
    }
    
    "evaluate array dereference on a heterogeneous set" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Cross(DerefArray),
        dag.LoadLocal(line, None, Root(line, PushString("/het/arrays")), Het),
        Root(line, PushNum("2")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(42, 12, 77, 1, 13)
    }
    
    "evaluate array dereference producing a heterogeneous set" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Cross(DerefArray),
        dag.LoadLocal(line, None, Root(line, PushString("/het/het-arrays")), Het),
        Root(line, PushNum("2")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
        case (VectorCase(_), SString(str)) => str
        case (VectorCase(_), SBoolean(b)) => b
      }
      
      result2 must contain(42, true, "daniel", 1, 13)
    }
    
    "evaluate matched binary numeric operation" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(Sub),
        Join(line, Map2Cross(DerefObject),
          dag.LoadLocal(line, None, Root(line, PushString("/hom/pairs")), Het),
          Root(line, PushString("first"))),
        Join(line, Map2Cross(DerefObject),
          dag.LoadLocal(line, None, Root(line, PushString("/hom/pairs")), Het),
          Root(line, PushString("second"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(36, 12, 115, -165)
    }
    
    "evaluate matched binary numeric operation dropping undefined result" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(Div),
        Join(line, Map2Cross(DerefObject),
          dag.LoadLocal(line, None, Root(line, PushString("/hom/pairs")), Het),
          Root(line, PushString("first"))),
        Join(line, Map2Cross(DerefObject),
          dag.LoadLocal(line, None, Root(line, PushString("/hom/pairs")), Het),
          Root(line, PushString("second"))))
        
      val result = testEval(input)
      
      result must haveSize(4)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toDouble
      }
      
      result2 must contain(7, -2.026315789473684, 0.006024096385542169, 13)
    }
    
    "compute the vunion of two homogeneous sets" in {
      val line = Line(0, "")
      
      val input = Join(line, VUnion,
        dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
        dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers3")), Het))
        
      val result = testEval(input)
      
      result must haveSize(8)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toDouble
      }
      
      result2 must contain(42, 12, 77, 1, 13, 14, -1, 0)
    }
    
    "compute the vintersect of two homogeneous sets" in {
      val line = Line(0, "")
      
      val input = Join(line, VIntersect,
        dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
        dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers3")), Het))
        
      val result = testEval(input)
      
      result must haveSize(2)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toDouble
      }
      
      result2 must contain(42, 77)
    }
    
    // TODO tests for iunion and iintersect
    
    "filter homogeneous numeric set by binary operation" >> {
      "less-than" >> {
        val line = Line(0, "")
        
        val input = Filter(line, None, None,
          dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
          Join(line, Map2Cross(Lt),
            dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
            Root(line, PushNum("13"))))
          
        val result = testEval(input)
        
        result must haveSize(2)
        
        val result2 = result collect {
          case (VectorCase(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(1, 12)
      }
      
      "less-than-equal" >> {
        val line = Line(0, "")
        
        val input = Filter(line, None, None,
          dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
          Join(line, Map2Cross(LtEq),
            dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
            Root(line, PushNum("13"))))
          
        val result = testEval(input)
        
        result must haveSize(3)
        
        val result2 = result collect {
          case (VectorCase(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(12, 1, 13)
      }
      
      "greater-than" >> {
        val line = Line(0, "")
        
        val input = Filter(line, None, None,
          dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
          Join(line, Map2Cross(Gt),
            dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
            Root(line, PushNum("13"))))
          
        val result = testEval(input)
        
        result must haveSize(2)
        
        val result2 = result collect {
          case (VectorCase(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(42, 77)
      }
      
      "greater-than-equal" >> {
        val line = Line(0, "")
        
        val input = Filter(line, None, None,
          dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
          Join(line, Map2Cross(GtEq),
            dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
            Root(line, PushNum("13"))))
          
        val result = testEval(input)
        
        result must haveSize(3)
        
        val result2 = result collect {
          case (VectorCase(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(42, 77, 13)
      }
      
      "equal" >> {
        val line = Line(0, "")
        
        val input = Filter(line, None, None,
          dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
          Join(line, Map2Cross(Eq),
            dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
            Root(line, PushNum("13"))))
          
        val result = testEval(input)
        
        result must haveSize(1)
        
        val result2 = result collect {
          case (VectorCase(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(13)
      }
      
      "not-equal" >> {
        val line = Line(0, "")
        
        val input = Filter(line, None, None,
          dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
          Join(line, Map2Cross(NotEq),
            dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
            Root(line, PushNum("13"))))
          
        val result = testEval(input)
        
        result must haveSize(4)
        
        val result2 = result collect {
          case (VectorCase(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(42, 12, 77, 1)
      }
      
      "and" >> {
        val line = Line(0, "")
        
        val input = Filter(line, None, None,
          dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
          Join(line, Map2Match(And),
            Join(line, Map2Cross(NotEq),
              dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
              Root(line, PushNum("77"))),
            Join(line, Map2Cross(NotEq),
              dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
              Root(line, PushNum("13")))))
          
        val result = testEval(input)
        
        result must haveSize(3)
        
        val result2 = result collect {
          case (VectorCase(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(42, 12, 1)
      }
      
      "or" >> {
        val line = Line(0, "")
        
        val input = Filter(line, None, None,
          dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
          Join(line, Map2Match(Or),
            Join(line, Map2Cross(Eq),
              dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
              Root(line, PushNum("77"))),
            Join(line, Map2Cross(Eq),
              dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
              Root(line, PushNum("13")))))
          
        val result = testEval(input)
        
        result must haveSize(2)
        
        val result2 = result collect {
          case (VectorCase(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(77, 13)
      }
      
      "complement of equality" >> {
        val line = Line(0, "")
        
        val input = Filter(line, None, None,
          dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
          Operate(line, Comp,
            Join(line, Map2Cross(Eq),
              dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
              Root(line, PushNum("13")))))
          
        val result = testEval(input)
        
        result must haveSize(4)
        
        val result2 = result collect {
          case (VectorCase(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(42, 12, 77, 1)
      }
    }
    
    "filter heterogeneous numeric set by binary operation" >> {
      "less-than" >> {
        val line = Line(0, "")
        
        val input = Filter(line, None, None,
          dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het),
          Join(line, Map2Cross(Lt),
            dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het),
            Root(line, PushNum("13"))))
          
        val result = testEval(input)
        
        result must haveSize(2)
        
        val result2 = result collect {
          case (VectorCase(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(1, 12)
      }
      
      "less-than-equal" >> {
        val line = Line(0, "")
        
        val input = Filter(line, None, None,
          dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het),
          Join(line, Map2Cross(LtEq),
            dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het),
            Root(line, PushNum("13"))))
          
        val result = testEval(input)
        
        result must haveSize(3)
        
        val result2 = result collect {
          case (VectorCase(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(12, 1, 13)
      }
      
      "greater-than" >> {
        val line = Line(0, "")
        
        val input = Filter(line, None, None,
          dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het),
          Join(line, Map2Cross(Gt),
            dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het),
            Root(line, PushNum("13"))))
          
        val result = testEval(input)
        
        result must haveSize(2)
        
        val result2 = result collect {
          case (VectorCase(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(42, 77)
      }
      
      "greater-than-equal" >> {
        val line = Line(0, "")
        
        val input = Filter(line, None, None,
          dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het),
          Join(line, Map2Cross(GtEq),
            dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het),
            Root(line, PushNum("13"))))
          
        val result = testEval(input)
        
        result must haveSize(3)
        
        val result2 = result collect {
          case (VectorCase(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(42, 77, 13)
      }
      
      "equal" >> {
        val line = Line(0, "")
        
        val input = Filter(line, None, None,
          dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het),
          Join(line, Map2Cross(Eq),
            dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het),
            Root(line, PushNum("13"))))
          
        val result = testEval(input)
        
        result must haveSize(1)
        
        val result2 = result collect {
          case (VectorCase(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(13)
      }
      
      "not-equal" >> {
        val line = Line(0, "")
        
        val input = Filter(line, None, None,
          dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het),
          Join(line, Map2Cross(NotEq),
            dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het),
            Root(line, PushNum("13"))))
          
        val result = testEval(input)
        
        result must haveSize(9)
        
        val result2 = result collect {
          case (VectorCase(_), SDecimal(d)) => d.toInt
          case (VectorCase(_), SBoolean(b)) => b
          case (VectorCase(_), SString(str)) => str
          case (VectorCase(_), SObject(obj)) => obj
          case (VectorCase(_), SArray(arr)) => arr
        }
        
        result2 must contain(42, 12, 77, 1, true, false, "daniel",
          Map("test" -> SString("fubar")), Vector())
      }
      
      "and" >> {
        val line = Line(0, "")
        
        val input = Filter(line, None, None,
          dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het),
          Join(line, Map2Match(And),
            Join(line, Map2Cross(NotEq),
              dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het),
              Root(line, PushNum("77"))),
            Join(line, Map2Cross(NotEq),
              dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het),
              Root(line, PushNum("13")))))
          
        val result = testEval(input)
        
        result must haveSize(8)
        
        val result2 = result collect {
          case (VectorCase(_), SDecimal(d)) => d.toInt
          case (VectorCase(_), SBoolean(b)) => b
          case (VectorCase(_), SString(str)) => str
          case (VectorCase(_), SObject(obj)) => obj
          case (VectorCase(_), SArray(arr)) => arr
        }
        
        result2 must contain(42, 12, 1, true, false, "daniel",
          Map("test" -> SString("fubar")), Vector())
      }
      
      "or" >> {
        val line = Line(0, "")
        
        val input = Filter(line, None, None,
          dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het),
          Join(line, Map2Match(Or),
            Join(line, Map2Cross(Eq),
              dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het),
              Root(line, PushNum("77"))),
            Join(line, Map2Cross(Eq),
              dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het),
              Root(line, PushNum("13")))))
          
        val result = testEval(input)
        
        result must haveSize(2)
        
        val result2 = result collect {
          case (VectorCase(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(77, 13)
      }
      
      "complement of equality" >> {
        val line = Line(0, "")
        
        val input = Filter(line, None, None,
          dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het),
          Operate(line, Comp,
            Join(line, Map2Cross(Eq),
              dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het),
              Root(line, PushNum("13")))))
          
        val result = testEval(input)
        
        result must haveSize(9)
        
        val result2 = result collect {
          case (VectorCase(_), SDecimal(d)) => d.toInt
          case (VectorCase(_), SBoolean(b)) => b
          case (VectorCase(_), SString(str)) => str
          case (VectorCase(_), SObject(obj)) => obj
          case (VectorCase(_), SArray(arr)) => arr
        }
        
        result2 must contain(42, 12, 77, 1, true, false, "daniel",
          Map("test" -> SString("fubar")), Vector())
      }
    }
    
    "correctly order a match following a cross" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(Mul),
        dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
        Join(line, Map2Cross(Sub),
          dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
          dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers3")), Het)))
          
      val result = testEval(input)
      
      result must haveSize(25)
      
      val result2 = result collect {
        case (VectorCase(_, _), SDecimal(d)) => d.toInt
      }
      
      result2 must haveSize(23)
      
      result2 must contain(0, -377, -780, 6006, -76, 5929, 1, 156, 169, 2, 1764,
        2695, 144, 1806, -360, 1176, -832, 182, 4851, -1470, -13, -41, -24)
    }
    
    "correctly order a match following a cross within a new" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(Mul),
        dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
        Join(line, Map2Cross(Sub),
          dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
          dag.New(line, dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het))))
          
      val result = testEval(input)
      
      result must haveSize(25)
      
      val result2 = result collect {
        case (VectorCase(_, _), SDecimal(d)) => d.toInt
      }
      
      result2 must haveSize(20)
      
      result2 must contain(0, 1260, -1470, 1722, 1218, -360, -780, 132, -12,
        2695, 5005, 5852, 4928, -41, -11, -76, -377, 13, -832, 156)
    }
    
    "split on a homogeneous set" in {
      val line = Line(0, "")
      
      /*
       * nums := dataset(//hom/numbers)
       * sums('n) :=
       *   m := max(nums where nums < 'n)
       *   (nums where nums = 'n) + m     -- actually, we used split root, but close enough
       * sums
       */
      val input = dag.Split(line,
        dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
        Join(line, Map2Cross(Add),
          SplitRoot(line, 0),
          dag.Reduce(line, Max,
            Filter(line, None, None,
              dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
              Join(line, Map2Cross(Lt),
                dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het),
                SplitRoot(line, 0))))))
              
      val result = testEval(input)
      
      result must haveSize(4)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(55, 13, 119, 25)
    }
    
    "evaluate a histogram function" in {
      val Expected = Map("daniel" -> 9, "kris" -> 8, "derek" -> 7, "nick" -> 18,
        "john" -> 14, "alissa" -> 7, "franco" -> 14, "matthew" -> 10, "jason" -> 13)
      
      val line = Line(0, "")
      
      /*
       * clicks := dataset(//clicks)
       * histogram('user) :=
       *   { user: 'user, num: count(clicks where clicks.user = 'user) }
       * histogram
       */
      val input = dag.Split(line,
        Join(line, Map2Cross(DerefObject),
          dag.LoadLocal(line, None, Root(line, PushString("/clicks")), Het),
          Root(line, PushString("user"))),
        Join(line, Map2Cross(JoinObject),
          Join(line, Map2Cross(WrapObject),
            Root(line, PushString("user")),
            SplitRoot(line, 0)),
          Join(line, Map2Cross(WrapObject),
            Root(line, PushString("num")),
            dag.Reduce(line, Count,
              Filter(line, None, None,
                dag.LoadLocal(line, None, Root(line, PushString("/clicks")), Het),
                Join(line, Map2Cross(Eq),
                  Join(line, Map2Cross(DerefObject),
                    dag.LoadLocal(line, None, Root(line, PushString("/clicks")), Het),
                    Root(line, PushString("user"))),
                  SplitRoot(line, 0)))))))
                  
      val result = testEval(input)
      
      result must haveSize(9)
      
      forall(result) {
        case (VectorCase(_), SObject(obj)) => {
          obj must haveKey("user")
          obj must haveKey("num")
          
          obj("user") must beLike {
            case SString(str) => {
              str must beOneOf("daniel", "kris", "derek", "nick", "john",
                "alissa", "franco", "matthew", "jason")
            }
          }
          val SString(user) = obj("user")
            
          obj("num") must beLike {
            case SDecimal(d) => d mustEqual Expected(user)
          }
        }
        
        case p => failure("'%s' does not match the expected pattern".format(p))
      }
    }
    
    "evaluate with on the clicks dataset" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Cross(JoinObject),
        dag.LoadLocal(line, None, Root(line, PushString("/clicks")), Het),
        Join(line, Map2Cross(WrapObject),
          Root(line, PushString("t")),
          Root(line, PushNum("42"))))
          
      val result = testEval(input)
      
      result must haveSize(100)
      
      forall(result) {
        case (VectorCase(_), SObject(obj)) => {
          obj must haveKey("user")
          obj must haveKey("time")
          obj must haveKey("page")
          obj must haveKey("t")
          
          obj("t") mustEqual SDecimal(42)
        }
      }
    }
    
    "evaluate filter on the results of a histogram function" in {
      val line = Line(0, "")
      
      /*
       * clicks := dataset(//clicks)
       * histogram('user) :=
       *   { user: 'user, num: count(clicks where clicks.user = 'user) }
       * histogram where histogram.num = 9
       */
       
      val histogram = dag.Split(line,
        Join(line, Map2Cross(DerefObject),
          dag.LoadLocal(line, None, Root(line, PushString("/clicks")), Het),
          Root(line, PushString("user"))),
        Join(line, Map2Cross(JoinObject),
          Join(line, Map2Cross(WrapObject),
            Root(line, PushString("user")),
            SplitRoot(line, 0)),
          Join(line, Map2Cross(WrapObject),
            Root(line, PushString("num")),
            dag.Reduce(line, Count,
              Filter(line, None, None,
                dag.LoadLocal(line, None, Root(line, PushString("/clicks")), Het),
                Join(line, Map2Cross(Eq),
                  Join(line, Map2Cross(DerefObject),
                    dag.LoadLocal(line, None, Root(line, PushString("/clicks")), Het),
                    Root(line, PushString("user"))),
                  SplitRoot(line, 0)))))))
       
      val input = Filter(line, None, None,
        histogram,
        Join(line, Map2Cross(Eq),
          Join(line, Map2Cross(DerefObject),
            histogram,
            Root(line, PushString("num"))),
          Root(line, PushNum("9"))))
                  
      val result = testEval(input)
      
      result must haveSize(1)
      result.toList.head must beLike {
        case (VectorCase(_), SObject(obj)) => {
          obj must haveKey("user")
          obj("user") must beLike { case SString("daniel") => ok }
          
          obj must haveKey("num")
          obj("num") must beLike { case SDecimal(d) => d mustEqual 9 }
        }
      }
    }
    
    "reduce homogeneous sets" >> {
      "count" >> {
        val line = Line(0, "")
        
        val input = dag.Reduce(line, Count,
          dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het))
          
        val result = testEval(input)
        
        result must haveSize(1)
        
        val result2 = result collect {
          case (VectorCase(), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(5)
      }
      
      "mean" >> {
        val line = Line(0, "")
        
        val input = dag.Reduce(line, Mean,
          dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het))
          
        val result = testEval(input)
        
        result must haveSize(1)
        
        val result2 = result collect {
          case (VectorCase(), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(29)
      }
      
      "median" >> {
        val line = Line(0, "")
        
        val input = dag.Reduce(line, Median,
          dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het))
          
        val result = testEval(input)
        
        result must haveSize(1)
        
        val result2 = result collect {
          case (VectorCase(), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(13)
      }.pendingUntilFixed
      
      "mode" >> {
        val line = Line(0, "")
        
        val input = dag.Reduce(line, Mode,
          dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers2")), Het))
          
        val result = testEval(input)
        
        result must haveSize(1)
        
        val result2 = result collect {
          case (VectorCase(), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(1)
      }.pendingUntilFixed
      
      "max" >> {
        val line = Line(0, "")
        
        val input = dag.Reduce(line, Max,
          dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het))
          
        val result = testEval(input)
        
        result must haveSize(1)
        
        val result2 = result collect {
          case (VectorCase(), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(77)
      }
      
      "min" >> {
        val line = Line(0, "")
        
        val input = dag.Reduce(line, Min,
          dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het))
          
        val result = testEval(input)
        
        result must haveSize(1)
        
        val result2 = result collect {
          case (VectorCase(), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(1)
      }
      
      "standard deviation" >> {
        val line = Line(0, "")
        
        val input = dag.Reduce(line, StdDev,
          dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het))
          
        val result = testEval(input)
        
        result must haveSize(1)
        
        val result2 = result collect {
          case (VectorCase(), SDecimal(d)) => d.toDouble
        }
        
        result2 must contain(27.575351312358652)
      }
      
      "sum" >> {
        val line = Line(0, "")
        
        val input = dag.Reduce(line, Sum,
          dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het))
          
        val result = testEval(input)
        
        result must haveSize(1)
        
        val result2 = result collect {
          case (VectorCase(), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(145)
      }
    }

    "non-reduction of homogeneous sets" >> {
      "year" >> {
        val line = Line(0, "")
        
        val input = dag.Operate(line, BuiltInFunction1(Year),
          dag.LoadLocal(line, None, Root(line, PushString("/hom/iso8601")), Het))
          
        val result = testEval(input)
        
        result must haveSize(5)
        
        val result2 = result collect {
          case (VectorCase(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(2010, 2011, 2012)
      }

      "quarter" >> {
        val line = Line(0, "")
        
        val input = dag.Operate(line, BuiltInFunction1(QuarterOfYear),
          dag.LoadLocal(line, None, Root(line, PushString("/hom/iso8601")), Het))
          
        val result = testEval(input)
        
        result must haveSize(5)
        
        val result2 = result collect {
          case (VectorCase(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(1, 2, 3, 4)
      }

      "monthOfYear" >> {
        val line = Line(0, "")
        
        val input = dag.Operate(line, BuiltInFunction1(MonthOfYear),
          dag.LoadLocal(line, None, Root(line, PushString("/hom/iso8601")), Het))
          
        val result = testEval(input)
        
        result must haveSize(5)
        
        val result2 = result collect {
          case (VectorCase(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(4, 2, 9, 12)
      }

      "weekOfYear" >> {
        val line = Line(0, "")
        
        val input = dag.Operate(line, BuiltInFunction1(WeekOfYear),
          dag.LoadLocal(line, None, Root(line, PushString("/hom/iso8601")), Het))
          
        val result = testEval(input)
        
        result must haveSize(5)
        
        val result2 = result collect {
          case (VectorCase(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(17, 8, 36, 6, 52)
      }

      "dayOfYear" >> {
        val line = Line(0, "")
        
        val input = dag.Operate(line, BuiltInFunction1(DayOfYear),
          dag.LoadLocal(line, None, Root(line, PushString("/hom/iso8601")), Het))
          
        val result = testEval(input)
        
        result must haveSize(5)
        
        val result2 = result collect {
          case (VectorCase(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(52, 119, 42, 249, 363)
      }

      "dayOfMonth" >> {
        val line = Line(0, "")
        
        val input = dag.Operate(line, BuiltInFunction1(DayOfMonth),
          dag.LoadLocal(line, None, Root(line, PushString("/hom/iso8601")), Het))
          
        val result = testEval(input)
        
        result must haveSize(5)
        
        val result2 = result collect {
          case (VectorCase(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(21, 29, 11, 6, 28)
      }

      "dayOfWeek" >> {
        val line = Line(0, "")
        
        val input = dag.Operate(line, BuiltInFunction1(DayOfWeek),
          dag.LoadLocal(line, None, Root(line, PushString("/hom/iso8601")), Het))
          
        val result = testEval(input)
        
        result must haveSize(5)
        
        val result2 = result collect {
          case (VectorCase(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(1, 2, 6, 5, 4)
      }

      "hourOfDay" >> {
        val line = Line(0, "")
        
        val input = dag.Operate(line, BuiltInFunction1(HourOfDay),
          dag.LoadLocal(line, None, Root(line, PushString("/hom/iso8601")), Het))
          
        val result = testEval(input)
        
        result must haveSize(5)
        
        val result2 = result collect {
          case (VectorCase(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(20, 6, 9)
      }

      "minuteOfHour" >> {
        val line = Line(0, "")
        
        val input = dag.Operate(line, BuiltInFunction1(MinuteOfHour),
          dag.LoadLocal(line, None, Root(line, PushString("/hom/iso8601")), Het))
          
        val result = testEval(input)
        
        result must haveSize(5)
        
        val result2 = result collect {
          case (VectorCase(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(9, 44, 11, 37, 38)
      }

      "secondOfMinute" >> {
        val line = Line(0, "")
        
        val input = dag.Operate(line, BuiltInFunction1(SecondOfMinute),
          dag.LoadLocal(line, None, Root(line, PushString("/hom/iso8601")), Het))
          
        val result = testEval(input)
        
        result must haveSize(5)
        
        val result2 = result collect {
          case (VectorCase(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(19, 59, 52, 33)
      }

      "millisOfSecond" >> {
        val line = Line(0, "")
        
        val input = dag.Operate(line, BuiltInFunction1(MillisOfSecond),
          dag.LoadLocal(line, None, Root(line, PushString("/hom/iso8601")), Het))
          
        val result = testEval(input)
        
        result must haveSize(5)
        
        val result2 = result collect {
          case (VectorCase(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(430, 165, 848, 394, 599)
      }
    }

    "reduce heterogeneous sets" >> {
      "count" >> {
        val line = Line(0, "")
        
        val input = dag.Reduce(line, Count,
          dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het))
          
        val result = testEval(input)
        
        result must haveSize(1)
        
        val result2 = result collect {
          case (VectorCase(), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(10)
      }
      
      "mean" >> {
        val line = Line(0, "")
        
        val input = dag.Reduce(line, Mean,
          dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het))
          
        val result = testEval(input)
        
        result must haveSize(1)
        
        val result2 = result collect {
          case (VectorCase(), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(29)
      }
      
      "median" >> {
        val line = Line(0, "")
        
        val input = dag.Reduce(line, Median,
          dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het))
          
        val result = testEval(input)
        
        result must haveSize(1)
        
        val result2 = result collect {
          case (VectorCase(), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(13)
      }.pendingUntilFixed
      
      "mode" >> {
        val line = Line(0, "")
        
        val input = dag.Reduce(line, Mode,
          dag.LoadLocal(line, None, Root(line, PushString("/het/numbers2")), Het))
          
        val result = testEval(input)
        
        result must haveSize(1)
        
        val result2 = result collect {
          case (VectorCase(), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(1)
      }.pendingUntilFixed
      
      "max" >> {
        val line = Line(0, "")
        
        val input = dag.Reduce(line, Max,
          dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het))
          
        val result = testEval(input)
        
        result must haveSize(1)
        
        val result2 = result collect {
          case (VectorCase(), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(77)
      }
      
      "min" >> {
        val line = Line(0, "")
        
        val input = dag.Reduce(line, Min,
          dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het))
          
        val result = testEval(input)
        
        result must haveSize(1)
        
        val result2 = result collect {
          case (VectorCase(), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(1)
      }
      
      "standard deviation" >> {
        val line = Line(0, "")
        
        val input = dag.Reduce(line, StdDev,
          dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het))
          
        val result = testEval(input)
        
        result must haveSize(1)
        
        val result2 = result collect {
          case (VectorCase(), SDecimal(d)) => d.toDouble
        }
        
        result2 must contain(27.575351312358652)
      }
      
      "sum" >> {
        val line = Line(0, "")
        
        val input = dag.Reduce(line, Sum,
          dag.LoadLocal(line, None, Root(line, PushString("/het/numbers")), Het))
          
        val result = testEval(input)
        
        result must haveSize(1)
        
        val result2 = result collect {
          case (VectorCase(), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(145)
      }
    }

    "non-reduction of heterogeneous sets" >> {
      "year" >> {
        val line = Line(0, "")
        
        val input = dag.Operate(line, BuiltInFunction1(Year),
          dag.LoadLocal(line, None, Root(line, PushString("/het/iso8601")), Het))
          
        val result = testEval(input)
        
        result must haveSize(5)
        
        val result2 = result collect {
          case (VectorCase(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(2010, 2011, 2012)
      }

      "quarter" >> {
        val line = Line(0, "")
        
        val input = dag.Operate(line, BuiltInFunction1(QuarterOfYear),
          dag.LoadLocal(line, None, Root(line, PushString("/het/iso8601")), Het))
          
        val result = testEval(input)
        
        result must haveSize(5)
        
        val result2 = result collect {
          case (VectorCase(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(1, 2, 3, 4)
      }

      "monthOfYear" >> {
        val line = Line(0, "")
        
        val input = dag.Operate(line, BuiltInFunction1(MonthOfYear),
          dag.LoadLocal(line, None, Root(line, PushString("/het/iso8601")), Het))
          
        val result = testEval(input)
        
        result must haveSize(5)
        
        val result2 = result collect {
          case (VectorCase(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(4, 2, 9, 12)
      }

      "weekOfYear" >> {
        val line = Line(0, "")
        
        val input = dag.Operate(line, BuiltInFunction1(WeekOfYear),
          dag.LoadLocal(line, None, Root(line, PushString("/het/iso8601")), Het))
          
        val result = testEval(input)
        
        result must haveSize(5)
        
        val result2 = result collect {
          case (VectorCase(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(17, 8, 36, 6, 52)
      }

      "dayOfYear" >> {
        val line = Line(0, "")
        
        val input = dag.Operate(line, BuiltInFunction1(DayOfYear),
          dag.LoadLocal(line, None, Root(line, PushString("/het/iso8601")), Het))
          
        val result = testEval(input)
        
        result must haveSize(5)
        
        val result2 = result collect {
          case (VectorCase(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(52, 119, 42, 249, 363)
      }

      "dayOfMonth" >> {
        val line = Line(0, "")
        
        val input = dag.Operate(line, BuiltInFunction1(DayOfMonth),
          dag.LoadLocal(line, None, Root(line, PushString("/het/iso8601")), Het))
          
        val result = testEval(input)
        
        result must haveSize(5)
        
        val result2 = result collect {
          case (VectorCase(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(21, 29, 11, 6, 28)
      }

      "dayOfWeek" >> {
        val line = Line(0, "")
        
        val input = dag.Operate(line, BuiltInFunction1(DayOfWeek),
          dag.LoadLocal(line, None, Root(line, PushString("/het/iso8601")), Het))
          
        val result = testEval(input)
        
        result must haveSize(5)
        
        val result2 = result collect {
          case (VectorCase(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(1, 2, 6, 5, 4)
      }

      "hourOfDay" >> {
        val line = Line(0, "")
        
        val input = dag.Operate(line, BuiltInFunction1(HourOfDay),
          dag.LoadLocal(line, None, Root(line, PushString("/het/iso8601")), Het))
          
        val result = testEval(input)
        
        result must haveSize(5)
        
        val result2 = result collect {
          case (VectorCase(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(20, 6, 9)
      }

      "minuteOfHour" >> {
        val line = Line(0, "")
        
        val input = dag.Operate(line, BuiltInFunction1(MinuteOfHour),
          dag.LoadLocal(line, None, Root(line, PushString("/het/iso8601")), Het))
          
        val result = testEval(input)
        
        result must haveSize(5)
        
        val result2 = result collect {
          case (VectorCase(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(9, 44, 11, 37, 38)
      }

      "secondOfMinute" >> {
        val line = Line(0, "")
        
        val input = dag.Operate(line, BuiltInFunction1(SecondOfMinute),
          dag.LoadLocal(line, None, Root(line, PushString("/het/iso8601")), Het))
          
        val result = testEval(input)
        
        result must haveSize(5)
        
        val result2 = result collect {
          case (VectorCase(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(19, 59, 52, 33)
      }

      "millisOfSecond" >> {
        val line = Line(0, "")
        
        val input = dag.Operate(line, BuiltInFunction1(MillisOfSecond),
          dag.LoadLocal(line, None, Root(line, PushString("/het/iso8601")), Het))
          
        val result = testEval(input)
        
        result must haveSize(5)
        
        val result2 = result collect {
          case (VectorCase(_), SDecimal(d)) => d.toInt
        }
        
        result2 must contain(430, 165, 848, 394, 599)
      }
    }
  }

  "sortByIdentities" should {
    def consumeToList(d: DatasetEnum[Unit, SEvent, IO]): List[SEvent] ={
      val enum = Await.result(d.fenum, intToDurationInt(5).seconds)
      (consume[Unit, Vector[SEvent], IO, List] &= enum[IO]).run(_ => sys.error("")).unsafePerformIO.flatten
    }

    "order the numbers set by specified identities" in {
      withMemoizationContext { ctx =>
        val numbers = {
          val base = eval[Unit]("testUID", dag.LoadLocal(Line(0, ""), None, Root(Line(0, ""), PushString("/hom/numbers")), Het))
          base.zipWithIndex map {
            case ((_, sv), id) => (VectorCase(id): Identities, sv)
          }
        }
        val max = 5

        val enum = numbers.zipWithIndex map {
          case ((ids, sv), i) => (ids :+ (5 - i), sv)
        }

        val sorted = sortByIdentities(enum, Vector(0), Identity.nextInt(), ctx)
        val sorted2 = sortByIdentities(enum, Vector(1), Identity.nextInt(), ctx)
        val sorted3 = sortByIdentities(enum, Vector(1, 0), Identity.nextInt(), ctx)

        consumeToList(sorted) mustEqual List((VectorCase(0, 5), SDecimal(42)),
           (VectorCase(1, 4), SDecimal(12)), (VectorCase(2, 3), SDecimal(77)),
           (VectorCase(3, 2), SDecimal(1)), (VectorCase(4, 1), SDecimal(13)))

        consumeToList(sorted2) mustEqual List((VectorCase(1, 4), SDecimal(13)),
           (VectorCase(2, 3), SDecimal(1)), (VectorCase(3, 2), SDecimal(77)),
           (VectorCase(4, 1), SDecimal(12)), (VectorCase(5, 0), SDecimal(42)))

        consumeToList(sorted3) mustEqual List((VectorCase(1, 4), SDecimal(13)),
           (VectorCase(2, 3), SDecimal(1)), (VectorCase(3, 2), SDecimal(77)),
           (VectorCase(4, 1), SDecimal(12)), (VectorCase(5, 0), SDecimal(42)))
      }
    }
  }
}
