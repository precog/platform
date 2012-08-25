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

import com.precog.common.Path

import com.precog.yggdrasil._
import com.precog.yggdrasil.memoization._
import com.precog.yggdrasil.serialization._
import com.precog.yggdrasil.test._
import com.precog.yggdrasil.util._
import com.precog.common.VectorCase
import com.precog.util.IOUtils
import com.precog.util.IdGen
import com.precog.bytecode._

import akka.dispatch.{Await, ExecutionContext}
import akka.util.duration._

import blueeyes.json._
import JsonAST.{JObject, JField, JArray, JNum}

import java.io._
import java.util.concurrent.Executors

import scalaz._
import scalaz.effect._
import scalaz.iteratee._
import scalaz.std.list._
import scalaz.{NonEmptyList => NEL, _}
import Iteratee._

import org.specs2.specification.Fragment
import org.specs2.specification.Fragments
import org.specs2.execute.Result
import org.specs2.mutable._

trait TestConfigComponent[M[+_]] extends table.StubColumnarTableModule[M] with IdSourceScannerModule[M] {
  val asyncContext = ExecutionContext fromExecutor Executors.newCachedThreadPool()
  
  object yggConfig extends YggConfig
  
  trait YggConfig extends EvaluatorConfig with DatasetConsumersConfig {
    val sortBufferSize = 1000
    val sortWorkDir: File = IOUtils.createTmpDir("idsoSpec").unsafePerformIO
    val clock = blueeyes.util.Clock.System
    val memoizationBufferSize = 1000
    val memoizationWorkDir: File = null //no filesystem storage in test!
    val flatMapTimeout = intToDurationInt(30).seconds
    val maxEvalDuration = intToDurationInt(30).seconds

    object valueSerialization extends SortSerialization[SValue] with SValueRunlengthFormatting with BinarySValueFormatting with ZippedStreamSerialization
    object eventSerialization extends SortSerialization[SEvent] with SEventRunlengthFormatting with BinarySValueFormatting with ZippedStreamSerialization
    object groupSerialization extends SortSerialization[(SValue, Identities, SValue)] with GroupRunlengthFormatting with BinarySValueFormatting with ZippedStreamSerialization
    object memoSerialization extends IncrementalSerialization[(Identities, SValue)] with SEventRunlengthFormatting with BinarySValueFormatting with ZippedStreamSerialization

    val idSource = new IdSource {
      private val source = new java.util.concurrent.atomic.AtomicLong
      def nextId() = source.getAndIncrement
    }
  }
}

trait EvaluatorSpecs[M[+_]] extends Specification
    with Evaluator[M]
    with StdLib[M]
    with TestConfigComponent[M] 
    with MemoryDatasetConsumer[M] { self =>
  
  import Function._
  
  import dag._
  import instructions._

  val testUID = "testUID"

  def testEval(graph: DepGraph, path: Path = Path.Root)(test: Set[SEvent] => Result): Result = withContext { ctx =>
    (consumeEval(testUID, graph, ctx, path) match {
      case Success(results) => test(results)
      case Failure(error) => throw error
    }) and 
    (consumeEval(testUID, graph, ctx, path, false) match {
      case Success(results) => test(results)
      case Failure(error) => throw error
    })
  }

  "evaluator" should {
    "evaluate simple two-value multiplication" in {
      val line = Line(0, "")
      
      val input = Join(line, Mul, CrossLeftSort,
        Root(line, PushNum("6")),
        Root(line, PushNum("7")))
        
      testEval(input) { result => 
        result must haveSize(1)
        
        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.isEmpty => d.toInt
        }
        
        result2 must contain(42)
      }
    }
    
    "evaluate single value roots" >> {
      "push_string" >> {
        val line = Line(0, "")
        val input = Root(line, PushString("daniel"))
        
        testEval(input) { result =>
          result must haveSize(1)
          
          val result2 = result collect {
            case (ids, SString(str)) if ids.isEmpty => str
          }
          
          result2 must contain("daniel")
        }
      }
      
      "push_num" >> {
        val line = Line(0, "")
        val input = Root(line, PushNum("42"))
        testEval(input) { result =>
          result must haveSize(1)
          
          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.isEmpty => d.toInt
          }
          
          result2 must contain(42)
        }
      }
      
      "push_true" >> {
        val line = Line(0, "")
        val input = Root(line, PushTrue)
        
        testEval(input) { result =>
          result must haveSize(1)
          
          val result2 = result collect {
            case (ids, SBoolean(b)) if ids.isEmpty => b
          }
          
          result2 must contain(true)
        }
      }
      
      "push_false" >> {
        val line = Line(0, "")
        val input = Root(line, PushFalse)
        
        testEval(input) { result =>
          result must haveSize(1)
          
          val result2 = result collect {
            case (ids, SBoolean(b)) if ids.isEmpty => b
          }
          
          result2 must contain(false)
        }      
      }

      "push_null" >> {
        val line = Line(0, "")
        val input = Root(line, PushNull)
        
        testEval(input) { result =>
          result must haveSize(1)
          result must contain((VectorCase(), SNull))
        }
      }
      
      "push_object" >> {
        val line = Line(0, "")
        val input = Root(line, PushObject)
        
        testEval(input) { result =>
          result must haveSize(1)
          
          val result2 = result collect {
            case (ids, SObject(obj)) if ids.isEmpty => obj
          }
          
          result2 must contain(Map())
        }
      }
      
      "push_array" >> {
        val line = Line(0, "")
        val input = Root(line, PushArray)
        
        testEval(input) { result =>
          result must haveSize(1)
          
          val result2 = result collect {
            case (ids, SArray(arr)) if ids.isEmpty => arr
          }
          
          result2 must contain(Vector())
        }
      }
    }
    
    "evaluate a load_local" in {
      val line = Line(0, "")
      val input = dag.LoadLocal(line, Root(line, PushString("/hom/numbers")))

      testEval(input) { result =>
        result must haveSize(5)
        
        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
        }
        
        result2 must contain(42, 12, 77, 1, 13)
      }
    }
    
    "evaluate a join given a relative path" in {
      val line = Line(0, "")

      val numbers = dag.LoadLocal(line, Root(line, PushString("/numbers")))

      val input = Join(line, Add, IdentitySort, numbers, numbers)

      testEval(input, Path("/hom")) { result =>
        result must haveSize(5)

        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
        }

        result2 must contain(84, 24, 154, 2, 26)
      }
    }     
    
    "evaluate a join given a relative path with two different JTypes" in {
      val line = Line(0, "")

      val numbers = dag.LoadLocal(line, Root(line, PushString("/numbers")))
      val numbers0 = dag.LoadLocal(line, Root(line, PushString("/numbers")), JNumberT)

      val input = Join(line, Add, IdentitySort, numbers, numbers0)

      testEval(input, Path("/hom")) { result =>
        result must haveSize(5)

        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
        }

        result2 must contain(84, 24, 154, 2, 26)
      }
    }       
    
    "evaluate a join given a relative path with two different datasets" in {
      val line = Line(0, "")

      val numbers = dag.LoadLocal(line, Root(line, PushString("/numbers")))
      val numbers2 = dag.LoadLocal(line, Root(line, PushString("/numbers2")))

      val input = Join(line, Add, CrossLeftSort, numbers, numbers2)

      testEval(input, Path("/hom")) { result =>
        result must haveSize(30)

        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 2 => d.toInt
        }

        result2 must contain(84,54,119,43,55,43,54,24,89,13,25,13,119,89,154,78,90,78,43,13,78,2,14,2,55,25,90,14,26,14)
      }
    }       
    
    "evaluate a negation mapped over numbers" in {
      val line = Line(0, "")
      
      val input = Operate(line, Neg,
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers"))))
        
      testEval(input) { result =>
        result must haveSize(5)
        
        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
        }
        
        result2 must contain(-42, -12, -77, -1, -13)
      }
    }
    
    "evaluate a new mapped over numbers as no-op" in {
      val line = Line(0, "")
      
      val input = dag.New(line,
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers"))))
        
      testEval(input) { result =>
        result must haveSize(5)
        
        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
        }
        
        result2 must contain(42, 12, 77, 1, 13)
      }
    }

    "evaluate a join of two reductions on the same dataset" in {
      val line = Line(0, "")

      val parent = dag.LoadLocal(line, Root(line, PushString("/hom/numbers7")))

      val input = Join(line, Add, CrossRightSort, 
        dag.Reduce(line, Count, parent),
        dag.Reduce(line, Sum, parent))

      testEval(input) { result =>
        result must haveSize(1)

        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 1 => d
        }

        result2 must contain(259)
      }
    }.pendingUntilFixed 

    "join two sets" >> {
      "from different paths" >> {
        val line = Line(0, "")

        val input = Join(line, Add, CrossLeftSort,
          Join(line, DerefObject, CrossLeftSort, 
            dag.LoadLocal(line, Root(line, PushString("/clicks"))),
            Root(line, PushString("time"))),
          Join(line, DerefObject, CrossLeftSort,
            dag.LoadLocal(line, Root(line, PushString("/hom/heightWeight"))),
            Root(line, PushString("height"))))

        testEval(input) { result =>
          result must haveSize(500)
        }
      }

      "from the same path" >> {
        val line = Line(0, "")
        val heightWeight = dag.LoadLocal(line, Root(line, PushString("/hom/heightWeight")))

        val input = Join(line, Add, IdentitySort,
          Join(line, DerefObject, CrossLeftSort, 
            heightWeight,
            Root(line, PushString("weight"))),
          Join(line, DerefObject, CrossLeftSort,
            heightWeight,
            Root(line, PushString("height"))))

        testEval(input) { result =>
          result must haveSize(5)
        }
      }

      "from the same path (with a relative path)" >> {
        val line = Line(0, "")
        val heightWeight = dag.LoadLocal(line, Root(line, PushString("/heightWeight")))

        val input = Join(line, Add, IdentitySort,
          Join(line, DerefObject, CrossLeftSort, 
            heightWeight,
            Root(line, PushString("weight"))),
          Join(line, DerefObject, CrossLeftSort,
            heightWeight,
            Root(line, PushString("height"))))

        testEval(input, Path("/hom")) { result =>
          result must haveSize(5)
        }
      }
    }

    "evaluate a binary numeric operation mapped over homogeneous numeric set" >> {
      "addition" >> {
        val line = Line(0, "")
        
        val input = Join(line, Add, CrossLeftSort,
          dag.LoadLocal(line, Root(line, PushString("/hom/numbers"))),
          Root(line, PushNum("5")))
          
        testEval(input) { result =>
          result must haveSize(5)
          
          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          }
          
          result2 must contain(47, 17, 82, 6, 18)
        }
      }
      
      "subtraction" >> {
        val line = Line(0, "")
        
        val input = Join(line, Sub, CrossLeftSort,
          dag.LoadLocal(line, Root(line, PushString("/hom/numbers"))),
          Root(line, PushNum("5")))
          
        testEval(input) { result =>
          
          result must haveSize(5)
          
          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          }
          
          result2 must contain(37, 7, 72, -4, 8)
        }
      }
      
      "multiplication" >> {
        val line = Line(0, "")
        
        val input = Join(line, Mul, CrossLeftSort,
          dag.LoadLocal(line, Root(line, PushString("/hom/numbers"))),
          Root(line, PushNum("5")))
          
        testEval(input) { result =>
          result must haveSize(5)
          
          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          }
          
          result2 must contain(210, 60, 385, 5, 65)
        }
      }
      
      "division" >> {
        val line = Line(0, "")
        
        val input = Join(line, Div, CrossLeftSort,
          dag.LoadLocal(line, Root(line, PushString("/hom/numbers"))),
          Root(line, PushNum("5")))
          
        testEval(input) { result =>
          
          result must haveSize(5)
          
          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toDouble
          }
          
          result2 must contain(8.4, 2.4, 15.4, 0.2, 2.6)
        }
      }
    }
    
    "evaluate a binary numeric operation mapped over heterogeneous numeric set" >> {
      "addition" >> {
        val line = Line(0, "")
        
        val input = Join(line, Add, CrossLeftSort,
          dag.LoadLocal(line, Root(line, PushString("/het/numbers"))),
          Root(line, PushNum("5")))
          
        testEval(input) { result =>
          result must haveSize(5)
          
          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          }
          
          result2 must contain(47, 17, 82, 6, 18)
        }
      }
      
      "subtraction" >> {
        val line = Line(0, "")
        
        val input = Join(line, Sub, CrossLeftSort,
          dag.LoadLocal(line, Root(line, PushString("/het/numbers"))),
          Root(line, PushNum("5")))
          
        testEval(input) { result =>
          result must haveSize(5)
          
          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          }
          
          result2 must contain(37, 7, 72, -4, 8)
        }
      }
      
      "multiplication" >> {
        val line = Line(0, "")
        
        val input = Join(line, Mul, CrossLeftSort,
          dag.LoadLocal(line, Root(line, PushString("/het/numbers"))),
          Root(line, PushNum("5")))
          
        testEval(input) { result =>
          result must haveSize(5)
          
          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          }
          
          result2 must contain(210, 60, 385, 5, 65)
        }
      }
      
      "division" >> {
        val line = Line(0, "")
        
        val input = Join(line, Div, CrossLeftSort,
          dag.LoadLocal(line, Root(line, PushString("/het/numbers"))),
          Root(line, PushNum("5")))
          
        testEval(input) { result =>
          result must haveSize(5)
          
          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toDouble
          }
          
          result2 must contain(8.4, 2.4, 15.4, 0.2, 2.6)
        }
      }
    }

    "count a filtered dataset" in {
      val line = Line(0, "")
      val clicks = dag.LoadLocal(line, Root(line, PushString("/clicks")))

      val input = dag.Reduce(line, Count,
        Filter(line, IdentitySort,
          clicks,
          Join(line, Gt, CrossLeftSort,
            Join(line, DerefObject, CrossLeftSort,
              clicks,
              Root(line, PushString("time"))),
            Root(line, PushNum("0")))))

      testEval(input) { result =>
        result must haveSize(1)

        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.isEmpty => d.toInt
        }

        result2 must contain(100)
      }
    }

    "filter a dataset to return a set of boolean" in {
      val line = Line(0, "")

      val input = Join(line, Gt, CrossLeftSort,
            Join(line, DerefObject, CrossLeftSort,
              dag.LoadLocal(line, Root(line, PushString("/clicks"))),
              Root(line, PushString("time"))),
            Root(line, PushNum("0")))

      testEval(input) { result =>
        result must haveSize(100)

        val result2 = result collect {
          case (ids, SBoolean(d)) if ids.size == 1 => d
        }

        result2 must contain(true).only
      }
    }

    "reduce a derefed object" in {
      val line = Line(0, "")

      val input = dag.Reduce(line, Count,
        Join(line, DerefObject, CrossLeftSort,
          dag.LoadLocal(line, Root(line, PushString("/clicks"))),
          Root(line, PushString("time"))))

      testEval(input) { result =>
        result must haveSize(1)

        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.isEmpty => d.toInt
        }

        result2 must contain(100)
      }
    }

    "evaluate cross when one side is a singleton" >> {
      "a reduction on the right side of the cross" >> {
        val line = Line(0, "")

        val input = Join(line, Add, CrossLeftSort, 
          dag.LoadLocal(line, Root(line, PushString("/hom/numbers"))),
          dag.Reduce(line, Count, 
            Root(line, PushNum("42"))))

        testEval(input) { result =>
          result must haveSize(5)

          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d
          }

          result2 must contain(43, 13, 78, 2, 14)
        }
      }.pendingUntilFixed

      "a reduction on the left side of the cross" >> {
        val line = Line(0, "")

        val input = Join(line, Add, CrossLeftSort, 
          dag.Reduce(line, Count, 
            Root(line, PushNum("42"))),
          dag.LoadLocal(line, Root(line, PushString("/hom/numbers"))))

        testEval(input) { result =>
          result must haveSize(5)

          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d
          }

          result2 must contain(43, 13, 78, 2, 14)
        }
      }.pendingUntilFixed

      "a root on the right side of the cross" >> {
        val line = Line(0, "")

        val input = Join(line, Add, CrossLeftSort,  
          dag.LoadLocal(line, Root(line, PushString("/hom/numbers"))),
          Root(line, PushNum("3")))
         
        testEval(input) { result =>
          result must haveSize(5)

          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d
          }

          result2 must contain(45, 15, 80, 4, 16)
        }
      }

      "a root on the left side of the cross" >> {
        val line = Line(0, "")

        val input = Join(line, Add, CrossLeftSort, 
          Root(line, PushNum("3")),
          dag.LoadLocal(line, Root(line, PushString("/hom/numbers"))))

        testEval(input) { result =>
          result must haveSize(5)

          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d
          }

          result2 must contain(45, 15, 80, 4, 16)
        }
      }
    }

    "evaluate wrap_object on a single numeric value" in {
      val line = Line(0, "")
      
      val input = Join(line, WrapObject, CrossLeftSort,
        Root(line, PushString("answer")),
        Root(line, PushNum("42")))
        
      testEval(input) { result =>
        result must haveSize(1)
        
        val optObj = result find {
          case (ids, SObject(_)) if ids.isEmpty => true
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
    }

    "evaluate wrap_object on an object" in {
      val line = Line(0, "")
      
      val input = Join(line, WrapObject, CrossLeftSort,
        Root(line, PushString("answer")),
        Join(line, WrapObject, CrossLeftSort,
          Root(line, PushString("question")),
          Root(line, PushNull)))
        
      testEval(input) { result =>
        result must haveSize(1)
        
        val optObj = result find {
          case (ids, SObject(_)) if ids.isEmpty => true
          case _ => false
        } collect {
          case (_, SObject(obj)) => obj
        }
        
        optObj must beSome
        val obj = optObj.get
        
        obj must haveKey("answer")
        obj("answer") must beLike {
          case SObject(obj) => { 
            obj must haveKey("question")
            obj("question") mustEqual SNull
          }
        }
      }
    }
    
    "evaluate wrap_object on clicks dataset" in {
      val line = Line(0, "")
      
      val input = Join(line, WrapObject, CrossLeftSort,
        Root(line, PushString("aa")),
        Join(line, DerefObject, CrossLeftSort,
          dag.LoadLocal(line, Root(line, PushString("/clicks"))),
          Root(line, PushString("user"))))
        
      testEval(input) { result =>
        result must haveSize(100)
        
        forall(result) {
          _ must beLike {
            case (ids, SObject(obj)) if ids.size == 1 => 
              obj must haveSize(1)
              obj must haveKey("aa")
          }
        }
      }
    }
    
    "evaluate wrap_array on a single numeric value" in {
      val line = Line(0, "")
      
      val input = Operate(line, WrapArray,
        Root(line, PushNum("42")))
        
      testEval(input) { result =>
        result must haveSize(1)
        
        val optArr = result find {
          case (ids, SArray(_)) if ids.isEmpty => true
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
    }

    "evaluate wrap_array on a single null value" in {
      val line = Line(0, "")
      
      val input = Operate(line, WrapArray,
        Root(line, PushNull))
        
      testEval(input) { result =>
        result must haveSize(1)
        
        val optArr = result find {
          case (ids, SArray(_)) if ids.isEmpty => true
          case _ => false
        } collect {
          case (_, SArray(arr)) => arr
        }
        
        optArr must beSome
        val arr = optArr.get
        
        arr must haveSize(1)
        arr.head mustEqual SNull
      }
    }
    
    "evaluate join_object on single values" in {
      val line = Line(0, "")
      
      val input = Join(line, JoinObject, CrossLeftSort,
        Join(line, WrapObject, CrossLeftSort,
          Root(line, PushString("question")),
          Root(line, PushString("What is six times nine?"))),
        Join(line, WrapObject, CrossLeftSort,
          Root(line, PushString("answer")),
          Root(line, PushNum("42"))))
        
      testEval(input) { result =>
        result must haveSize(1)
        
        val optObj = result find {
          case (ids, SObject(_)) if ids.isEmpty => true
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
    }.pendingUntilFixed
    
    "evaluate join_array on single values" in {
      val line = Line(0, "")
      
      val input = Join(line, JoinArray, CrossLeftSort,
        Operate(line, WrapArray,
          Root(line, PushNum("24"))),
        Operate(line, WrapArray,
          Root(line, PushNum("42"))))
        
      testEval(input) { result =>
        result must haveSize(1)
        
        val optArr = result find {
          case (ids, SArray(_)) if ids.isEmpty => true
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
    }.pendingUntilFixed
    
    "evaluate array_swap on single values" >> {
      "at start" >> {
        val line = Line(0, "")
        
        val input = Join(line, ArraySwap, CrossLeftSort,
          Join(line, JoinArray, CrossLeftSort,
            Operate(line, WrapArray,
              Root(line, PushNum("12"))),
            Join(line, JoinArray, CrossLeftSort,
              Operate(line, WrapArray,
                Root(line, PushNum("24"))),
              Operate(line, WrapArray,
                Root(line, PushNum("42"))))),
          Root(line, PushNum("1")))
          
        testEval(input) { result =>
          result must haveSize(1)
          
          val optArr = result find {
            case (ids, SArray(_)) if ids.isEmpty => true
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
      }.pendingUntilFixed
      
      "at end" >> {
        val line = Line(0, "")
        
        val input = Join(line, ArraySwap, CrossLeftSort,
          Join(line, JoinArray, CrossLeftSort,
            Operate(line, WrapArray,
              Root(line, PushNum("12"))),
            Join(line, JoinArray, CrossLeftSort,
              Operate(line, WrapArray,
                Root(line, PushNum("24"))),
              Operate(line, WrapArray,
                Root(line, PushNum("42"))))),
          Root(line, PushNum("2")))
          
        testEval(input) { result =>
          result must haveSize(1)
          
          val optArr = result find {
            case (ids, SArray(_)) if ids.isEmpty => true
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
      }.pendingUntilFixed
    }
    
    "evaluate descent on a homogeneous set" in {
      val line = Line(0, "")
      
      val input = Join(line, DerefObject, CrossLeftSort,
        dag.LoadLocal(line, Root(line, PushString("/hom/pairs"))),
        Root(line, PushString("first")))
        
      testEval(input) { result =>
        result must haveSize(5)
        
        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
        }
        
        result2 must contain(42, 12, 77, 1, 13)
      }
    }
    
    "evaluate descent on a heterogeneous set" in {
      val line = Line(0, "")
      
      val input = Join(line, DerefObject, CrossLeftSort,
        dag.LoadLocal(line, Root(line, PushString("/het/pairs"))),
        Root(line, PushString("first")))
        
      testEval(input) { result =>
        result must haveSize(5)
        
        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          case (ids, SNull) if ids.size == 1 => SNull
        }
        
        result2 must contain(42, 12, 1, 13, SNull)
      }
    }
    
    "evaluate descent producing a heterogeneous set" in {
      val line = Line(0, "")
      
      val input = Join(line, DerefObject, CrossLeftSort,
        dag.LoadLocal(line, Root(line, PushString("/het/het-pairs"))),
        Root(line, PushString("first")))
        
      testEval(input) { result =>
        result must haveSize(5)
        
        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          case (ids, SString(str)) if ids.size == 1 => str
          case (ids, SBoolean(b)) if ids.size == 1 => b
          case (ids, SNull) if ids.size == 1 => SNull
        }
        
        result2 must contain(42, true, "daniel", 1, SNull)
      }
    }

    "an array must return an array" in {
      val line = Line(0, "")

      val input = dag.LoadLocal(line, Root(line, PushString("/hom/arrays")))

      testEval(input) { result =>
        result must haveSize(5)

        val result2 = result collect {
          case (ids, SArray(arr)) if ids.size == 1 => arr
        }

        result2 must contain(Vector(SDecimal(-9), SDecimal(-42), SDecimal(42), SDecimal(87), SDecimal(4)))
      }
    }

    "MegaReduce must return an array" in {
      val line = Line(0, "")

      val parent = dag.LoadLocal(line, Root(line, PushString("/hom/numbers")))
      val input = dag.MegaReduce(line, NEL(dag.Reduce(line, Count, parent)), parent)

      testEval(input) { result =>
        result must haveSize(1)

        val result2 = result collect {
          case (ids, SArray(arr)) if ids.size == 0 => arr
        }

        result2.size mustEqual 1
        result2.head must contain(SDecimal(5))
      }
    }
    
    "evaluate array dereference on a MegaReduce" in {
      val line = Line(0, "")
      
      val parent = dag.LoadLocal(line, Root(line, PushString("/hom/numbers")))
      val red = Count
      
      val input = Join(line, DerefArray, CrossLeftSort,
        dag.MegaReduce(line, NEL(dag.Reduce(line, red, parent)), parent),
        Root(line, PushNum("0")))
        
      testEval(input) { result =>
        result must haveSize(1)
        
        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 0 => d.toInt
        }
        
        result2 must contain(5)
      }
    }    

    "evaluate array dereference on a homogeneous set" in {
      val line = Line(0, "")
      
      val input = Join(line, DerefArray, CrossLeftSort,
        dag.LoadLocal(line, Root(line, PushString("/hom/arrays"))),
        Root(line, PushNum("2")))
        
      testEval(input) { result =>
        result must haveSize(5)
        
        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
        }
        
        result2 must contain(42, 12, 77, 1, 13)
      }
    }
    
    "evaluate array dereference on a heterogeneous set" in {
      val line = Line(0, "")
      
      val input = Join(line, DerefArray, CrossLeftSort,
        dag.LoadLocal(line, Root(line, PushString("/het/arrays"))),
        Root(line, PushNum("2")))
        
      testEval(input) { result =>
        result must haveSize(5)
        
        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
        }
        
        result2 must contain(42, 12, 77, 1, 13)
      }
    }
    
    "evaluate array dereference producing a heterogeneous set" in {
      val line = Line(0, "")
      
      val input = Join(line, DerefArray, CrossLeftSort,
        dag.LoadLocal(line, Root(line, PushString("/het/het-arrays"))),
        Root(line, PushNum("2")))
        
      testEval(input) { result =>
        result must haveSize(5)
        
        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          case (ids, SString(str)) if ids.size == 1 => str
          case (ids, SBoolean(b)) if ids.size == 1 => b
          case (ids, SNull) if ids.size == 1 => SNull
        }
        
        result2 must contain(42, true, "daniel", 1, SNull)
      }
    }
    
    "evaluate matched binary numeric operation" in {
      val line = Line(0, "")
      val pairs = dag.LoadLocal(line, Root(line, PushString("/hom/pairs")))
      
      val input = Join(line, Sub, IdentitySort,
        Join(line, DerefObject, CrossLeftSort,
          pairs,
          Root(line, PushString("first"))),
        Join(line, DerefObject, CrossLeftSort,
          pairs,
          Root(line, PushString("second"))))
        
      testEval(input) { result =>
        result must haveSize(5)
        
        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
        }
        
        result2 must contain(36, 12, 115, -165)
      }
    }
    
    "evaluate matched binary numeric operation dropping undefined result" in {
      val line = Line(0, "")
      val pairs = dag.LoadLocal(line, Root(line, PushString("/hom/pairs")))
      
      val input = Join(line, Div, IdentitySort,
        Join(line, DerefObject, CrossLeftSort,
          pairs,
          Root(line, PushString("first"))),
        Join(line, DerefObject, CrossLeftSort,
          pairs,
          Root(line, PushString("second"))))
        
      testEval(input) { result =>
        result must haveSize(4)
        
        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 1 => d.toDouble
        }
        
        result2 must contain(7, -2.026315789473684, 0.006024096385542169, 13)
      }
    }    

    "evaluate matched binary numeric operation dropping undefined result (with relative path)" in {
      val line = Line(0, "")
      val pairs = dag.LoadLocal(line, Root(line, PushString("/pairs")))
      
      val input = Join(line, Div, IdentitySort,
        Join(line, DerefObject, CrossLeftSort,
          pairs,
          Root(line, PushString("first"))),
        Join(line, DerefObject, CrossLeftSort,
          pairs,
          Root(line, PushString("second"))))
        
      testEval(input, Path("/hom")) { result =>
        result must haveSize(4)
        
        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 1 => d.toDouble
        }
        
        result2 must contain(7, -2.026315789473684, 0.006024096385542169, 13)
      }
    }
    
    "compute the set difference of two sets" in {
      val line = Line(0, "")
      val clicks2 = dag.LoadLocal(line, Root(line, PushString("/clicks2")))
      
      val input = Diff(line,
        clicks2,
        Join(line, DerefObject, CrossLeftSort,
          clicks2,
          Root(line, PushString("time"))))
        
      testEval(input) { result =>
        result must haveSize(6)
        
        forall(result) {
          _ must beLike {
            case (ids, SObject(obj)) if ids.size == 1 => 
              obj must not haveKey("time")
  
            case (ids, SString(s)) if ids.size == 1 => 
              s mustEqual "string cheese"
          }
        }
      }
    }.pendingUntilFixed
    
    "compute the set difference of the set difference" in {
      val line = Line(0, "")
      val clicks2 = dag.LoadLocal(line, Root(line, PushString("/clicks2")))
      
      val input = Diff(line,
        clicks2,
        Diff(line,
          clicks2,
          Join(line, DerefObject, CrossLeftSort,
            clicks2,
            Root(line, PushString("time")))))

      testEval(input) { result =>
        result must haveSize(101)
        
        forall(result) {
          _ must beLike {
            case (ids, SObject(obj)) if ids.size == 1 => 
              obj must haveKey("time")
  
            case (ids, SString(s)) if ids.size == 1 => 
              s mustEqual "string cheese"
          }
        }
      }
    }.pendingUntilFixed
    
    "compute the iunion of two homogeneous sets" in {
      val line = Line(0, "")
      
      val input = IUI(line, true,
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers"))),
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers3"))))
        
      testEval(input) { result =>
        result must haveSize(10)
        
        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 1 => d.toDouble
        }
        
        result2 must contain(42, 12, 77, 1, 13, 14, -1, 0)
      }
    }.pendingUntilFixed    

    "compute the iunion of two homogeneous sets (with relative path)" in {
      val line = Line(0, "")
      
      val input = IUI(line, true,
        dag.LoadLocal(line, Root(line, PushString("/numbers"))),
        dag.LoadLocal(line, Root(line, PushString("/numbers3"))))
        
      testEval(input, Path("/hom")) { result =>
        result must haveSize(10)
        
        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 1 => d.toDouble
        }
        
        result2 must contain(42, 12, 77, 1, 13, 14, -1, 0)
      }
    }.pendingUntilFixed
    
    "compute the iunion of two datasets" in {
      val line = Line(0, "")
      
      val input = IUI(line, true,
        dag.LoadLocal(line, Root(line, PushString("/clicks"))),
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers3"))))
        
      testEval(input) { result =>
        result must haveSize(105)
      }
    }.pendingUntilFixed
    
    "compute the iintersect of two homogeneous sets" in {
      val line = Line(0, "")
      
      val input = IUI(line, false,
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers"))),
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers4"))))
        
      testEval(input) { result =>
        result must haveSize(2)
        
        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 1 => d.toDouble
        }
        
        result2 must contain(1, 42)
      }
    }.pendingUntilFixed

    "compute the iintersect of two heterogeneous sets" in {
      val line = Line(0, "")
      
      val input = IUI(line, false,
        dag.LoadLocal(line, Root(line, PushString("/het/numbers"))),
        dag.LoadLocal(line, Root(line, PushString("/het/numbers4"))))
        
      testEval(input) { result =>
        result must haveSize(7)

        forall(result) {
          _ must beLike {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toDouble must beOneOf(1, 42)
            case (ids, SBoolean(d)) if ids.size == 1 => d must beOneOf(true, false)
            case (ids, SObject(d)) if ids.size == 1 => {
              d must haveKey("test")
              d must haveValue("fubar")
            }
            case (ids, SString(d)) if ids.size == 1 => d mustEqual "daniel"
            case (ids, SArray(d)) if ids.size == 1 => d.size mustEqual 0
          }
        }
      }
    }.pendingUntilFixed
    
    "compute the iintersect of two nonintersecting sets of numbers" in {
      val line = Line(0, "")
      
      val input = IUI(line, false,
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers"))),
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers3"))))
        
      testEval(input) { result =>
        result must haveSize(0)
      }
    }

    "compute the iintersect of two nonintersecting datasets" in {
      val line = Line(0, "")
      
      val input = IUI(line, false,
        dag.LoadLocal(line, Root(line, PushString("/clicks"))),
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers3"))))
        
      testEval(input) { result =>
        result must haveSize(0)
      }
    }
    
    "filter homogeneous numeric set by binary operation" >> {
      //"less-than" >> {
      //  val line = Line(0, "")
      //  val numbers = dag.LoadLocal(line, Root(line, PushString("/hom/numbers")))
      //  
      //  val input = Filter(line, IdentitySort,
      //    numbers,
      //    Join(line, Lt, CrossLeftSort,
      //      numbers,
      //      Root(line, PushNum("13"))))
      //    
      //  testEval(input) { result =>
      //    result must haveSize(2)
      //    
      //    val result2 = result collect {
      //      case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
      //    }
      //    
      //    result2 must contain(1, 12)
      //  }
      //}      
      //
      //"less-than (with relative paths)" >> {
      //  val line = Line(0, "")
      //  val numbers = dag.LoadLocal(line, Root(line, PushString("/numbers")))
      //  
      //  val input = Filter(line, IdentitySort,
      //    numbers,
      //    Join(line, Lt, CrossLeftSort,
      //      numbers,
      //      Root(line, PushNum("13"))))
      //    
      //  testEval(input, Path("/hom")) { result =>
      //    result must haveSize(2)
      //    
      //    val result2 = result collect {
      //      case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
      //    }
      //    
      //    result2 must contain(1, 12)
      //  }
      //}
      //
      //"less-than-equal" >> {
      //  val line = Line(0, "")
      //  val numbers = dag.LoadLocal(line, Root(line, PushString("/hom/numbers")))
      //  
      //  val input = Filter(line, IdentitySort,
      //    numbers,
      //    Join(line, LtEq, CrossLeftSort,
      //      numbers,
      //      Root(line, PushNum("13"))))
      //    
      //  testEval(input) { result =>
      //    result must haveSize(3)
      //    
      //    val result2 = result collect {
      //      case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
      //    }
      //    
      //    result2 must contain(12, 1, 13)
      //  }
      //}
      //
      //"greater-than" >> {
      //  val line = Line(0, "")
      //  val numbers = dag.LoadLocal(line, Root(line, PushString("/hom/numbers")))
      //  
      //  val input = Filter(line, IdentitySort,
      //    numbers,
      //    Join(line, Gt, CrossLeftSort,
      //      numbers,
      //      Root(line, PushNum("13"))))
      //    
      //  testEval(input) { result =>
      //    result must haveSize(2)
      //    
      //    val result2 = result collect {
      //      case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
      //    }
      //    
      //    result2 must contain(42, 77)
      //  }
      //}
      //
      //"greater-than-equal" >> {
      //  val line = Line(0, "")
      //  val numbers = dag.LoadLocal(line, Root(line, PushString("/hom/numbers")))
      //  
      //  val input = Filter(line, IdentitySort,
      //    numbers,
      //    Join(line, GtEq, CrossLeftSort,
      //      numbers,
      //      Root(line, PushNum("13"))))
      //    
      //  testEval(input) { result =>
      //    result must haveSize(3)
      //    
      //    val result2 = result collect {
      //      case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
      //    }
      //    
      //    result2 must contain(42, 77, 13)
      //  }
      //}
      
      "equal with a number literal" >> {
        val line = Line(0, "")
        val numbers = dag.LoadLocal(line, Root(line, PushString("/hom/numbers")))
        
        val input = Filter(line, IdentitySort,
          numbers,
          Join(line, Eq, CrossLeftSort,
            numbers,
            Root(line, PushNum("13"))))
          
        testEval(input) { result =>
          result must haveSize(1)
          
          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          }
          
          result2 must contain(13)
        }
      }       

      //"equal without a filter" >> {
      //  val line = Line(0, "")
      //  
      //  val input = Join(line, Eq, CrossLeftSort,
      //      dag.LoadLocal(line, Root(line, PushString("/hom/numbers"))),
      //      Root(line, PushNum("13")))
      //    
      //  testEval(input) { result =>
      //    result must haveSize(5)
      //    
      //    val result2 = result collect {
      //      case (ids, SBoolean(d)) if ids.size == 1 => d
      //    }
      //    
      //    result2 must contain(true, false)
      //  }
      //}
      //
      //"not-equal" >> {
      //  val line = Line(0, "")
      //  val numbers = dag.LoadLocal(line, Root(line, PushString("/hom/numbers")))
      //  
      //  val input = Filter(line, IdentitySort,
      //    numbers,
      //    Join(line, NotEq, CrossLeftSort,
      //      numbers,
      //      Root(line, PushNum("13"))))
      //    
      //  testEval(input) { result =>
      //    val result2 = result collect {
      //      case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
      //    }
      //    
      //    result2 must contain(42, 12, 77, 1)
      //  }
      //}
      //
      //"and" >> {
      //  val line = Line(0, "")
      //  val numbers = dag.LoadLocal(line, Root(line, PushString("/hom/numbers")))
      //  
      //  val input = Filter(line, IdentitySort,
      //    numbers,
      //    Join(line, And, IdentitySort,
      //      Join(line, NotEq, CrossLeftSort,
      //        numbers,
      //        Root(line, PushNum("77"))),
      //      Join(line, NotEq, CrossLeftSort,
      //        numbers,
      //        Root(line, PushNum("13")))))
      //    
      //  testEval(input) { result =>
      //    result must haveSize(3)
      //    
      //    val result2 = result collect {
      //      case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
      //    }
      //    
      //    result2 must contain(42, 12, 1)
      //  }
      //}      

      //"and (with relative paths)" >> {
      //  val line = Line(0, "")
      //  val numbers = dag.LoadLocal(line, Root(line, PushString("/numbers")))
      //  
      //  val input = Filter(line, IdentitySort,
      //    numbers,
      //    Join(line, And, IdentitySort,
      //      Join(line, NotEq, CrossLeftSort,
      //        numbers,
      //        Root(line, PushNum("77"))),
      //      Join(line, NotEq, CrossLeftSort,
      //        numbers,
      //        Root(line, PushNum("13")))))
      //    
      //  testEval(input, Path("/hom")) { result =>
      //    result must haveSize(3)
      //    
      //    val result2 = result collect {
      //      case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
      //    }
      //    
      //    result2 must contain(42, 12, 1)
      //  }
      //}
      //
      //"or" >> {
      //  val line = Line(0, "")
      //  val numbers = dag.LoadLocal(line, Root(line, PushString("/hom/numbers")))
      //  
      //  val input = Filter(line, IdentitySort,
      //    numbers,
      //    Join(line, Or, IdentitySort,
      //      Join(line, Eq, CrossLeftSort,
      //        numbers,
      //        Root(line, PushNum("77"))),
      //      Join(line, Eq, CrossLeftSort,
      //        numbers,
      //        Root(line, PushNum("13")))))
      //    
      //  testEval(input) { result =>
      //    result must haveSize(2)
      //    
      //    val result2 = result collect {
      //      case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
      //    }
      //    
      //    result2 must contain(77, 13)
      //  }
      //}
      //
      //"complement of equality" >> {
      //  val line = Line(0, "")
      //  val numbers = dag.LoadLocal(line, Root(line, PushString("/hom/numbers")))
      //  
      //  val input = Filter(line, IdentitySort,
      //    numbers,
      //    Operate(line, Comp,
      //      Join(line, Eq, CrossLeftSort,
      //        numbers,
      //        Root(line, PushNum("13")))))
      //    
      //  testEval(input) { result =>
      //    result must haveSize(4)
      //    
      //    val result2 = result collect {
      //      case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
      //    }
      //    
      //    result2 must contain(42, 12, 77, 1)
      //  }
      //}
    }
    
    "filter heterogeneous numeric set by binary operation" >> {
      "less-than" >> {
        val line = Line(0, "")
        val numbers = dag.LoadLocal(line, Root(line, PushString("/het/numbers")))
        
        val input = Filter(line, IdentitySort,
          numbers,
          Join(line, Lt, CrossLeftSort,
            numbers,
            Root(line, PushNum("13"))))
          
        testEval(input) { result =>
          result must haveSize(2)
          
          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          }
          
          result2 must contain(1, 12)
        }
      }
      
      "less-than-equal" >> {
        val line = Line(0, "")
        val numbers = dag.LoadLocal(line, Root(line, PushString("/het/numbers")))
        
        val input = Filter(line, IdentitySort,
          numbers,
          Join(line, LtEq, CrossLeftSort,
            numbers,
            Root(line, PushNum("13"))))
          
        testEval(input) { result =>
          result must haveSize(3)
          
          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          }
          
          result2 must contain(12, 1, 13)
        }
      }      

      "less-than-equal (with relative path)" >> {
        val line = Line(0, "")
        val numbers = dag.LoadLocal(line, Root(line, PushString("/numbers")))
        
        val input = Filter(line, IdentitySort,
          numbers,
          Join(line, LtEq, CrossLeftSort,
            numbers,
            Root(line, PushNum("13"))))
          
        testEval(input, Path("/het")) { result =>
          result must haveSize(3)
          
          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          }
          
          result2 must contain(12, 1, 13)
        }
      }
      
      "greater-than" >> {
        val line = Line(0, "")
        val numbers = dag.LoadLocal(line, Root(line, PushString("/het/numbers")))
        
        val input = Filter(line, IdentitySort,
          numbers,
          Join(line, Gt, CrossLeftSort,
            numbers,
            Root(line, PushNum("13"))))
          
        testEval(input) { result =>
          result must haveSize(2)
          
          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          }
          
          result2 must contain(42, 77)
        }
      }
      
      "greater-than-equal" >> {
        val line = Line(0, "")
        val numbers = dag.LoadLocal(line, Root(line, PushString("/het/numbers")))
        
        val input = Filter(line, IdentitySort,
          numbers,
          Join(line, GtEq, CrossLeftSort,
            numbers,
            Root(line, PushNum("13"))))
          
        testEval(input) { result =>
          result must haveSize(3)
          
          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          }
          
          result2 must contain(42, 77, 13)
        }
      }
      
      "equal with boolean set as the source" >> {
        val line = Line(0, "")
        val numbers = dag.LoadLocal(line, Root(line, PushString("/het/numbers")))
        
        val input = Filter(line, IdentitySort,
          Join(line, Eq, CrossLeftSort,
            numbers,
            Root(line, PushNum("13"))),
          Join(line, Eq, CrossLeftSort,
            numbers,
            Root(line, PushNum("13"))))
          
        testEval(input) { result =>
          result must haveSize(1)
          
          val result2 = result collect {
            case (ids, SBoolean(d)) if ids.size == 1 => d
          }
          
          result2 must contain(true)
        }
      }      

      "equal" >> {
        val line = Line(0, "")
        val numbers = dag.LoadLocal(line, Root(line, PushString("/het/numbers")))
        
        val input = Filter(line, IdentitySort,
          numbers,
          Join(line, Eq, CrossLeftSort,
            numbers,
            Root(line, PushNum("13"))))
          
        testEval(input) { result =>
          result must haveSize(1)
          
          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          }
          
          result2 must contain(13)
        }
      }      

      "equal with empty array" >> {
        val line = Line(0, "")
        val numbers9 = dag.LoadLocal(line, Root(line, PushString("/het/numbers9")))
        
        val input = Filter(line, IdentitySort,
          numbers9,
          Join(line, Eq, CrossLeftSort,
            numbers9,
            Root(line, PushArray)))
          
        testEval(input) { result =>
          result must haveSize(1)
          
          val result2 = result collect {
            case (ids, SArray(arr)) if ids.size == 1 => arr
          }
          
          result2 must contain(Vector())
        }
      }       

      "equal with empty object" >> {
        val line = Line(0, "")
        val numbers9 = dag.LoadLocal(line, Root(line, PushString("/het/numbers9")))
        
        val input = Filter(line, IdentitySort,
          numbers9,
          Join(line, Eq, CrossLeftSort,
            numbers9,
            Root(line, PushObject)))
          
        testEval(input) { result =>
          result must haveSize(1)
          
          val result2 = result collect {
            case (ids, SObject(obj)) if ids.size == 1 => obj
          }
          
          result2 must contain(Map())
        }
      } 

      "equal with an array" >> {
        val line = Line(0, "")
        val numbers = dag.LoadLocal(line, Root(line, PushString("/het/numbers")))
        
        val input = Filter(line, IdentitySort,
          numbers,
          Join(line, Eq, CrossLeftSort,
            numbers,
            Join(line, JoinArray, CrossLeftSort,
              Join(line, JoinArray, CrossLeftSort,
                Operate(line, WrapArray, Root(line, PushNum("9"))),
                Operate(line, WrapArray, Root(line, PushNum("10")))),
              Operate(line, WrapArray, Root(line, PushNum("11"))))))


        testEval(input) { result =>
          result must haveSize(1)

          val result2 = result collect {
            case (ids, SArray(arr)) if ids.size == 1 => arr
          }

          result2 must contain(Vector(8, 9, 10))
        }
      }.pendingUntilFixed  
      
      "equal with an object" >> {
        val line = Line(0, "")
        val numbers = dag.LoadLocal(line, Root(line, PushString("/het/numbers")))
        
        val input = Filter(line, IdentitySort,
          numbers,
          Join(line, Eq, CrossLeftSort,
            numbers,
              Join(line, WrapObject, CrossLeftSort,
                Root(line, PushString("foo")),
                Root(line, PushString("bar")))))

        testEval(input) { result =>
          result must haveSize(1)

          forall(result) {
            _ must beLike {
              case (ids, SObject(obj)) if ids.size == 1 => {
                obj must haveKey("foo")
                obj must haveValue("bar")
              }
            }
          }
        }
      }.pendingUntilFixed 

      "equal without a filter" >> {
        val line = Line(0, "")
        
        val input = Join(line, Eq, CrossLeftSort,
            dag.LoadLocal(line, Root(line, PushString("/het/numbers"))),
            Root(line, PushNum("13")))
          
        testEval(input) { result =>
          result must haveSize(10)
          
          val result2 = result.groupBy {
            case (ids, SBoolean(d)) if ids.size == 1 => Some(d)
            case _                                   => None
          }
          
          result2.keySet must contain(Some(true), Some(false))
          result2(Some(true)).size mustEqual 1
          result2(Some(false)).size mustEqual 9
        }
      }
      
      "not equal without a filter" >> {
        val line = Line(0, "")
        
        val input = Join(line, NotEq, CrossLeftSort,
            dag.LoadLocal(line, Root(line, PushString("/het/numbers"))),
            Root(line, PushNum("13")))
          
        testEval(input) { result =>
          result must haveSize(10)
          
          val result2 = result.groupBy {
            case (ids, SBoolean(d)) if ids.size == 1 => Some(d)
            case _                                   => None
          }
          
          result2.keySet must contain(Some(true), Some(false))
          result2(Some(true)).size mustEqual 9
          result2(Some(false)).size mustEqual 1
        }
      }
      
      "not-equal" >> {
        val line = Line(0, "")
        val numbers = dag.LoadLocal(line, Root(line, PushString("/het/numbers")))
        
        val input = Filter(line, IdentitySort,
          numbers,
          Join(line, NotEq, CrossLeftSort,
            numbers,
            Root(line, PushNum("13"))))
          
        testEval(input) { result =>
          result must haveSize(9)
          
          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
            case (ids, SBoolean(b)) if ids.size == 1 => b
            case (ids, SString(str)) if ids.size == 1 => str
            case (ids, SObject(obj)) if ids.size == 1 => obj
            case (ids, SArray(arr)) if ids.size == 1 => arr
          }
          
          result2 must contain(42, 12, 77, 1, true, false, "daniel",
            Map("test" -> SString("fubar")), Vector())
        }
      }
      
      "not-equal with empty array" >> {
        val line = Line(0, "")
        val numbers = dag.LoadLocal(line, Root(line, PushString("/het/numbers10")))
        
        val input = Filter(line, IdentitySort,
          numbers,
          Join(line, NotEq, CrossLeftSort,
            numbers,
            Root(line, PushArray)))
          
        testEval(input) { result =>
          result must haveSize(3)
          
          val result2 = result collect {
            case (ids, SArray(arr)) if ids.size == 1 => arr
            case (ids, SObject(obj)) if ids.size == 1 => obj
          }
          
          result2 must contain(Vector(SDecimal(9), SDecimal(10)), Map.empty[String, SValue], Map("foo" -> SNull))
        }
      }       

      "not-equal with empty object" >> {
        val line = Line(0, "")
        val numbers = dag.LoadLocal(line, Root(line, PushString("/het/numbers10")))
        
        val input = Filter(line, IdentitySort,
          numbers,
          Join(line, NotEq, CrossLeftSort,
            numbers,
            Root(line, PushObject)))
          
        testEval(input) { result =>
          result must haveSize(3)
          
          val result2 = result collect {
            case (ids, SArray(arr)) if ids.size == 1 => arr
            case (ids, SObject(obj)) if ids.size == 1 => obj
          }
          
          result2 must contain(Vector.empty[SValue], Vector(SDecimal(9), SDecimal(10)), Map("foo" -> SNull))
        }
      } 

      "not-equal with an array" >> {
        val line = Line(0, "")
        val numbers = dag.LoadLocal(line, Root(line, PushString("/het/numbers10")))
        
        val input = Filter(line, IdentitySort,
          numbers,
          Join(line, NotEq, CrossLeftSort,
            numbers,
            Join(line, JoinArray, CrossLeftSort,
              Operate(line, WrapArray, Root(line, PushNum("9"))),
              Operate(line, WrapArray, Root(line, PushNum("10"))))))


        testEval(input) { result =>
          result must haveSize(3)
          
          val result2 = result collect {
            case (ids, SArray(arr)) if ids.size == 1 => arr
            case (ids, SObject(obj)) if ids.size == 1 => obj
          }
          
          result2 must contain(Map.empty[String, SValue], Vector.empty[SValue], Map("foo" -> SNull))
 
        }
      }.pendingUntilFixed  
      
      "not-equal with an object" >> {
        val line = Line(0, "")
        val numbers = dag.LoadLocal(line, Root(line, PushString("/het/numbers10")))
        
        val input = Filter(line, IdentitySort,
          numbers,
          Join(line, NotEq, CrossLeftSort,
            numbers,
            Join(line, WrapObject, CrossLeftSort,
              Root(line, PushString("foo")),
              Root(line, PushString("bar")))))

        testEval(input) { result =>
          result must haveSize(3)

          val result2 = result collect {
            case (ids, SArray(arr)) if ids.size == 1 => arr
            case (ids, SObject(obj)) if ids.size == 1 => obj
          }
          
          result2 must contain(Vector.empty[SValue], Vector(SDecimal(9), SDecimal(10)), Map.empty[String, SValue])

        }.pendingUntilFixed  //TODO first place to look:  buildWrappedCrossSpec
      }

      "and" >> {
        val line = Line(0, "")
        val numbers = dag.LoadLocal(line, Root(line, PushString("/het/numbers")))
        
        val input = Filter(line, IdentitySort,
          numbers,
          Join(line, And, IdentitySort,
            Join(line, NotEq, CrossLeftSort,
              numbers,
              Root(line, PushNum("77"))),
            Join(line, NotEq, CrossLeftSort,
              numbers,
              Root(line, PushNum("13")))))
          
        testEval(input) { result =>
          result must haveSize(8)
          
          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
            case (ids, SBoolean(b)) if ids.size == 1 => b
            case (ids, SString(str)) if ids.size == 1 => str
            case (ids, SObject(obj)) if ids.size == 1 => obj
            case (ids, SArray(arr)) if ids.size == 1 => arr
          }
          
          result2 must contain(42, 12, 1, true, false, "daniel",
            Map("test" -> SString("fubar")), Vector())
        }
      }.pendingUntilFixed
      
      "or" >> {
        val line = Line(0, "")
        val numbers = dag.LoadLocal(line, Root(line, PushString("/het/numbers")))
        
        val input = Filter(line, IdentitySort,
          numbers,
          Join(line, Or, IdentitySort,
            Join(line, Eq, CrossLeftSort,
              numbers,
              Root(line, PushNum("77"))),
            Join(line, Eq, CrossLeftSort,
              numbers,
              Root(line, PushNum("13")))))
          
        testEval(input) { result =>
          result must haveSize(2)
          
          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          }
          
          result2 must contain(77, 13)
        }
      }.pendingUntilFixed
      
      "complement of equality" >> {
        val line = Line(0, "")
        val numbers = dag.LoadLocal(line, Root(line, PushString("/het/numbers")))
        
        val input = Filter(line, IdentitySort,
          numbers,
          Operate(line, Comp,
            Join(line, Eq, CrossLeftSort,
              numbers,
              Root(line, PushNum("13")))))
          
        testEval(input) { result =>
          result must haveSize(9)
          
          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
            case (ids, SBoolean(b)) if ids.size == 1 => b
            case (ids, SString(str)) if ids.size == 1 => str
            case (ids, SObject(obj)) if ids.size == 1 => obj
            case (ids, SArray(arr)) if ids.size == 1 => arr
          }
          
          result2 must contain(42, 12, 77, 1, true, false, "daniel",
            Map("test" -> SString("fubar")), Vector())
        }
      }
    }
    
    "correctly order a match following a cross" in {
      val line = Line(0, "")
        
      val numbers = dag.LoadLocal(line, Root(line, PushString("/hom/numbers")))
      
      val input = Join(line, Mul, IdentitySort,
        numbers,
        Join(line, Sub, CrossLeftSort,
          numbers,
          dag.LoadLocal(line, Root(line, PushString("/hom/numbers3")))))
          
      testEval(input) { result =>
        result must haveSize(25)
        
        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 2 => d.toInt
        }
        
        result2 must haveSize(23)
        
        result2 must contain(0, -377, -780, 6006, -76, 5929, 1, 156, 169, 2, 1764,
          2695, 144, 1806, -360, 1176, -832, 182, 4851, -1470, -13, -41, -24)
      }
    }.pendingUntilFixed
    
    "correctly evaluate a match following a cross with equality" in {
      val line = Line(0, "")
      
      val numbers = dag.LoadLocal(line, Root(line, PushString("/hom/numbers")))
      val numbers3 = dag.LoadLocal(line, Root(line, PushString("/hom/numbers3")))
      
      val input = Join(line, And, IdentitySort,
        Join(line, And, CrossLeftSort,
          Join(line, Eq, IdentitySort, numbers, numbers),
          Join(line, Eq, IdentitySort, numbers3, numbers3)),
        Join(line, Eq, IdentitySort, numbers3, numbers3))
      
      testEval(input) { _ must not(beEmpty) }
    }.pendingUntilFixed
    
    "correctly order a match following a cross within a new" in {
      val line = Line(0, "")
      val numbers = dag.LoadLocal(line, Root(line, PushString("/hom/numbers")))
      
      val input = Join(line, Mul, IdentitySort,
        numbers,
        Join(line, Sub, CrossLeftSort,
          numbers, 
          dag.New(line, numbers)))
          
      testEval(input) { result =>
        result must haveSize(25)
        
        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 2 => d.toInt
        }
        
        result2 must haveSize(20)
        
        result2 must contain(0, 1260, -1470, 1722, 1218, -360, -780, 132, -12,
          2695, 5005, 5852, 4928, -41, -11, -76, -377, 13, -832, 156)
      }
    }.pendingUntilFixed
    
    "split on a homogeneous set" in {
      val line = Line(0, "")
      
      // 
      // nums := dataset(//hom/numbers)
      // sums('n) :=
      //   m := max(nums where nums < 'n)
      //   (nums where nums = 'n) + m     -- actually, we used split root, but close enough
      // sums
      // 
       
      val nums = dag.LoadLocal(line, Root(line, PushString("/hom/numbers")))
      
      lazy val input: dag.Split = dag.Split(line,
        dag.Group(1, nums, UnfixedSolution(0, nums)),
        Join(line, Add, CrossLeftSort,
          SplitGroup(line, 1, nums.identities)(input),
          dag.Reduce(line, Max,
            Filter(line, IdentitySort,
              nums,
              Join(line, Lt, CrossLeftSort,
                nums,
                SplitParam(line, 0)(input))))))
              
      testEval(input) { result =>
        result must haveSize(4)
        
        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
        }
        
        result2 must contain(55, 13, 119, 25)
      }
    }.pendingUntilFixed
    
    "evaluate a histogram function" in {
      val Expected = Map("daniel" -> 9, "kris" -> 8, "derek" -> 7, "nick" -> 17,
        "john" -> 13, "alissa" -> 7, "franco" -> 13, "matthew" -> 10, "jason" -> 13, SNull -> 3)
      
      val line = Line(0, "")
      
      // 
      // clicks := dataset(//clicks)
      // histogram('user) :=
      //   { user: 'user, num: count(clicks where clicks.user = 'user) }
      // histogram
      // 
      // 
      val clicks = dag.LoadLocal(line, Root(line, PushString("/clicks")))
       
      lazy val input: dag.Split = dag.Split(line,
        dag.Group(1,
          clicks,
          UnfixedSolution(0, 
            Join(line, DerefObject, CrossLeftSort,
              clicks,
              Root(line, PushString("user"))))),
        Join(line, JoinObject, CrossLeftSort,
          Join(line, WrapObject, CrossLeftSort,
            Root(line, PushString("user")),
            SplitParam(line, 0)(input)),
          Join(line, WrapObject, CrossLeftSort,
            Root(line, PushString("num")),
            dag.Reduce(line, Count,
              SplitGroup(line, 1, clicks.identities)(input)))))
      
      testEval(input) { result =>
        result must haveSize(10)
        
        forall(result) {
          _ must beLike {
            case (ids, SObject(obj)) if ids.size == 1 => {
              obj must haveKey("user")
              obj must haveKey("num")
              
              obj("user") must beLike {
                case SString(str) => {
                  str must beOneOf("daniel", "kris", "derek", "nick", "john",
                    "alissa", "franco", "matthew", "jason")
                }
                case SNull => ok
              }
  
              val user = (obj("user") : @unchecked) match {
                case SString(user) => user
                case SNull => SNull
              }
                
              obj("num") must beLike {
                case SDecimal(d) => d mustEqual Expected(user)
              }
            }
          }
        }
      }
    }.pendingUntilFixed

    "evaluate with on the clicks dataset" in {
      val line = Line(0, "")
      
      val input = Join(line, JoinObject, CrossLeftSort,
        dag.LoadLocal(line, Root(line, PushString("/clicks"))),
        Join(line, WrapObject, CrossLeftSort,
          Root(line, PushString("t")),
          Root(line, PushNum("42"))))
          
      testEval(input) { result =>
        result must haveSize(100)
        
        forall(result) {
          case (ids, SObject(obj)) if ids.size == 1 => 
            obj must haveKey("user")
            obj must haveKey("time")
            obj must haveKey("page")
            obj must haveKey("t")
            
            obj("t") mustEqual SDecimal(42)
          
          case _ => failure("Result has wrong shape")
        }
      }
    }.pendingUntilFixed
    
    "evaluate filter with null" in {
      val line = Line(0, "")
      val clicks = dag.LoadLocal(line, Root(line, PushString("/clicks")))

      //
      // //clicks where //clicks.user = null
      //
      //
      val input = Filter(line, IdentitySort,
        clicks,
        Join(line, Eq, CrossLeftSort,
          Join(line, DerefObject, CrossLeftSort,
            clicks,
            Root(line, PushString("user"))),
          Root(line, PushNull)))

      testEval(input) { result =>
        result must haveSize(3)

        forall(result) {
          _ must beLike {
            case (ids, SObject(obj)) if ids.size == 1 => 
              obj must haveKey("user")
              obj("user") must_== SNull
          }
        }
      }
    }

    "evaluate filter on the results of a histogram function" in {
      val line = Line(0, "")
      
      // 
      // clicks := //clicks
      // histogram('user) :=
      //   { user: 'user, num: count(clicks where clicks.user = 'user) }
      // histogram where histogram.num = 9
      // 
      // 
      val clicks = dag.LoadLocal(line, Root(line, PushString("/clicks")))
       
      lazy val histogram: dag.Split = dag.Split(line,
        dag.Group(1,
          clicks,
          UnfixedSolution(0,
            Join(line, DerefObject, CrossLeftSort,
              clicks,
              Root(line, PushString("user"))))),
        Join(line, JoinObject, CrossLeftSort,
          Join(line, WrapObject, CrossLeftSort,
            Root(line, PushString("user")),
            SplitParam(line, 0)(histogram)),
          Join(line, WrapObject, CrossLeftSort,
            Root(line, PushString("num")),
            dag.Reduce(line, Count,
              SplitGroup(line, 1, clicks.identities)(histogram)))))
       
      val input = Filter(line, IdentitySort,
        histogram,
        Join(line, Eq, CrossLeftSort,
          Join(line, DerefObject, CrossLeftSort,
            histogram,
            Root(line, PushString("num"))),
          Root(line, PushNum("9"))))
                  
      testEval(input) { result =>
        result must haveSize(1)
        result.toList.head must beLike {
          case (ids, SObject(obj)) if ids.size == 1 => {
            obj must haveKey("user")
            obj("user") must beLike { case SString("daniel") => ok }
            
            obj must haveKey("num")
            obj("num") must beLike { case SDecimal(d) => d mustEqual 9 }
          }
        }
      }
    }.pendingUntilFixed
    
    "perform a naive cartesian product on the clicks dataset" in {
      val line = Line(0, "")
      val clicks = dag.LoadLocal(line, Root(line, PushString("/clicks")))
      
      val input = Join(line, JoinObject, CrossLeftSort,
        Join(line, WrapObject, CrossLeftSort,
          Root(line, PushString("aa")),
          Join(line, DerefObject, CrossLeftSort,
            clicks,
            Root(line, PushString("user")))),
        Join(line, WrapObject, CrossLeftSort,
          Root(line, PushString("bb")),
          Join(line, DerefObject, CrossLeftSort,
            dag.New(line, clicks),
            Root(line, PushString("user")))))
            
      testEval(input) { result =>
        result must haveSize(10000)
        
        forall(result) {
          _ must beLike {
            case (ids, SObject(obj)) if ids.size == 2 => 
              obj must haveSize(2)
              obj must haveKey("aa")
              obj must haveKey("bb")
          }
        }
      }
    }

    "distinct homogenous set of numbers" in {
      val line = Line(0, "")
      
      val input = dag.Distinct(line,
      dag.LoadLocal(line, Root(line, PushString("/hom/numbers2"))))
      
      testEval(input) { result =>
        result must haveSize(5)
        
        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
        }
        
        result2 must contain(42, 12, 77, 1, 13)
      }
    }.pendingUntilFixed
    
    "distinct heterogenous sets" in {
      val line = Line(0, "")
      
      val input = dag.Distinct(line,
      dag.LoadLocal(line, Root(line, PushString("/het/numbers2"))))
      
      testEval(input) { result =>
        result must haveSize(10)
        
        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          case (ids, SBoolean(b)) if ids.size == 1 => b
          case (ids, SString(s)) if ids.size == 1 => s
          case (ids, SArray(a)) if ids.size == 1 => a
          case (ids, SObject(o)) if ids.size == 1 => o
        }
        
        result2 must contain(42, 12, 77, 1, 13, true, false, "daniel", Map("test" -> SString("fubar")), Vector())
      }
    }.pendingUntilFixed
    
    "join two sets according to a value sort" in {
      val line = Line(0, "")
      
      val clicks = dag.LoadLocal(line, Root(line, PushString("/clicks")))
      val clicks2 = dag.LoadLocal(line, Root(line, PushString("/clicks2")))
      
      val input = dag.Join(line,
        Add,
        ValueSort(0),
        SortBy(clicks, "time", "time", 0),
        SortBy(clicks2, "time", "time", 0))
        
      testEval(dag.Join(line, DerefObject, CrossLeftSort, clicks, Root(line, PushString("time")))) { expected =>
        val expectedValues = expected collect {
          case (_, SDecimal(d)) => d * 2
        }
        
        testEval(input) { result =>
          result must haveSize(100)
          
          forall(result) { result =>
            result must beLike {
              case (ids, SDecimal(d)) => {
                ids must haveSize(1)
                expectedValues must contain(d)
              }
            }
          }
        }
      }
    }.pendingUntilFixed
    
    "filter two sets according to a value sort" in {
      val line = Line(0, "")
      
      val clicks = dag.LoadLocal(line, Root(line, PushString("/clicks")))
      val clicks2 = dag.LoadLocal(line, Root(line, PushString("/clicks2")))
      
      val input = dag.Filter(line,
        ValueSort(0),
        SortBy(clicks, "time", "time", 0),
        dag.Join(line,
          Gt,
          CrossLeftSort,
          SortBy(clicks2, "time", "time", 0),
          Root(line, PushNum("500"))))
        
      testEval(dag.Join(line, DerefObject, CrossLeftSort, clicks, Root(line, PushString("time")))) { expected =>
        val expectedValues = expected collect {
          case (ids, SDecimal(d)) if d > 500 => (ids, d)
        }
        
        testEval(input) { result =>
          result must haveSize(expectedValues.size)
          
          forall(result) { result =>
            result must beLike {
              case sev @ (ids, SDecimal(_)) => {
                ids must haveSize(1)
                expectedValues must contain(sev)
              }
            }
          }
        }
      }
    }.pendingUntilFixed
  }
}

object EvaluatorSpecs extends EvaluatorSpecs[YId] {
  val M = test.YId.M
  val coM = test.YId.M
}

