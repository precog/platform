package com.precog
package daze

import com.precog.common.Path

import com.precog.yggdrasil._
import com.precog.yggdrasil.table._
import com.precog.yggdrasil.serialization._
import com.precog.yggdrasil.test._
import com.precog.yggdrasil.util._
import com.precog.common.json._
import com.precog.util.IOUtils
import com.precog.util.IdGen
import com.precog.bytecode._

import akka.dispatch.{Await, ExecutionContext}
import akka.util.duration._

import blueeyes.json._
import JsonAST._

import java.io._
import java.util.concurrent.Executors

import scalaz._
import scalaz.effect._
import scalaz.syntax.copointed._
import scalaz.std.anyVal._
import scalaz.std.list._

import org.specs2.specification.Fragment
import org.specs2.specification.Fragments
import org.specs2.execute.Result
import org.specs2.mutable._

trait EvaluatorTestSupport[M[+_]] extends Evaluator[M] with BaseBlockStoreTestModule[M] with IdSourceScannerModule[M] {
  val asyncContext = ExecutionContext fromExecutor Executors.newCachedThreadPool()

  private val groupId = new java.util.concurrent.atomic.AtomicInteger
  def newGroupId = groupId.getAndIncrement

  val projections = Map.empty[ProjectionDescriptor, Projection]

  //object Table extends TableCompanion

  private var initialIndices = collection.mutable.Map[Path, Int]()    // if we were doing this for real: j.u.c.HashMap
  private var currentIndex = 0                                        // if we were doing this for real: j.u.c.a.AtomicInteger
  private val indexLock = new AnyRef                                  // if we were doing this for real: DIE IN A FIRE!!!
  
  class YggConfig extends IdSourceConfig with ColumnarTableModuleConfig with EvaluatorConfig with BlockStoreColumnarTableModuleConfig {
    val sortBufferSize = 1000
    val sortWorkDir: File = IOUtils.createTmpDir("idsoSpec").unsafePerformIO
    val clock = blueeyes.util.Clock.System
    val memoizationBufferSize = 1000
    val memoizationWorkDir: File = null //no filesystem storage in test!
    val flatMapTimeout = intToDurationInt(30).seconds
    val maxSliceSize = 10

    val idSource = new IdSource {
      private val source = new java.util.concurrent.atomic.AtomicLong
      def nextId() = source.getAndIncrement
    }
  }

  object yggConfig extends YggConfig 

  override def load(table: Table, uid: UserId, jtpe: JType) = {
    table.toJson map { events =>
      fromJson {
        events.toStream flatMap {
          case JString(pathStr) => indexLock synchronized {      // block the WHOLE WORLD
            val path = Path(pathStr)
      
            val index = initialIndices get path getOrElse {
              initialIndices += (path -> currentIndex)
              currentIndex
            }
            
            val target = path.path.replaceAll("/$", ".json")
            val src = io.Source fromInputStream getClass.getResourceAsStream(target)
            val parsed = src.getLines map JsonParser.parse toStream
            
            currentIndex += parsed.length
            
            parsed zip (Stream from index) map {
              case (value, id) => JObject(JField("key", JArray(JNum(id) :: Nil)) :: JField("value", value) :: Nil)
            }
          }

          case x => sys.error("Attempted to load JSON as a table from something that wasn't a string: " + x)
        }
      }
    }
  }
  
}


trait EvaluatorSpecs[M[+_]] extends Specification
    with EvaluatorTestSupport[M]
    with StdLib[M]
    with MemoryDatasetConsumer[M] { self =>
  
  import Function._
  
  import dag._
  import instructions._

  val testUID = "testUID"

  def testEval(graph: DepGraph, path: Path = Path.Root)(test: Set[SEvent] => Result): Result = withContext { ctx =>
    (consumeEval(testUID, graph, ctx, path, true) match {
      case Success(results) => test(results)
      case Failure(error) => throw error
    })/* and 
    (consumeEval(testUID, graph, ctx, path, false) match {
      case Success(results) => test(results)
      case Failure(error) => throw error
    })*/
  }
  
  "evaluator" should {
    "evaluate simple two-value multiplication" in {
      val line = Line(0, "")
      
      val input = Join(line, Mul, CrossLeftSort,
        Root(line, CLong(6)),
        Root(line, CLong(7)))
        
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
        val input = Root(line, CString("daniel"))
        
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
        val input = Root(line, CLong(42))
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
        val input = Root(line, CBoolean(true))
        
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
        val input = Root(line, CBoolean(false))
        
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
        val input = Root(line, CNull)
        
        testEval(input) { result =>
          result must haveSize(1)
          result.map(_._2) must contain(SNull)
        }
      }
      
      "push_object" >> {
        val line = Line(0, "")
        val input = Root(line, CEmptyObject)
        
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
        val input = Root(line, CEmptyArray)
        
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
      val input = dag.LoadLocal(line, Root(line, CString("/hom/numbers")))

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

      val numbers = dag.LoadLocal(line, Root(line, CString("/numbers")))

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

      val numbers = dag.LoadLocal(line, Root(line, CString("/numbers")))
      val numbers0 = dag.LoadLocal(line, Root(line, CString("/numbers")), JNumberT)

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

      val numbers = dag.LoadLocal(line, Root(line, CString("/numbers")))
      val numbers2 = dag.LoadLocal(line, Root(line, CString("/numbers2")))

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
        dag.LoadLocal(line, Root(line, CString("/hom/numbers"))))
        
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
        dag.LoadLocal(line, Root(line, CString("/hom/numbers"))))
        
      testEval(input) { result =>
        result must haveSize(5)
        
        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
        }
        
        result2 must contain(42, 12, 77, 1, 13)
      }
    }
    
    "evaluate a new literal" in {
      val line = Line(0, "")
      
      val input = dag.New(line, Root(line, CString("foo")))
        
      testEval(input) { result =>
        result must haveSize(1)
        
        val result2 = result collect {
          case (ids, SString(s)) if ids.size == 1 => s
        }
        
        result2 must contain("foo")
      }
    }

    "evaluate a join of two reductions on the same dataset" in {
      val line = Line(0, "")

      val parent = dag.LoadLocal(line, Root(line, CString("/hom/numbers7")))

      val input = Join(line, Add, CrossRightSort,
        dag.Reduce(line, Count, parent),
        dag.Reduce(line, Sum, parent))

      testEval(input) { result =>
        result must haveSize(1)

        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 0 => d
        }

        result2 must contain(259)
      }
    }

    "evaluate a join of two reductions on the same dataset using a MegaReduce" in {
      val line = Line(0, "")

      val parent = dag.LoadLocal(line, Root(line, CString("/hom/numbers7")))
      
      val spec = trans.Leaf(trans.Source)
      val reds = List(Count, Sum) 
      val mega = dag.MegaReduce(line, 
        List((spec, reds)),
        parent)

      val input = Join(line, Add, CrossRightSort, 
        joinDeref(mega, 0, 0, line),
        joinDeref(mega, 0, 1, line))
        
      testEval(input) { result =>
        result must haveSize(1)

        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 0 => d
        }

        result2 must contain(259)
      }
    }

    "MegaReduce of two tuples must return an array" in {
      val line = Line(0, "")

      val parent = dag.LoadLocal(line, Root(line, CString("/hom/heightWeightAcrossSlices")))
      
      val height = trans.DerefObjectStatic(trans.Leaf(trans.Source), CPathField("height"))
      val weight = trans.DerefObjectStatic(trans.Leaf(trans.Source), CPathField("weight"))
      val mean = List(Mean) 
      val max = List(Max) 

      val input = dag.MegaReduce(line, 
        List((weight, mean), (height, max)),
        parent)

      testEval(input) { result =>
        result must haveSize(1)

        val result2 = result collect {
          case (ids, SArray(arr)) if ids.size == 0 => arr
        }

        result2 must contain(Vector(SArray(Vector(SDecimal(104))), SArray(Vector(SDecimal(138)))))
      }
    }


    "evaluate a join of two reductions on two datasets with the same parent using a MegaReduce" in {
      val line = Line(0, "")

      val parent = dag.LoadLocal(line, Root(line, CString("/hom/heightWeightAcrossSlices")))
      
      val height = trans.DerefObjectStatic(trans.Leaf(trans.Source), CPathField("height"))
      val weight = trans.DerefObjectStatic(trans.Leaf(trans.Source), CPathField("weight"))
      val mean = List(Mean) 
      val max = List(Max) 

      val mega = dag.MegaReduce(line, 
        List((weight, mean), (height, max)),
        parent)

      val input = Join(line, Add, CrossRightSort, 
        joinDeref(mega, 0, 0, line),
        joinDeref(mega, 1, 0, line))

      testEval(input) { result =>
        result must haveSize(1)

        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 0 => d
        }

        result2 must contain(138 + 104)
      }
    }


    "evaluate a join of three reductions on the same dataset using a MegaReduce" in {
      val line = Line(0, "")

      val parent = dag.LoadLocal(line, Root(line, CString("/hom/numbers7")))
      
      val mega = dag.MegaReduce(line, List((trans.Leaf(trans.Source), List(Count, Sum, Mean))), parent)

      val input = Join(line, Add, CrossLeftSort,
        joinDeref(mega, 0, 0, line),
        Join(line, Add, CrossLeftSort, 
          joinDeref(mega, 0, 1, line),
          joinDeref(mega, 0, 2, line)))

      testEval(input) { result =>
        result must haveSize(1)

        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 0 => d
        }

        result2 must contain(237 + 22 + (237.0 / 22))
      }
    }

    "evaluate a rewrite/eval of a 3-way mega reduce" in {
      import trans._

      val line = Line(0, "")

      val load = dag.LoadLocal(line, Root(line, CString("/hom/heightWeightAcrossSlices")))

      val id = Join(line, DerefObject, CrossLeftSort, load, Root(line, CString("userId")))
      val height = Join(line, DerefObject, CrossLeftSort, load, Root(line, CString("height")))
      val weight = Join(line, DerefObject, CrossLeftSort, load, Root(line, CString("weight")))

      val r1 = dag.Reduce(line, Min, id)
      val r2 = dag.Reduce(line, Max, height)
      val r3 = dag.Reduce(line, Mean, weight)

      val input = Join(line, Sub, CrossLeftSort, r1, Join(line, Add, CrossLeftSort, r2, r3))

      testEval(input) { result =>
        result must haveSize(1)

        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 0 => d
        }

        result2 must contain(1 - (104 + 138))
      }
    }

    "evaluate a rewrite/eval of reductions" in {
      import trans._

      val line = Line(0, "")

      val load = dag.LoadLocal(line, Root(line, CString("/hom/heightWeightAcrossSlices")))

      val height = Join(line, DerefObject, CrossLeftSort, load, Root(line, CString("height")))
      val weight = Join(line, DerefObject, CrossLeftSort, load, Root(line, CString("weight")))

      val r1 = dag.Reduce(line, Min, height)
      val r2 = dag.Reduce(line, Max, height)
      val r3 = dag.Reduce(line, Mean, weight)

      val input = Join(line, Sub, CrossLeftSort, r1, Join(line, Add, CrossLeftSort, r2, r3))

      testEval(input) { result =>
        result must haveSize(1)

        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 0 => d
        }

        result2 must contain(30 - (104 + 138))
      }
    }
    
    "three reductions on the same dataset" in {
      import trans._

      val line = Line(0, "")

      val load = dag.LoadLocal(line, Root(line, CString("/hom/heightWeightAcrossSlices")))

      val weight = Join(line, DerefObject, CrossLeftSort, load, Root(line, CString("weight")))

      val r1 = dag.Reduce(line, Min, weight)
      val r2 = dag.Reduce(line, Max, weight)
      val r3 = dag.Reduce(line, Mean, weight)

      val input = Join(line, Sub, CrossLeftSort, r1, Join(line, Add, CrossLeftSort, r2, r3))

      testEval(input) { result =>
        result must haveSize(1)

        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 0 => d
        }

        result2 must contain(29 - (231 + 138))
      }
    }

    "the same reduction on three datasets" in {
      import trans._

      val line = Line(0, "")

      val load = dag.LoadLocal(line, Root(line, CString("/hom/heightWeightAcrossSlices")))

      val id = Join(line, DerefObject, CrossLeftSort, load, Root(line, CString("userId")))
      val height = Join(line, DerefObject, CrossLeftSort, load, Root(line, CString("height")))
      val weight = Join(line, DerefObject, CrossLeftSort, load, Root(line, CString("weight")))

      val r1 = dag.Reduce(line, Max, id)
      val r2 = dag.Reduce(line, Max, height)
      val r3 = dag.Reduce(line, Max, weight)

      val input = Join(line, Sub, CrossLeftSort, r1, Join(line, Add, CrossLeftSort, r2, r3))

      testEval(input) { result =>
        result must haveSize(1)

        val result2 = result collect { 
          case (ids, SDecimal(num)) if ids.size == 0 => num
        }

        result2 must contain(22 - (104 + 231))
      }
    }
    
    "join two sets" >> {
      "from different paths" >> {
        val line = Line(0, "")

        val input = Join(line, Add, CrossLeftSort,
          Join(line, DerefObject, CrossLeftSort, 
            dag.LoadLocal(line, Root(line, CString("/clicks"))),
            Root(line, CString("time"))),
          Join(line, DerefObject, CrossLeftSort,
            dag.LoadLocal(line, Root(line, CString("/hom/heightWeight"))),
            Root(line, CString("height"))))

        testEval(input) { result =>
          result must haveSize(500)

          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 2 => ids
          }

          result2 must haveSize(500)
        }
      }

      "from the same path" >> {
        val line = Line(0, "")
        val heightWeight = dag.LoadLocal(line, Root(line, CString("/hom/heightWeight")))

        val input = Join(line, Add, IdentitySort,
          Join(line, DerefObject, CrossLeftSort, 
            heightWeight,
            Root(line, CString("weight"))),
          Join(line, DerefObject, CrossLeftSort,
            heightWeight,
            Root(line, CString("height"))))

        testEval(input) { result =>
          result must haveSize(5)

          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d
          }

          result2 must contain(218, 147, 118, 172, 224)
        }
      }

      "from the same path (with a relative path)" >> {
        val line = Line(0, "")
        val heightWeight = dag.LoadLocal(line, Root(line, CString("/heightWeight")))

        val input = Join(line, Add, IdentitySort,
          Join(line, DerefObject, CrossLeftSort, 
            heightWeight,
            Root(line, CString("weight"))),
          Join(line, DerefObject, CrossLeftSort,
            heightWeight,
            Root(line, CString("height"))))

        testEval(input, Path("/hom")) { result =>
          result must haveSize(5)

          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d
          }

          result2 must contain(218, 147, 118, 172, 224)
        }
      }
    }

    "evaluate a binary numeric operation mapped over homogeneous numeric set" >> {
      "addition" >> {
        val line = Line(0, "")
        
        val input = Join(line, Add, CrossLeftSort,
          dag.LoadLocal(line, Root(line, CString("/hom/numbers"))),
          Root(line, CLong(5)))
          
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
          dag.LoadLocal(line, Root(line, CString("/hom/numbers"))),
          Root(line, CLong(5)))
          
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
          dag.LoadLocal(line, Root(line, CString("/hom/numbers"))),
          Root(line, CLong(5)))
          
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
          dag.LoadLocal(line, Root(line, CString("/hom/numbers"))),
          Root(line, CLong(5)))
          
        testEval(input) { result =>
          result must haveSize(5)
          
          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toDouble
          }
          
          result2 must contain(8.4, 2.4, 15.4, 0.2, 2.6)
        }
      }      

      "mod both positive" >> {
        val line = Line(0, "")
        
        val input = Join(line, Mod, CrossLeftSort,
          Root(line, CLong(11)),
          Root(line, CLong(4)))
          
        testEval(input) { result =>
          result must haveSize(1)
          
          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 0 => d.toDouble
          }
          
          result2 must contain(3)
        }
      }
      "mod both negative" >> {
        val line = Line(0, "")
        
        val input = Join(line, Mod, CrossLeftSort,
          Root(line, CLong(-11)),
          Root(line, CLong(-4)))
          
        testEval(input) { result =>
          result must haveSize(1)
          
          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 0 => d.toDouble
          }
          
          result2 must contain(-3)
        }
      }
      "mod negative left" >> {
        val line = Line(0, "")
        
        val input = Join(line, Mod, CrossLeftSort,
          Root(line, CLong(-11)),
          Root(line, CLong(4)))
          
        testEval(input) { result =>
          result must haveSize(1)
          
          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 0 => d.toDouble
          }
          
          result2 must contain(1)
        }
      }
      "mod" >> {
        val line = Line(0, "")
        
        val input = Join(line, Mod, CrossLeftSort,
          Root(line, CLong(11)),
          Root(line, CLong(-4)))
          
        testEval(input) { result =>
          result must haveSize(1)
          
          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 0 => d.toDouble
          }
          
          result2 must contain(-1)
        }
      }
    }
    
    "evaluate a binary numeric operation mapped over heterogeneous numeric set" >> {
      "addition" >> {
        val line = Line(0, "")
        
        val input = Join(line, Add, CrossLeftSort,
          dag.LoadLocal(line, Root(line, CString("/het/numbers"))),
          Root(line, CLong(5)))
          
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
          dag.LoadLocal(line, Root(line, CString("/het/numbers"))),
          Root(line, CLong(5)))
          
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
          dag.LoadLocal(line, Root(line, CString("/het/numbers"))),
          Root(line, CLong(5)))
          
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
          dag.LoadLocal(line, Root(line, CString("/het/numbers"))),
          Root(line, CLong(5)))
          
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
      val clicks = dag.LoadLocal(line, Root(line, CString("/clicks")))

      val input = dag.Reduce(line, Count,
        Filter(line, IdentitySort,
          clicks,
          Join(line, Gt, CrossLeftSort,
            Join(line, DerefObject, CrossLeftSort,
              clicks,
              Root(line, CString("time"))),
            Root(line, CLong(0)))))

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
              dag.LoadLocal(line, Root(line, CString("/clicks"))),
              Root(line, CString("time"))),
            Root(line, CLong(0)))

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
          dag.LoadLocal(line, Root(line, CString("/clicks"))),
          Root(line, CString("time"))))

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
          dag.LoadLocal(line, Root(line, CString("/hom/numbers"))),
          dag.Reduce(line, Count, 
            Root(line, CLong(42))))

        testEval(input) { result =>
          result must haveSize(5)

          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d
          }

          result2 must contain(43, 13, 78, 2, 14)
        }
      }

      "a reduction on the left side of the cross" >> {
        val line = Line(0, "")

        val input = Join(line, Add, CrossLeftSort, 
          dag.Reduce(line, Count, 
            Root(line, CLong(42))),
          dag.LoadLocal(line, Root(line, CString("/hom/numbers"))))

        testEval(input) { result =>
          result must haveSize(5)

          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d
          }

          result2 must contain(43, 13, 78, 2, 14)
        }
      }

      "a root on the right side of the cross" >> {
        val line = Line(0, "")

        val input = Join(line, Add, CrossLeftSort,  
          dag.LoadLocal(line, Root(line, CString("/hom/numbers"))),
          Root(line, CLong(3)))
         
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
          Root(line, CLong(3)),
          dag.LoadLocal(line, Root(line, CString("/hom/numbers"))))

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
        Root(line, CString("answer")),
        Root(line, CLong(42)))
        
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
        Root(line, CString("answer")),
        Join(line, WrapObject, CrossLeftSort,
          Root(line, CString("question")),
          Root(line, CNull)))
        
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
        Root(line, CString("aa")),
        Join(line, DerefObject, CrossLeftSort,
          dag.LoadLocal(line, Root(line, CString("/clicks"))),
          Root(line, CString("user"))))
        
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
        Root(line, CLong(42)))
        
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
        Root(line, CNull))
        
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
          Root(line, CString("question")),
          Root(line, CString("What is six times seven?"))),
        Join(line, WrapObject, CrossLeftSort,
          Root(line, CString("answer")),
          Root(line, CLong(42))))
        
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
          case SString(str) => str mustEqual "What is six times seven?"
        }
      }
    }
    
    "evaluate join_array on single values" in {
      val line = Line(0, "")
      
      val input = Join(line, JoinArray, CrossLeftSort,
        Operate(line, WrapArray,
          Root(line, CLong(24))),
        Operate(line, WrapArray,
          Root(line, CLong(42))))
        
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
    }

    "create an array" >> {
      val line = Line(0, "")
      
      val input = 
        Join(line, JoinArray, CrossLeftSort,
          Operate(line, WrapArray,
            Root(line, CLong(12))),
          Join(line, JoinArray, CrossLeftSort,
            Operate(line, WrapArray,
              Root(line, CLong(24))),
            Operate(line, WrapArray,
              Root(line, CLong(42)))))
        
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
            d2 mustEqual 24
            d3 mustEqual 42
          }
        }
      }
    }
    
    "evaluate array_swap on single values" >> {
      "at start" >> {
        val line = Line(0, "")
        
        val input = Join(line, ArraySwap, CrossLeftSort,
          Join(line, JoinArray, CrossLeftSort,
            Operate(line, WrapArray,
              Root(line, CLong(12))),
            Join(line, JoinArray, CrossLeftSort,
              Operate(line, WrapArray,
                Root(line, CLong(24))),
              Operate(line, WrapArray,
                Root(line, CLong(42))))),
          Root(line, CLong(1)))
          
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
      }
      
      "at end" >> {
        val line = Line(0, "")
        
        val input = Join(line, ArraySwap, CrossLeftSort,
          Join(line, JoinArray, CrossLeftSort,
            Operate(line, WrapArray,
              Root(line, CLong(12))),
            Join(line, JoinArray, CrossLeftSort,
              Operate(line, WrapArray,
                Root(line, CLong(24))),
              Operate(line, WrapArray,
                Root(line, CLong(42))))),
          Root(line, CLong(2)))
          
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
              d1 mustEqual 42
              d2 mustEqual 24
              d3 mustEqual 12
            }
          }
        }
      }
    }
    
    "evaluate descent on a homogeneous set" in {
      val line = Line(0, "")
      
      val input = Join(line, DerefObject, CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/hom/pairs"))),
        Root(line, CString("first")))
        
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
        dag.LoadLocal(line, Root(line, CString("/het/pairs"))),
        Root(line, CString("first")))
        
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
        dag.LoadLocal(line, Root(line, CString("/het/het-pairs"))),
        Root(line, CString("first")))
        
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

      val input = dag.LoadLocal(line, Root(line, CString("/hom/arrays")))

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

      val parent = dag.LoadLocal(line, Root(line, CString("/hom/numbers")))
      val input = dag.MegaReduce(line, List((trans.Leaf(trans.Source), List(Count, Sum))), parent)

      testEval(input) { result =>
        result must haveSize(1)

        val result2 = result collect {
          case (ids, SArray(arr)) if ids.size == 0 => arr
        }

        result2 must contain(Vector(SArray(Vector(SDecimal(145), SDecimal(5)))))
      }
    }
    
    "evaluate array dereference on a MegaReduce" in {
      val line = Line(0, "")
      
      val parent = dag.LoadLocal(line, Root(line, CString("/hom/numbers")))
      val red = Sum

      val mega = dag.MegaReduce(line, List((trans.Leaf(trans.Source), List(red))), parent)
      val input = Join(line, DerefArray, CrossLeftSort, Join(line, DerefArray, CrossLeftSort, mega, Root(line, CLong(0))), Root(line, CLong(0)))
        
      testEval(input) { result =>
        result must haveSize(1)
        
        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 0 => d.toInt
        }
        
        result2 must contain(145)
      }
    }    

    "evaluate array dereference on a homogeneous set" in {
      val line = Line(0, "")
      
      val input = Join(line, DerefArray, CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/hom/arrays"))),
        Root(line, CLong(2)))
        
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
        dag.LoadLocal(line, Root(line, CString("/het/arrays"))),
        Root(line, CLong(2)))
        
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
        dag.LoadLocal(line, Root(line, CString("/het/het-arrays"))),
        Root(line, CLong(2)))
        
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
      val pairs = dag.LoadLocal(line, Root(line, CString("/hom/pairs")))
      
      val input = Join(line, Sub, IdentitySort,
        Join(line, DerefObject, CrossLeftSort,
          pairs,
          Root(line, CString("first"))),
        Join(line, DerefObject, CrossLeftSort,
          pairs,
          Root(line, CString("second"))))
        
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
      val pairs = dag.LoadLocal(line, Root(line, CString("/hom/pairs")))
      
      val input = Join(line, Div, IdentitySort,
        Join(line, DerefObject, CrossLeftSort,
          pairs,
          Root(line, CString("first"))),
        Join(line, DerefObject, CrossLeftSort,
          pairs,
          Root(line, CString("second"))))
        
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
      val pairs = dag.LoadLocal(line, Root(line, CString("/pairs")))
      
      val input = Join(line, Div, IdentitySort,
        Join(line, DerefObject, CrossLeftSort,
          pairs,
          Root(line, CString("first"))),
        Join(line, DerefObject, CrossLeftSort,
          pairs,
          Root(line, CString("second"))))
        
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
      val clicks2 = dag.LoadLocal(line, Root(line, CString("/clicks2")))
      
      val input = Diff(line,
        clicks2,
        Filter(line, IdentitySort,
          clicks2, 
          Join(line, Gt, CrossLeftSort,
            Join(line, DerefObject, CrossLeftSort,
              clicks2,
              Root(line, CString("time"))),
            Root(line, CLong(0)))))
        
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
    }
    
    "compute the set difference of the set difference" in {
      val line = Line(0, "")
      val clicks2 = dag.LoadLocal(line, Root(line, CString("/clicks2")))
      
      val input = Diff(line,
        clicks2,
        Diff(line,
          clicks2,
          Filter(line, IdentitySort,
            clicks2, 
            Join(line, Gt, CrossLeftSort,
              Join(line, DerefObject, CrossLeftSort,
                clicks2,
                Root(line, CString("time"))),
              Root(line, CLong(0))))))


      testEval(input) { result =>
        result must haveSize(101)
        
        forall(result) {
          _ must beLike {
            case (ids, SObject(obj)) if ids.size == 1 => 
              obj must haveKey("time")
          }
        }
      }
    }
    
    "compute the iunion of a set with itself" in {
      val line = Line(0, "")
      
      val numbers = dag.LoadLocal(line, Root(line, CString("/hom/numbers")))

      val input = IUI(line, true, numbers, numbers)
        
      testEval(input) { result =>
        result must haveSize(5)
      }
    }

    "compute the iunion of two homogeneous sets" in {
      val line = Line(0, "")
      
      val input = IUI(line, true,
        dag.LoadLocal(line, Root(line, CString("/hom/numbers"))),
        dag.LoadLocal(line, Root(line, CString("/hom/numbers3"))))
        
      testEval(input) { result =>
        result must haveSize(10)
        
        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 1 => d.toDouble
        }
        result2 must contain(42, 12, 77, 1, 13, 14, -1, 0)

        val result3 = result collect {
          case (ids, _) => ids
        }
        result3 must haveSize(10)
      }
    }

    "compute the iunion of two homogeneous sets (with relative path)" in {
      val line = Line(0, "")
      
      val input = IUI(line, true,
        dag.LoadLocal(line, Root(line, CString("/numbers"))),
        dag.LoadLocal(line, Root(line, CString("/numbers3"))))
        
      testEval(input, Path("/hom")) { result =>
        result must haveSize(10)
        
        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 1 => d.toDouble
        }
        
        result2 must contain(42, 12, 77, 1, 13, 14, -1, 0)
      }
    }
    
    "compute the iunion of two datasets, one with objects" in {
      val line = Line(0, "")
      
      val input = IUI(line, true,
        dag.LoadLocal(line, Root(line, CString("/clicks"))),
        dag.LoadLocal(line, Root(line, CString("/hom/numbers3"))))
        
      testEval(input) { result =>
        result must haveSize(105)
      }
    }
    
    "compute the iintersect of two nonintersecting sets of numbers" in {
      val line = Line(0, "")
      
      val input = IUI(line, false,
        dag.LoadLocal(line, Root(line, CString("/hom/numbers"))),
        dag.LoadLocal(line, Root(line, CString("/hom/numbers3"))))
        
      testEval(input) { result =>
        result must haveSize(0)
      }
    }

    "compute the iintersect of two nonintersecting datasets" in {
      val line = Line(0, "")
      
      val input = IUI(line, false,
        dag.LoadLocal(line, Root(line, CString("/clicks"))),
        dag.LoadLocal(line, Root(line, CString("/hom/numbers3"))))
        
      testEval(input) { result =>
        result must haveSize(0)
      }
    }

    "compute the iintersect of mod2 and mod3" in {
      val line = Line(0, "")
      val numbers = dag.LoadLocal(line, Root(line, CString("/hom/numbersmod")))

      val input = IUI(line, false,
        Filter(line, IdentitySort,
          numbers,
          Join(line, Eq, CrossLeftSort,
            Join(line, Mod, CrossLeftSort,
              numbers,
              Root(line, CLong(2))),
            Root(line, CLong(0)))),
        Filter(line, IdentitySort,
          numbers,
          Join(line, Eq, CrossLeftSort,
            Join(line, Mod, CrossLeftSort,
              numbers,
              Root(line, CLong(3))),
            Root(line, CLong(0)))))

      testEval(input) { result =>
        result must haveSize(3)

        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
        }

        result2 must contain(6, 12, 24)
      }
    }
    
    "filter homogeneous numeric set by binary operation" >> {
      "less-than" >> {
        val line = Line(0, "")
        val numbers = dag.LoadLocal(line, Root(line, CString("/hom/numbers")))
        
        val input = Filter(line, IdentitySort,
          numbers,
          Join(line, Lt, CrossLeftSort,
            numbers,
            Root(line, CLong(13))))
          
        testEval(input) { result =>
          result must haveSize(2)
          
          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          }
          
          result2 must contain(1, 12)
        }
      }      
      
      "less-than (with relative paths)" >> {
        val line = Line(0, "")
        val numbers = dag.LoadLocal(line, Root(line, CString("/numbers")))
        
        val input = Filter(line, IdentitySort,
          numbers,
          Join(line, Lt, CrossLeftSort,
            numbers,
            Root(line, CLong(13))))
          
        testEval(input, Path("/hom")) { result =>
          result must haveSize(2)
          
          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          }
          
          result2 must contain(1, 12)
        }
      }
      
      "less-than-equal" >> {
        val line = Line(0, "")
        val numbers = dag.LoadLocal(line, Root(line, CString("/hom/numbers")))
        
        val input = Filter(line, IdentitySort,
          numbers,
          Join(line, LtEq, CrossLeftSort,
            numbers,
            Root(line, CLong(13))))
          
        testEval(input) { result =>
          result must haveSize(3)
          
          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          }
          
          result2 must contain(12, 1, 13)
        }
      }
      
      "greater-than" >> {
        val line = Line(0, "")
        val numbers = dag.LoadLocal(line, Root(line, CString("/hom/numbers")))
        
        val input = Filter(line, IdentitySort,
          numbers,
          Join(line, Gt, CrossLeftSort,
            numbers,
            Root(line, CLong(13))))
          
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
        val numbers = dag.LoadLocal(line, Root(line, CString("/hom/numbers")))
        
        val input = Filter(line, IdentitySort,
          numbers,
          Join(line, GtEq, CrossLeftSort,
            numbers,
            Root(line, CLong(13))))
          
        testEval(input) { result =>
          result must haveSize(3)
          
          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          }
          
          result2 must contain(42, 77, 13)
        }
      }
      
      "equal with a number literal" >> {
        val line = Line(0, "")
        val numbers = dag.LoadLocal(line, Root(line, CString("/hom/numbers")))
        
        val input = Filter(line, IdentitySort,
          numbers,
          Join(line, Eq, CrossLeftSort,
            numbers,
            Root(line, CLong(13))))
          
        testEval(input) { result =>
          result must haveSize(1)
          
          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          }
          
          result2 must contain(13)
        }
      }       

      "equal without a filter" >> {
        val line = Line(0, "")
        
        val input = Join(line, Eq, CrossLeftSort,
            dag.LoadLocal(line, Root(line, CString("/hom/numbers"))),
            Root(line, CLong(13)))
          
        testEval(input) { result =>
          result must haveSize(5)
          
          val result2 = result collect {
            case (ids, SBoolean(d)) if ids.size == 1 => d
          }
          
          result2 must contain(true, false)
        }
      }
      
      "not-equal" >> {
        val line = Line(0, "")
        val numbers = dag.LoadLocal(line, Root(line, CString("/hom/numbers")))
        
        val input = Filter(line, IdentitySort,
          numbers,
          Join(line, NotEq, CrossLeftSort,
            numbers,
            Root(line, CLong(13))))
          
        testEval(input) { result =>
          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          }
          
          result2 must contain(42, 12, 77, 1)
        }
      }
      
      "and" >> {
        val line = Line(0, "")
        val numbers = dag.LoadLocal(line, Root(line, CString("/hom/numbers")))
        
        val input = Filter(line, IdentitySort,
          numbers,
          Join(line, And, IdentitySort,
            Join(line, NotEq, CrossLeftSort,
              numbers,
              Root(line, CLong(77))),
            Join(line, NotEq, CrossLeftSort,
              numbers,
              Root(line, CLong(13)))))
          
        testEval(input) { result =>
          result must haveSize(3)
          
          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          }
          
          result2 must contain(42, 12, 1)
        }
      }      

      "and (with relative paths)" >> {
        val line = Line(0, "")
        val numbers = dag.LoadLocal(line, Root(line, CString("/numbers")))
        
        val input = Filter(line, IdentitySort,
          numbers,
          Join(line, And, IdentitySort,
            Join(line, NotEq, CrossLeftSort,
              numbers,
              Root(line, CLong(77))),
            Join(line, NotEq, CrossLeftSort,
              numbers,
              Root(line, CLong(13)))))
          
        testEval(input, Path("/hom")) { result =>
          result must haveSize(3)
          
          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          }
          
          result2 must contain(42, 12, 1)
        }
      }
      
      "or" >> {
        val line = Line(0, "")
        val numbers = dag.LoadLocal(line, Root(line, CString("/hom/numbers")))
        
        val input = Filter(line, IdentitySort,
          numbers,
          Join(line, Or, IdentitySort,
            Join(line, Eq, CrossLeftSort,
              numbers,
              Root(line, CLong(77))),
            Join(line, Eq, CrossLeftSort,
              numbers,
              Root(line, CLong(13)))))
          
        testEval(input) { result =>
          result must haveSize(2)
          
          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          }
          
          result2 must contain(77, 13)
        }
      }
      
      "complement of equality" >> {
        val line = Line(0, "")
        val numbers = dag.LoadLocal(line, Root(line, CString("/hom/numbers")))
        
        val input = Filter(line, IdentitySort,
          numbers,
          Operate(line, Comp,
            Join(line, Eq, CrossLeftSort,
              numbers,
              Root(line, CLong(13)))))
          
        testEval(input) { result =>
          result must haveSize(4)
          
          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          }
          
          result2 must contain(42, 12, 77, 1)
        }
      }
    }
    
    "filter heterogeneous numeric set by binary operation" >> {
      "less-than" >> {
        val line = Line(0, "")
        val numbers = dag.LoadLocal(line, Root(line, CString("/het/numbers")))
        
        val input = Filter(line, IdentitySort,
          numbers,
          Join(line, Lt, CrossLeftSort,
            numbers,
            Root(line, CLong(13))))
          
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
        val numbers = dag.LoadLocal(line, Root(line, CString("/het/numbers")))
        
        val input = Filter(line, IdentitySort,
          numbers,
          Join(line, LtEq, CrossLeftSort,
            numbers,
            Root(line, CLong(13))))
          
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
        val numbers = dag.LoadLocal(line, Root(line, CString("/numbers")))
        
        val input = Filter(line, IdentitySort,
          numbers,
          Join(line, LtEq, CrossLeftSort,
            numbers,
            Root(line, CLong(13))))
          
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
        val numbers = dag.LoadLocal(line, Root(line, CString("/het/numbers")))
        
        val input = Filter(line, IdentitySort,
          numbers,
          Join(line, Gt, CrossLeftSort,
            numbers,
            Root(line, CLong(13))))
          
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
        val numbers = dag.LoadLocal(line, Root(line, CString("/het/numbers")))
        
        val input = Filter(line, IdentitySort,
          numbers,
          Join(line, GtEq, CrossLeftSort,
            numbers,
            Root(line, CLong(13))))
          
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
        val numbers = dag.LoadLocal(line, Root(line, CString("/het/numbers")))
        
        val input = Filter(line, IdentitySort,
          Join(line, Eq, CrossLeftSort,
            numbers,
            Root(line, CLong(13))),
          Join(line, Eq, CrossLeftSort,
            numbers,
            Root(line, CLong(13))))
          
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
        val numbers = dag.LoadLocal(line, Root(line, CString("/het/numbers")))
        
        val input = Filter(line, IdentitySort,
          numbers,
          Join(line, Eq, CrossLeftSort,
            numbers,
            Root(line, CLong(13))))
          
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
        val numbers9 = dag.LoadLocal(line, Root(line, CString("/het/numbers9")))
        
        val input = Filter(line, IdentitySort,
          numbers9,
          Join(line, Eq, CrossLeftSort,
            numbers9,
            Root(line, CEmptyArray)))
          
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
        val numbers9 = dag.LoadLocal(line, Root(line, CString("/het/numbers9")))
        
        val input = Filter(line, IdentitySort,
          numbers9,
          Join(line, Eq, CrossLeftSort,
            numbers9,
            Root(line, CEmptyObject)))
          
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
        val numbers = dag.LoadLocal(line, Root(line, CString("/het/numbers6")))
        
        val input = Filter(line, IdentitySort,
          numbers,
          Join(line, Eq, CrossLeftSort,
            numbers,
            Join(line, JoinArray, CrossLeftSort,
              Join(line, JoinArray, CrossLeftSort,
                Operate(line, WrapArray, Root(line, CLong(9))),
                Operate(line, WrapArray, Root(line, CLong(10)))),
              Operate(line, WrapArray, Root(line, CLong(11))))))

        testEval(input) { result =>
          result must haveSize(1)

          val result2 = result collect {
            case (ids, SArray(arr)) if ids.size == 1 => arr
          }

          result2 must contain(Vector(SDecimal(9), SDecimal(10), SDecimal(11)))
        }
      }

      "equal with a singleton array" >> {
        val line = Line(0, "")
        val numbers = dag.LoadLocal(line, Root(line, CString("/het/array")))
        
        val input = Filter(line, IdentitySort,
          numbers,
          Join(line, Eq, CrossLeftSort,
            numbers,
            Join(line, JoinArray, CrossLeftSort,
              Join(line, JoinArray, CrossLeftSort,
                Operate(line, WrapArray, Root(line, CLong(9))),
                Operate(line, WrapArray, Root(line, CLong(10)))),
              Operate(line, WrapArray, Root(line, CLong(11))))))


        testEval(input) { result =>
          result must haveSize(1)

          val result2 = result collect {
            case (ids, SArray(arr)) if ids.size == 1 => arr
          }

          result2 must contain(Vector(SDecimal(9), SDecimal(10), SDecimal(11)))
        }
      }
      
      "equal with an object" >> {
        val line = Line(0, "")
        val numbers = dag.LoadLocal(line, Root(line, CString("/het/numbers6")))
        
        val input = Filter(line, IdentitySort,
          numbers,
          Join(line, Eq, CrossLeftSort,
            numbers,
              Join(line, WrapObject, CrossLeftSort,
                Root(line, CString("foo")),
                Root(line, CString("bar")))))

        testEval(input) { result =>
          result must haveSize(1)

          forall(result) {
            _ must beLike {
              case (ids, SObject(obj)) if ids.size == 1 => {
                obj must haveKey("foo")
                obj must haveValue(SString("bar"))
              }
            }
          }
        }
      }

      "equal without a filter" >> {
        val line = Line(0, "")
        
        val input = Join(line, Eq, CrossLeftSort,
            dag.LoadLocal(line, Root(line, CString("/het/numbers"))),
            Root(line, CLong(13)))
          
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
            dag.LoadLocal(line, Root(line, CString("/het/numbers"))),
            Root(line, CLong(13)))
          
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
        val numbers = dag.LoadLocal(line, Root(line, CString("/het/numbers")))
        
        val input = Filter(line, IdentitySort,
          numbers,
          Join(line, NotEq, CrossLeftSort,
            numbers,
            Root(line, CLong(13))))
          
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
        val numbers = dag.LoadLocal(line, Root(line, CString("/het/numbers10")))
        
        val input = Filter(line, IdentitySort,
          numbers,
          Join(line, NotEq, CrossLeftSort,
            numbers,
            Root(line, CEmptyArray)))
          
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
        val numbers = dag.LoadLocal(line, Root(line, CString("/het/numbers10")))
        
        val input = Filter(line, IdentitySort,
          numbers,
          Join(line, NotEq, CrossLeftSort,
            numbers,
            Root(line, CEmptyObject)))
          
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
        val numbers = dag.LoadLocal(line, Root(line, CString("/het/numbers10")))
        
        val input = Filter(line, IdentitySort,
          numbers,
          Join(line, NotEq, CrossLeftSort,
            numbers,
            Join(line, JoinArray, CrossLeftSort,
              Operate(line, WrapArray, Root(line, CLong(9))),
              Operate(line, WrapArray, Root(line, CLong(10))))))


        testEval(input) { result =>
          result must haveSize(3)
          
          val result2 = result collect {
            case (ids, SArray(arr)) if ids.size == 1 => arr
            case (ids, SObject(obj)) if ids.size == 1 => obj
          }
          
          result2 must contain(Map.empty[String, SValue], Vector.empty[SValue], Map("foo" -> SNull))
 
        }
      }

      "not-equal with an object" >> {
        val line = Line(0, "")
        val numbers = dag.LoadLocal(line, Root(line, CString("/het/numbers10")))
        
        val input = Filter(line, IdentitySort,
          numbers,
          Join(line, NotEq, CrossLeftSort,
            numbers,
            Join(line, WrapObject, CrossLeftSort,
              Root(line, CString("foo")),
              Root(line, CNull))))

        testEval(input) { result =>
          result must haveSize(3)

          val result2 = result collect {
            case (ids, SArray(arr)) if ids.size == 1 => arr
            case (ids, SObject(obj)) if ids.size == 1 => obj
          }
          
          result2 must contain(Vector.empty[SValue], Vector(SDecimal(9), SDecimal(10)), Map.empty[String, SValue])

        }
      }

      "and" >> {
        val line = Line(0, "")
        val numbers = dag.LoadLocal(line, Root(line, CString("/het/numbers")))
        
        val input = Filter(line, IdentitySort,
          numbers,
          Join(line, And, IdentitySort,
            Join(line, NotEq, CrossLeftSort,
              numbers,
              Root(line, CLong(77))),
            Join(line, NotEq, CrossLeftSort,
              numbers,
              Root(line, CLong(13)))))
          
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
      }

      "or" >> {
        val line = Line(0, "")
        val numbers = dag.LoadLocal(line, Root(line, CString("/het/numbers")))
        
        val input = Filter(line, IdentitySort,
          numbers,
          Join(line, Or, IdentitySort,
            Join(line, Eq, CrossLeftSort,
              numbers,
              Root(line, CLong(77))),
            Join(line, Eq, CrossLeftSort,
              numbers,
              Root(line, CLong(13)))))
          
        testEval(input) { result =>
          result must haveSize(2)
          
          val result2 = result collect {
            case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
          }
          
          result2 must contain(77, 13)
        }
      }

      "complement of equality" >> {
        val line = Line(0, "")
        val numbers = dag.LoadLocal(line, Root(line, CString("/het/numbers")))
        
        val input = Filter(line, IdentitySort,
          numbers,
          Operate(line, Comp,
            Join(line, Eq, CrossLeftSort,
              numbers,
              Root(line, CLong(13)))))
          
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
        
      val numbers = dag.LoadLocal(line, Root(line, CString("/hom/numbers")))
      
      val input = Join(line, Mul, IdentitySort,
        numbers,
        Join(line, Sub, CrossLeftSort,
          numbers,
          dag.LoadLocal(line, Root(line, CString("/hom/numbers3")))))
          
      testEval(input) { result =>
        result must haveSize(25)
        
        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 2 => d.toInt
        }
        
        result2 must haveSize(23)
        
        result2 must contain(0, -377, -780, 6006, -76, 5929, 1, 156, 169, 2, 1764,
          2695, 144, 1806, -360, 1176, -832, 182, 4851, -1470, -13, -41, -24)
      }
    }
    
    "correctly evaluate a match following a cross with equality" in {
      val line = Line(0, "")
      
      val numbers = dag.LoadLocal(line, Root(line, CString("/hom/numbers")))
      val numbers3 = dag.LoadLocal(line, Root(line, CString("/hom/numbers3")))
      
      val input = Join(line, And, IdentitySort,
        Join(line, And, CrossLeftSort,
          Join(line, Eq, IdentitySort, numbers, numbers),
          Join(line, Eq, IdentitySort, numbers3, numbers3)),
        Join(line, Eq, IdentitySort, numbers3, numbers3))
      
      testEval(input) { _ must not(beEmpty) }
    }
    
    "correctly order a match following a cross within a new" in {
      val line = Line(0, "")
      val numbers = dag.LoadLocal(line, Root(line, CString("/hom/numbers")))
      
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
    }
    
    "split on a homogeneous set" in {
      val line = Line(0, "")
      
      // 
      // nums := dataset(//hom/numbers)
      // sums('n) :=
      //   m := max(nums where nums < 'n)
      //   (nums where nums = 'n) + m     -- actually, we used split root, but close enough
      // sums
      // 
       
      val nums = dag.LoadLocal(line, Root(line, CString("/hom/numbers")))
      
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
    }

    "memoize properly in a load" in {
      val line = Line(0, "")

      val input0 = dag.Memoize(dag.LoadLocal(line, Root(line, CString("/clicks"))), 1)
      val input1 = dag.LoadLocal(line, Root(line, CString("/clicks")))

      testEval(input0) { result0 => {
        testEval(input1) { result1 =>
          result0.map({case (ids, v) => (ids.toSeq, v)}) must_== result1.map({case (ids, v) => (ids.toSeq, v)})
        }
      }}
    }

    "memoize properly in an add" in {
      val line = Line(0, "")

      val input0 = dag.Memoize(
        dag.Join(line, Add, CrossLeftSort, 
          dag.LoadLocal(line, Root(line, CString("/clicks"))),
          Root(line, CLong(5))),
        1)

      val input1 = dag.Join(line, Add, CrossLeftSort, 
          dag.LoadLocal(line, Root(line, CString("/clicks"))),
          Root(line, CLong(5)))

      testEval(input0) { result0 => {
        testEval(input1) { result1 =>
          result0.map({case (ids, v) => (ids.toSeq, v)}) must_== result1.map({case (ids, v) => (ids.toSeq, v)})
        }
      }}
    }
    
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
      val clicks = dag.LoadLocal(line, Root(line, CString("/clicks")))
       
      lazy val input: dag.Split = dag.Split(line,
        dag.Group(1,
          clicks,
          UnfixedSolution(0, 
            Join(line, DerefObject, CrossLeftSort,
              clicks,
              Root(line, CString("user"))))),
        Join(line, JoinObject, CrossLeftSort,
          Join(line, WrapObject, CrossLeftSort,
            Root(line, CString("user")),
            SplitParam(line, 0)(input)),
          Join(line, WrapObject, CrossLeftSort,
            Root(line, CString("num")),
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
    }

    "evaluate with on the clicks dataset" in {
      val line = Line(0, "")
      
      val input = Join(line, JoinObject, CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/clicks"))),
        Join(line, WrapObject, CrossLeftSort,
          Root(line, CString("t")),
          Root(line, CLong(42))))
          
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
    }
    
    "evaluate filter with null" in {
      val line = Line(0, "")
      val clicks = dag.LoadLocal(line, Root(line, CString("/clicks")))

      //
      // //clicks where //clicks.user = null
      //
      //
      val input = Filter(line, IdentitySort,
        clicks,
        Join(line, Eq, CrossLeftSort,
          Join(line, DerefObject, CrossLeftSort,
            clicks,
            Root(line, CString("user"))),
          Root(line, CNull)))

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
      val clicks = dag.LoadLocal(line, Root(line, CString("/clicks")))
       
      lazy val histogram: dag.Split = dag.Split(line,
        dag.Group(1,
          clicks,
          UnfixedSolution(0,
            Join(line, DerefObject, CrossLeftSort,
              clicks,
              Root(line, CString("user"))))),
        Join(line, JoinObject, CrossLeftSort,
          Join(line, WrapObject, CrossLeftSort,
            Root(line, CString("user")),
            SplitParam(line, 0)(histogram)),
          Join(line, WrapObject, CrossLeftSort,
            Root(line, CString("num")),
            dag.Reduce(line, Count,
              SplitGroup(line, 1, clicks.identities)(histogram)))))
       
      val input = Filter(line, IdentitySort,
        histogram,
        Join(line, Eq, CrossLeftSort,
          Join(line, DerefObject, CrossLeftSort,
            histogram,
            Root(line, CString("num"))),
          Root(line, CLong(9))))
                  
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
    }

    "evaluate with on the results of a histogram function" in {
      val line = Line(0, "")
      
      // 
      // clicks := //clicks
      // histogram('user) :=
      //   { user: 'user, num: count(clicks where clicks.user = 'user) }
      // histogram with {rank: std::stats::rank(histogram.num)}
      // 

      val clicks = dag.LoadLocal(line, Root(line, CString("/clicks")))
       
      lazy val histogram: dag.Split = dag.Split(line,
        dag.Group(1,
          clicks,
          UnfixedSolution(0,
            Join(line, DerefObject, CrossLeftSort,
              clicks,
              Root(line, CString("user"))))),
        Join(line, JoinObject, CrossLeftSort,
          Join(line, WrapObject, CrossLeftSort,
            Root(line, CString("user")),
            SplitParam(line, 0)(histogram)),
          Join(line, WrapObject, CrossLeftSort,
            Root(line, CString("num")),
            dag.Reduce(line, Count,
              SplitGroup(line, 1, clicks.identities)(histogram)))))

      val input = Join(line, JoinObject, IdentitySort,
        histogram,
        Join(line, WrapObject, CrossLeftSort,
          Root(line, CString("rank")),
          dag.Morph1(line, Rank, 
            Join(line, DerefObject, CrossLeftSort,
              histogram,
              Root(line, CString("num"))))))

      testEval(input) { resultsE =>
        resultsE must haveSize(10)
        
        val results = resultsE collect {
          case (ids, sv) if ids.length == 1 => sv
        }

        results must contain(SObject(Map("user" -> SString("daniel"), "num" -> SDecimal(BigDecimal("9")), "rank" -> SDecimal(BigDecimal("5")))))
        results must contain(SObject(Map("user" -> SString("kris"), "num" -> SDecimal(BigDecimal("8")), "rank" -> SDecimal(BigDecimal("4")))))
        results must contain(SObject(Map("user" -> SString("derek"), "num" -> SDecimal(BigDecimal("7")), "rank" -> SDecimal(BigDecimal("2")))))
        results must contain(SObject(Map("user" -> SString("nick"), "num" -> SDecimal(BigDecimal("17")), "rank" -> SDecimal(BigDecimal("10")))))
        results must contain(SObject(Map("user" -> SString("john"), "num" -> SDecimal(BigDecimal("13")), "rank" -> SDecimal(BigDecimal("7")))))
        results must contain(SObject(Map("user" -> SString("alissa"), "num" -> SDecimal(BigDecimal("7")), "rank" -> SDecimal(BigDecimal("2")))))
        results must contain(SObject(Map("user" -> SString("franco"), "num" -> SDecimal(BigDecimal("13")), "rank" -> SDecimal(BigDecimal("7")))))
        results must contain(SObject(Map("user" -> SString("matthew"), "num" -> SDecimal(BigDecimal("10")), "rank" -> SDecimal(BigDecimal("6")))))
        results must contain(SObject(Map("user" -> SString("jason"), "num" -> SDecimal(BigDecimal("13")), "rank" -> SDecimal(BigDecimal("7")))))
        results must contain(SObject(Map("user" -> SNull, "num" -> SDecimal(BigDecimal("3")), "rank" -> SDecimal(BigDecimal("1")))))
      }
    }
    
    "perform a naive cartesian product on the clicks dataset" in {
      val line = Line(0, "")
      val clicks = dag.LoadLocal(line, Root(line, CString("/clicks")))
      
      val input = Join(line, JoinObject, CrossLeftSort,
        Join(line, WrapObject, CrossLeftSort,
          Root(line, CString("aa")),
          Join(line, DerefObject, CrossLeftSort,
            clicks,
            Root(line, CString("user")))),
        Join(line, WrapObject, CrossLeftSort,
          Root(line, CString("bb")),
          Join(line, DerefObject, CrossLeftSort,
            dag.New(line, clicks),
            Root(line, CString("user")))))
            
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
      dag.LoadLocal(line, Root(line, CString("/hom/numbers2"))))
      
      testEval(input) { result =>
        result must haveSize(5)
        
        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
        }
        
        result2 must contain(42, 12, 77, 1, 13)
      }
    }

    "distinct heterogenous sets" in {
      val line = Line(0, "")
      
      val input = dag.Distinct(line,
      dag.LoadLocal(line, Root(line, CString("/het/numbers2"))))
      
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
    }
    
    "join two sets according to a value sort" in {
      val line = Line(0, "")
      
      val clicks = dag.LoadLocal(line, Root(line, CString("/clicks")))
      val clicks2 = dag.LoadLocal(line, Root(line, CString("/clicks2")))
      
      val input = dag.Join(line,
        Add,
        ValueSort(0),
        SortBy(clicks, "time", "time", 0),
        SortBy(clicks2, "time", "time", 0))
        
      testEval(dag.Join(line, DerefObject, CrossLeftSort, clicks, Root(line, CString("time")))) { expected =>
        val decimalValues = expected.toList collect {
          case (_, SDecimal(d)) => d 
        }

        val cross = 
          for {
            x <- decimalValues
            y <- decimalValues
          } yield {
            (x, y) 
          }

        val expectedResult = cross collect { case (x, y) if x == y => x + y }

        testEval(input) { result =>
          result must haveSize(expectedResult.size)
          
          val decis = result.toList collect { 
            case (_, SDecimal(d)) => d
          }
          decis.sorted mustEqual expectedResult.sorted

          forall(result) { result =>
            result must beLike {
              case (ids, SDecimal(d)) => {
                ids must haveSize(2)
                expectedResult must contain(d)
              }
            }
          }
        }
      }
    }    

    "join two sets according to a value sort and then an identity sort" in {
      val line = Line(0, "")
      
      val clicks = dag.LoadLocal(line, Root(line, CString("/clicks")))
      val clicks2 = dag.LoadLocal(line, Root(line, CString("/clicks2")))
      
      val input = dag.Join(line, Eq, IdentitySort,
        dag.Join(line, Add, ValueSort(0),
          SortBy(clicks, "time", "time", 0),
          SortBy(clicks2, "time", "time", 0)),
        dag.Join(line, Mul, CrossLeftSort,
          dag.Join(line, DerefObject, CrossLeftSort, clicks, Root(line, CString("time"))),
          Root(line, CLong(2))))
        
      testEval(input) { result =>
        result must haveSize(106)

        val result2 = result collect {
          case (ids, SBoolean(b)) if ids.size == 2 => b
        }

        result2 must contain(true)
        result2 must not(contain(false))
      }
    }

    "filter two sets according to a value sort" in {
      val line = Line(0, "")
      
      val clicks = dag.LoadLocal(line, Root(line, CString("/clicks")))
      val clicks2 = dag.LoadLocal(line, Root(line, CString("/clicks2")))
      
      val input = dag.Filter(line,
        ValueSort(0),
        SortBy(clicks, "time", "time", 0),
        dag.Join(line,
          Gt,
          CrossLeftSort,
          SortBy(clicks2, "time", "time", 0),
          Root(line, CLong(500))))
        
      testEval(dag.Join(line, DerefObject, CrossLeftSort, clicks, Root(line, CString("time")))) { expected =>
        val decimalValues = expected.toList collect {
          case (_, SDecimal(d)) => d
        }

        val cross = 
          for {
            x <- decimalValues
            y <- decimalValues
          } yield {
            (x, y) 
          }

        val expectedResult = cross collect { case (x, y) if x > 500 && x == y  => x }

        testEval(input) { result =>
          result must haveSize(expectedResult.size)

          val decis = result.toList collect { 
            case (_, SDecimal(d)) => d
          }
          decis.sorted mustEqual expectedResult.sorted

          
          forall(result) { result =>
            result must beLike {
              case (ids, SDecimal(d)) => {
                ids must haveSize(2)
                expectedResult must contain(d)
              }
            }
          }
        }
      }
    }
    
    "produce a preemptive error when crossing enormous sets" in {
      val line = Line(0, "")
      
      val tweets = dag.LoadLocal(line, Root(line, CString("/election/tweets")))
      
      val input = dag.Join(line, Add, CrossLeftSort,
        dag.Join(line, Add, CrossLeftSort,
          tweets,
          tweets),
        tweets)
        
      testEval(input) { _ => failure } must throwAn[EnormousCartesianException]
    }
  }

  def joinDeref(left: DepGraph, first: Int, second: Int, line: Line): DepGraph = 
    Join(line, DerefArray, CrossLeftSort,
      Join(line, DerefArray, CrossLeftSort,
        left,
        Root(line, CLong(first))),
      Root(line, CLong(second)))

}

object EvaluatorSpecs extends EvaluatorSpecs[YId] with test.YIdInstances
