package com.precog
package daze

import com.precog.common.Path

import com.precog.yggdrasil._
import com.precog.yggdrasil.memoization._
import com.precog.yggdrasil.serialization._
import com.precog.common.VectorCase
import com.precog.util.IOUtils
import com.precog.util.IdGen

import akka.dispatch.Await
import akka.util.duration._

import java.io._
import scalaz._
import scalaz.effect._
import scalaz.iteratee._
import scalaz.std.list._
import Iteratee._

import org.specs2.mutable._

trait TestConfigComponent extends table.ColumnarTableModule {
  object yggConfig extends YggConfig
  
  trait StubTableOps extends ColumnarTableOps {
    private var initialIndices = collection.mutable.Map[Path, Int]()
    private var currentIndex = 0
    
    override def loadStatic(path: Path): Table = {
      import blueeyes.json._
      import JsonAST.{JObject, JField, JArray, JInt}
      
      val index = initialIndices get path getOrElse {
        initialIndices += (path -> currentIndex)
        currentIndex
      }
      
      val target = path.path.replaceAll("/$", ".json")
      val src = io.Source fromInputStream getClass.getResourceAsStream(target)
      val parsed = src.getLines map JsonParser.parse toStream
      
      currentIndex += parsed.length
      
      val attributed = parsed zip (Stream from index) map {
        case (value, id) => JObject(JField("key", JArray(JInt(id) :: Nil)) :: JField("value", value) :: Nil)
      }
      
      Table fromJson attributed
    }
    
    override def loadDynamic(source: Table): Table = sys.error("todo")
  }
  
  override def ops = new StubTableOps {}

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

class EvaluatorSpecs extends Specification
    with Evaluator
    with TestConfigComponent 
    with StdLib
    with MemoryDatasetConsumer { self =>
  
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

      "push_null" >> {
        val line = Line(0, "")
        val input = Root(line, PushNull)
        val result = testEval(input)
        
        result must haveSize(1)
        
        result must contain((VectorCase(), SNull))
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
      val input = dag.LoadLocal(line, Root(line, PushString("/hom/numbers")))
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
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers"))))
        
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
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(42, 12, 77, 1, 13)
    }

    "join two sets with a match" >> {
      "from different paths" >> {
        val line = Line(0, "")

        val input = Join(line, Map2Match(Add),
          Join(line, Map2Cross(DerefObject), 
            dag.LoadLocal(line, Root(line, PushString("/clicks"))),
            Root(line, PushString("time"))),
          Join(line, Map2Cross(DerefObject),
            dag.LoadLocal(line, Root(line, PushString("/hom/heightWeight"))),
            Root(line, PushString("height"))))

        val result = testEval(input)

        result must haveSize(500)
      }

      "from the same path" >> {
        val line = Line(0, "")

        val input = Join(line, Map2Match(Add),
          Join(line, Map2Cross(DerefObject), 
            dag.LoadLocal(line, Root(line, PushString("/hom/heightWeight"))),
            Root(line, PushString("weight"))),
          Join(line, Map2Cross(DerefObject),
            dag.LoadLocal(line, Root(line, PushString("/hom/heightWeight"))),
            Root(line, PushString("height"))))

        val result = testEval(input)

        result must haveSize(5)
      }
    }

    "evaluate a binary numeric operation mapped over homogeneous numeric set" >> {
      "addition" >> {
        val line = Line(0, "")
        
        val input = Join(line, Map2Cross(Add),
          dag.LoadLocal(line, Root(line, PushString("/hom/numbers"))),
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
          dag.LoadLocal(line, Root(line, PushString("/hom/numbers"))),
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
          dag.LoadLocal(line, Root(line, PushString("/hom/numbers"))),
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
          dag.LoadLocal(line, Root(line, PushString("/hom/numbers"))),
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
          dag.LoadLocal(line, Root(line, PushString("/het/numbers"))),
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
          dag.LoadLocal(line, Root(line, PushString("/het/numbers"))),
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
          dag.LoadLocal(line, Root(line, PushString("/het/numbers"))),
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
          dag.LoadLocal(line, Root(line, PushString("/het/numbers"))),
          Root(line, PushNum("5")))
          
        val result = testEval(input)
        
        result must haveSize(5)
        
        val result2 = result collect {
          case (VectorCase(_), SDecimal(d)) => d.toDouble
        }
        
        result2 must contain(8.4, 2.4, 15.4, 0.2, 2.6)
      }
    }

    "reduce a filtered dataset" >> {
      val line = Line(0, "")

      val input = dag.Reduce(line, Count,
        Filter(line, None,
          dag.LoadLocal(line, Root(line, PushString("/clicks"))),
          Join(line, Map2Cross(Gt),
            Join(line, Map2Cross(DerefObject),
              dag.LoadLocal(line, Root(line, PushString("/clicks"))),
              Root(line, PushString("time"))),
            Root(line, PushNum("0")))))

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect {
        case (VectorCase(), SDecimal(d)) => d.toInt
      }

      result2 must contain(100)
    }

    "evaluate cross when one side is a singleton" >> {
      "a reduction on the right side of the cross" >> {
        val line = Line(0, "")

        val input = Join(line, Map2Cross(Add), 
          dag.LoadLocal(line, Root(line, PushString("/hom/numbers"))),
          dag.Reduce(line, Count, 
            Root(line, PushNum("42"))))

        val result = testEval(input)

        result must haveSize(5)

        val result2 = result collect {
          case (VectorCase(_), SDecimal(d)) => d
        }

        result2 must contain(43, 13, 78, 2, 14)
      }

      "a reduction on the left side of the cross" >> {
        val line = Line(0, "")

        val input = Join(line, Map2Cross(Add), 
          dag.Reduce(line, Count, 
            Root(line, PushNum("42"))),
          dag.LoadLocal(line, Root(line, PushString("/hom/numbers"))))

        val result = testEval(input)

        result must haveSize(5)

        val result2 = result collect {
          case (VectorCase(_), SDecimal(d)) => d
        }

        result2 must contain(43, 13, 78, 2, 14)
      }

      "a root on the right side of the cross" >> {
        val line = Line(0, "")

        val input = Join(line, Map2Cross(Add),  
          dag.LoadLocal(line, Root(line, PushString("/hom/numbers"))),
          Root(line, PushNum("3")))
         
        val result = testEval(input)

        result must haveSize(5)

        val result2 = result collect {
          case (VectorCase(_), SDecimal(d)) => d
        }

        result2 must contain(45, 15, 80, 4, 16)
      }

      "a root on the left side of the cross" >> {
        val line = Line(0, "")

        val input = Join(line, Map2Cross(Add), 
          Root(line, PushNum("3")),
          dag.LoadLocal(line, Root(line, PushString("/hom/numbers"))))

        val result = testEval(input)

        result must haveSize(5)

        val result2 = result collect {
          case (VectorCase(_), SDecimal(d)) => d
        }

        result2 must contain(45, 15, 80, 4, 16)
      }
    }

    "evaluate wrap_object on a single numeric value" in {
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

    "evaluate wrap_object on an object" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Cross(WrapObject),
        Root(line, PushString("answer")),
        Join(line, Map2Cross(WrapObject),
          Root(line, PushString("question")),
          Root(line, PushNull)))
        
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
        case SObject(obj) => { 
          obj must haveKey("question")
          obj("question") mustEqual SNull
        }
      }
    }
    
    "evaluate wrap_object on clicks dataset" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Cross(WrapObject),
        Root(line, PushString("aa")),
        Join(line, Map2Cross(DerefObject),
          dag.LoadLocal(line, Root(line, PushString("/clicks"))),
          Root(line, PushString("user"))))
        
      val result = testEval(input)
      
      result must haveSize(100)
      
      forall(result) {
        case (VectorCase(_), SObject(obj)) => {
          obj must haveSize(1)
          obj must haveKey("aa")
        }
      }
    }
    
    "evaluate wrap_array on a single numeric value" in {
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

    "evaluate wrap_array on a single null value" in {
      val line = Line(0, "")
      
      val input = Operate(line, WrapArray,
        Root(line, PushNull))
        
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
      arr.head mustEqual SNull
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
        dag.LoadLocal(line, Root(line, PushString("/hom/pairs"))),
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
        dag.LoadLocal(line, Root(line, PushString("/het/pairs"))),
        Root(line, PushString("first")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
        case (VectorCase(_), SNull) => SNull
      }
      
      result2 must contain(42, 12, 1, 13, SNull)
    }
    
    "evaluate descent producing a heterogeneous set" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Cross(DerefObject),
        dag.LoadLocal(line, Root(line, PushString("/het/het-pairs"))),
        Root(line, PushString("first")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
        case (VectorCase(_), SString(str)) => str
        case (VectorCase(_), SBoolean(b)) => b
        case (VectorCase(_), SNull) => SNull
      }
      
      result2 must contain(42, true, "daniel", 1, SNull)
    }
    
    "evaluate array dereference on a homogeneous set" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Cross(DerefArray),
        dag.LoadLocal(line, Root(line, PushString("/hom/arrays"))),
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
        dag.LoadLocal(line, Root(line, PushString("/het/arrays"))),
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
        dag.LoadLocal(line, Root(line, PushString("/het/het-arrays"))),
        Root(line, PushNum("2")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
        case (VectorCase(_), SString(str)) => str
        case (VectorCase(_), SBoolean(b)) => b
        case (VectorCase(_), SNull) => SNull
      }
      
      result2 must contain(42, true, "daniel", 1, SNull)
    }
    
    "evaluate matched binary numeric operation" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Match(Sub),
        Join(line, Map2Cross(DerefObject),
          dag.LoadLocal(line, Root(line, PushString("/hom/pairs"))),
          Root(line, PushString("first"))),
        Join(line, Map2Cross(DerefObject),
          dag.LoadLocal(line, Root(line, PushString("/hom/pairs"))),
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
          dag.LoadLocal(line, Root(line, PushString("/hom/pairs"))),
          Root(line, PushString("first"))),
        Join(line, Map2Cross(DerefObject),
          dag.LoadLocal(line, Root(line, PushString("/hom/pairs"))),
          Root(line, PushString("second"))))
        
      val result = testEval(input)
      
      result must haveSize(4)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toDouble
      }
      
      result2 must contain(7, -2.026315789473684, 0.006024096385542169, 13)
    }
    
    "compute the set difference of two sets" in {
      val line = Line(0, "")
      
      val input = Join(line, SetDifference,
        dag.LoadLocal(line, Root(line, PushString("/clicks2"))),
        Join(line, Map2Cross(DerefObject),
          dag.LoadLocal(line, Root(line, PushString("/clicks2"))),
          Root(line, PushString("time"))))
        
      val result = testEval(input)
      
      result must haveSize(6)
      
      forall(result) {
        case (VectorCase(_), SObject(obj)) => {
          obj must not haveKey("time")
        }
        case (VectorCase(_), SString(s)) => s mustEqual "string cheese"
      }
    }    
    "compute the set difference of the set difference" in {
      val line = Line(0, "")
      
      val input = Join(line, SetDifference,
        dag.LoadLocal(line, Root(line, PushString("/clicks2"))),
        Join(line, SetDifference,
          dag.LoadLocal(line, Root(line, PushString("/clicks2"))),
          Join(line, Map2Cross(DerefObject),
            dag.LoadLocal(line, Root(line, PushString("/clicks2"))),
            Root(line, PushString("time")))))

      val result = testEval(input)
      
      result must haveSize(101)
      
      forall(result) {
        case (VectorCase(_), SObject(obj)) => {
          obj must haveKey("time")
        }
        case (VectorCase(_), SString(s)) => s mustEqual "string cheese"
      }
    }      
    
    "compute the iunion of two homogeneous sets" in {
      val line = Line(0, "")
      
      val input = Join(line, IUnion,
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers"))),
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers3"))))
        
      val result = testEval(input)
      
      result must haveSize(10)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toDouble
      }
      
      result2 must contain(42, 12, 77, 1, 13, 14, -1, 0)
    }
    
    "compute the iunion of two datasets" in {
      val line = Line(0, "")
      
      val input = Join(line, IUnion,
        dag.LoadLocal(line, Root(line, PushString("/clicks"))),
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers3"))))
        
      val result = testEval(input)
      
      result must haveSize(105)
    }
    
    "compute the iintersect of two homogeneous sets" in {
      val line = Line(0, "")
      
      val input = Join(line, IIntersect,
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers"))),
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers3"))))
        
      val result = testEval(input)
      
      result must haveSize(0)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toDouble
      }
      
      result2 must beEmpty
    }
    
    "compute the iintersect of two datasets" in {
      val line = Line(0, "")
      
      val input = Join(line, IIntersect,
        dag.LoadLocal(line, Root(line, PushString("/clicks"))),
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers3"))))
        
      val result = testEval(input)
      
      result must haveSize(0)
    }
    
    
    
    "filter homogeneous numeric set by binary operation" >> {
      "less-than" >> {
        val line = Line(0, "")
        
        val input = Filter(line, None,
          dag.LoadLocal(line, Root(line, PushString("/hom/numbers"))),
          Join(line, Map2Cross(Lt),
            dag.LoadLocal(line, Root(line, PushString("/hom/numbers"))),
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
        
        val input = Filter(line, None,
          dag.LoadLocal(line, Root(line, PushString("/hom/numbers"))),
          Join(line, Map2Cross(LtEq),
            dag.LoadLocal(line, Root(line, PushString("/hom/numbers"))),
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
        
        val input = Filter(line, None,
          dag.LoadLocal(line, Root(line, PushString("/hom/numbers"))),
          Join(line, Map2Cross(Gt),
            dag.LoadLocal(line, Root(line, PushString("/hom/numbers"))),
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
        
        val input = Filter(line, None,
          dag.LoadLocal(line, Root(line, PushString("/hom/numbers"))),
          Join(line, Map2Cross(GtEq),
            dag.LoadLocal(line, Root(line, PushString("/hom/numbers"))),
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
        
        val input = Filter(line, None,
          dag.LoadLocal(line, Root(line, PushString("/hom/numbers"))),
          Join(line, Map2Cross(Eq),
            dag.LoadLocal(line, Root(line, PushString("/hom/numbers"))),
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
        
        val input = Filter(line, None,
          dag.LoadLocal(line, Root(line, PushString("/hom/numbers"))),
          Join(line, Map2Cross(NotEq),
            dag.LoadLocal(line, Root(line, PushString("/hom/numbers"))),
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
        
        val input = Filter(line, None,
          dag.LoadLocal(line, Root(line, PushString("/hom/numbers"))),
          Join(line, Map2Match(And),
            Join(line, Map2Cross(NotEq),
              dag.LoadLocal(line, Root(line, PushString("/hom/numbers"))),
              Root(line, PushNum("77"))),
            Join(line, Map2Cross(NotEq),
              dag.LoadLocal(line, Root(line, PushString("/hom/numbers"))),
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
        
        val input = Filter(line, None,
          dag.LoadLocal(line, Root(line, PushString("/hom/numbers"))),
          Join(line, Map2Match(Or),
            Join(line, Map2Cross(Eq),
              dag.LoadLocal(line, Root(line, PushString("/hom/numbers"))),
              Root(line, PushNum("77"))),
            Join(line, Map2Cross(Eq),
              dag.LoadLocal(line, Root(line, PushString("/hom/numbers"))),
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
        
        val input = Filter(line, None,
          dag.LoadLocal(line, Root(line, PushString("/hom/numbers"))),
          Operate(line, Comp,
            Join(line, Map2Cross(Eq),
              dag.LoadLocal(line, Root(line, PushString("/hom/numbers"))),
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
        
        val input = Filter(line, None,
          dag.LoadLocal(line, Root(line, PushString("/het/numbers"))),
          Join(line, Map2Cross(Lt),
            dag.LoadLocal(line, Root(line, PushString("/het/numbers"))),
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
        
        val input = Filter(line, None,
          dag.LoadLocal(line, Root(line, PushString("/het/numbers"))),
          Join(line, Map2Cross(LtEq),
            dag.LoadLocal(line, Root(line, PushString("/het/numbers"))),
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
        
        val input = Filter(line, None,
          dag.LoadLocal(line, Root(line, PushString("/het/numbers"))),
          Join(line, Map2Cross(Gt),
            dag.LoadLocal(line, Root(line, PushString("/het/numbers"))),
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
        
        val input = Filter(line, None,
          dag.LoadLocal(line, Root(line, PushString("/het/numbers"))),
          Join(line, Map2Cross(GtEq),
            dag.LoadLocal(line, Root(line, PushString("/het/numbers"))),
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
        
        val input = Filter(line, None,
          dag.LoadLocal(line, Root(line, PushString("/het/numbers"))),
          Join(line, Map2Cross(Eq),
            dag.LoadLocal(line, Root(line, PushString("/het/numbers"))),
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
        
        val input = Filter(line, None,
          dag.LoadLocal(line, Root(line, PushString("/het/numbers"))),
          Join(line, Map2Cross(NotEq),
            dag.LoadLocal(line, Root(line, PushString("/het/numbers"))),
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
        
        val input = Filter(line, None,
          dag.LoadLocal(line, Root(line, PushString("/het/numbers"))),
          Join(line, Map2Match(And),
            Join(line, Map2Cross(NotEq),
              dag.LoadLocal(line, Root(line, PushString("/het/numbers"))),
              Root(line, PushNum("77"))),
            Join(line, Map2Cross(NotEq),
              dag.LoadLocal(line, Root(line, PushString("/het/numbers"))),
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
        
        val input = Filter(line, None,
          dag.LoadLocal(line, Root(line, PushString("/het/numbers"))),
          Join(line, Map2Match(Or),
            Join(line, Map2Cross(Eq),
              dag.LoadLocal(line, Root(line, PushString("/het/numbers"))),
              Root(line, PushNum("77"))),
            Join(line, Map2Cross(Eq),
              dag.LoadLocal(line, Root(line, PushString("/het/numbers"))),
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
        
        val input = Filter(line, None,
          dag.LoadLocal(line, Root(line, PushString("/het/numbers"))),
          Operate(line, Comp,
            Join(line, Map2Cross(Eq),
              dag.LoadLocal(line, Root(line, PushString("/het/numbers"))),
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
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers"))),
        Join(line, Map2Cross(Sub),
          dag.LoadLocal(line, Root(line, PushString("/hom/numbers"))),
          dag.LoadLocal(line, Root(line, PushString("/hom/numbers3")))))
          
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
        dag.LoadLocal(line, Root(line, PushString("/hom/numbers"))),
        Join(line, Map2Cross(Sub),
          dag.LoadLocal(line, Root(line, PushString("/hom/numbers"))),
          dag.New(line, dag.LoadLocal(line, Root(line, PushString("/hom/numbers"))))))
          
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
        Join(line, Map2Cross(Add),
          SplitGroup(line, 1, nums.provenance)(input),
          dag.Reduce(line, Max,
            Filter(line, None,
              nums,
              Join(line, Map2Cross(Lt),
                nums,
                SplitParam(line, 0)(input))))))
              
      val result = testEval(input)
      
      result must haveSize(4)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(55, 13, 119, 25)
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
      val clicks = dag.LoadLocal(line, Root(line, PushString("/clicks")))
       
      lazy val input: dag.Split = dag.Split(line,
        dag.Group(1,
          clicks,
          UnfixedSolution(0, 
            Join(line, Map2Cross(DerefObject),
              clicks,
              Root(line, PushString("user"))))),
        Join(line, Map2Cross(JoinObject),
          Join(line, Map2Cross(WrapObject),
            Root(line, PushString("user")),
            SplitParam(line, 0)(input)),
          Join(line, Map2Cross(WrapObject),
            Root(line, PushString("num")),
            dag.Reduce(line, Count,
              SplitGroup(line, 1, clicks.provenance)(input)))))
      
      val result = testEval(input)
      
      result must haveSize(10)
      
      forall(result) {
        case (VectorCase(_), SObject(obj)) => {
          obj must haveKey("user")
          obj must haveKey("num")
          
          obj("user") must beLike {
            case SString(str) => {
              str must beOneOf("daniel", "kris", "derek", "nick", "john",
                "alissa", "franco", "matthew", "jason")
            }
            case SNull => ok
          }

          val user = obj("user") match {
            case SString(user) => user
            case SNull => SNull
          }
            
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
        dag.LoadLocal(line, Root(line, PushString("/clicks"))),
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
    
    "evaluate filter with null" in {
      val line = Line(0, "")

      //
      // //clicks where //clicks.user = null
      //
      //
      val input = Filter(line, None,
        dag.LoadLocal(line, Root(line, PushString("/clicks"))),
        Join(line, Map2Cross(Eq),
          Join(line, Map2Cross(DerefObject),
            dag.LoadLocal(line, Root(line, PushString("/clicks"))),
            Root(line, PushString("user"))),
          Root(line, PushNull)))

      val result = testEval(input)

      result must haveSize(3)

      forall(result) {
        case (VectorCase(_), SObject(obj)) => {
          obj must haveKey("user")

          obj("user") must beLike {
            case SNull => ok
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
            Join(line, Map2Cross(DerefObject),
              clicks,
              Root(line, PushString("user"))))),
        Join(line, Map2Cross(JoinObject),
          Join(line, Map2Cross(WrapObject),
            Root(line, PushString("user")),
            SplitParam(line, 0)(histogram)),
          Join(line, Map2Cross(WrapObject),
            Root(line, PushString("num")),
            dag.Reduce(line, Count,
              SplitGroup(line, 1, clicks.provenance)(histogram)))))
       
      val input = Filter(line, None,
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
    
    "perform a naive cartesian product on the clicks dataset" in {
      val line = Line(0, "")
      
      val input = Join(line, Map2Cross(JoinObject),
        Join(line, Map2Cross(WrapObject),
          Root(line, PushString("aa")),
          Join(line, Map2Cross(DerefObject),
            dag.LoadLocal(line, Root(line, PushString("/clicks"))),
            Root(line, PushString("user")))),
        Join(line, Map2Cross(WrapObject),
          Root(line, PushString("bb")),
          Join(line, Map2Cross(DerefObject),
            dag.New(line, dag.LoadLocal(line, Root(line, PushString("/clicks")))),
            Root(line, PushString("user")))))
            
      val result = testEval(input)
      
      result must haveSize(10000)
      
      forall(result) {
        case (VectorCase(_, _), SObject(obj)) => {
          obj must haveSize(2)
          obj must haveKey("aa")
          obj must haveKey("bb")
        }
      }
    }

    "distinct homogenous set of numbers" >> {
      val line = Line(0, "")
      
      val input = dag.Distinct(line,
      dag.LoadLocal(line, Root(line, PushString("/hom/numbers2"))))
      
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(42, 12, 77, 1, 13)
    }
    
    "distinct heterogenous sets" >> {
      val line = Line(0, "")
      
      val input = dag.Distinct(line,
      dag.LoadLocal(line, Root(line, PushString("/het/numbers2"))))
      
      val result = testEval(input)
      
      result must haveSize(10)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
        case (VectorCase(_), SBoolean(b)) => b
        case (VectorCase(_), SString(s)) => s
        case (VectorCase(_), SArray(a)) => a
        case (VectorCase(_), SObject(o)) => o
      }
      
      result2 must contain(42, 12, 77, 1, 13, true, false, "daniel", Map("test" -> SString("fubar")), Vector())
    }
  }
}
