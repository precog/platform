package com.precog
package daze

import yggdrasil._
import yggdrasil.test._

import org.specs2.execute.Result
import org.specs2.mutable.Specification
import com.precog.common.Path
import com.precog.bytecode.JType.JUnfixedT
import com.precog.yggdrasil.SObject

import scalaz.Failure
import scalaz.Success

trait PathRelativizerSpec[M[+_]] extends Specification
    with Evaluator[M]
    with StdLib[M]
    with TestConfigComponent[M]
    with MemoryDatasetConsumer[M] { self =>
  
  import Function._
  
  import dag._
  import instructions._

  val testUID = "testUID"

  def testEval(graph: DepGraph,prefix: Path)(test: Set[SEvent] => Result): Result = withContext { ctx =>
    (consumeEval(testUID, graph, ctx,prefix) match {
      case Success(results) => test(results)
      case Failure(error) => throw error
    }) 
  }

  "path relativization" should {
    "prefix LoadLocal paths" in {
      val line = Line(0, "")
      val input = dag.LoadLocal(line, Root(line, PushString("/numbers")))

      testEval(input, Path("/hom")) { result =>
        result must haveSize(5)
        
        val result2 = result collect {
          case (ids, SDecimal(d)) if ids.size == 1 => d.toInt
        }
        
        result2 must contain(42, 12, 77, 1, 13)
      }
    }

    "prefix multiple LoadLocal paths" in {
      val line = Line(0, "")

      val input = Join(line, Add, IdentitySort,
        Join(line, DerefObject, CrossLeftSort, 
          dag.LoadLocal(line, Root(line, PushString("/heightWeight"))),
          Root(line, PushString("weight"))),
        Join(line, DerefObject, CrossLeftSort,
          dag.LoadLocal(line, Root(line, PushString("/heightWeight"))),
          Root(line, PushString("height"))))

      testEval(input, Path("/hom")) { result =>
        result must haveSize(5)
      }
    }
  }
}

object PathRelativizerSpec extends PathRelativizerSpec[YId] with YIdInstances
