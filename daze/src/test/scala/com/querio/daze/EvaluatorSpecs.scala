package com.querio
package daze

import org.specs2.mutable._

object EvaluatorSpecs extends Specification with Evaluator {
  import IterV._
  
  import dag._
  import instructions._
  
  "bytecode evaluator" should {
    "evaluate simple two-value multiplication" in {
      val line = Line(0, "")
      val input = Join(line, Map2Cross(Mul), Root(line, PushNum("6")), Root(line, PushNum("7")))
      val result = consumeEval(input)
      
      result must haveSize(1)
      
      val (_, sv) = result.head
      sv must beLike {
        case SDecimal(d) => d mustEqual 42
      }
    }
  }
  
  private def consumeEval(graph: DepGraph): Vector[SEvent] =
    (consume >>== eval(graph).enum) run { err => sys.error("O NOES!!!") }
  
  // apparently, this doesn't *really* exist in Scalaz
  private def consume: IterV[A, Vector[A]] = {
    def step(acc: Vector[A])(in: Input[A]): IterV[A, Vector[A]] = {
      in(el = { e => Cont(step(acc :+ e)) },
         empty = Cont(step(acc)),
         eof = Done(acc, EOF.apply))
    }
    
    Cont(step(Vector()))
  }
}
