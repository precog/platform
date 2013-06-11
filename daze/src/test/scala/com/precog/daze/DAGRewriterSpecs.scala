package com.precog.daze

import org.specs2.mutable._

import com.precog.common._
import com.precog.yggdrasil._
import org.joda.time.DateTime

import scalaz.{ FirstOption, NaturalTransformation, Tag }
import scalaz.std.anyVal.booleanInstance.disjunction
import scalaz.std.option.optionFirst
import scalaz.syntax.comonad._

trait DAGRewriterSpecs[M[+_]] extends Specification
    with EvaluatorTestSupport[M] {

  import dag._
  import instructions._

  implicit val nt = NaturalTransformation.refl[M]

  val evaluator = Evaluator(M)
  import evaluator._
  import library._

  "DAG rewriting" should {
    "compute identities given a relative path" in {
      val line = Line(1, 1, "")

      val input = dag.AbsoluteLoad(Const(CString("/numbers"))(line))(line)

      val ctx = defaultEvaluationContext
      val result = fullRewriteDAG(true, ctx)(input)

      result.identities mustEqual Identities.Specs(Vector(LoadIds("/numbers")))
    }

    "rewrite to have constant" in {
      /*
       * foo := //foo
       * foo.a + count(foo) + foo.c
       */

      val line = Line(1, 1, "")

      val t1 = dag.AbsoluteLoad(Const(CString("/hom/pairs"))(line))(line)

      val input =
        Join(Add, IdentitySort,
          Join(Add, Cross(None),
            Join(DerefObject, Cross(None),
              t1,
              Const(CString("first"))(line))(line),
            dag.Reduce(Count, t1)(line))(line),
          Join(DerefObject, Cross(None),
            t1,
            Const(CString("second"))(line))(line))(line)

      val ctx = defaultEvaluationContext
      val optimize = true

      // The should be a MegaReduce for the Count reduction
      val optimizedDAG = fullRewriteDAG(optimize, ctx)(input)
      val megaReduce = optimizedDAG.foldDown(true) {
        case m@MegaReduce(_, _) => Tag(Some(m)): FirstOption[DepGraph]
      }

      megaReduce must beSome

      val rewritten = inlineNodeValue(
        optimizedDAG,
        megaReduce.get,
        CNum(42))

      val hasMegaReduce = rewritten.foldDown(false) {
        case m@MegaReduce(_, _) => true
      }(disjunction)
      val hasConst = rewritten.foldDown(false) {
        case m@Const(CNum(n)) if n == 42 => true
      }(disjunction)

      // Must be turned into a Const node
      hasMegaReduce must beFalse
      hasConst must beTrue
    }
  }
}

object DAGRewriterSpecs extends DAGRewriterSpecs[test.YId] with test.YIdInstances
