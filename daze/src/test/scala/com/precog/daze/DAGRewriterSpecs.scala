package com.precog.daze

import org.specs2.mutable._

import com.precog.common.Path
import com.precog.yggdrasil._
import org.joda.time.DateTime

import scalaz.{ Tag, FirstOption }
import scalaz.std.anyVal.booleanInstance.disjunction
import scalaz.std.option.optionFirst
import scalaz.syntax.copointed._

trait DAGRewriterSpecs[M[+_]] extends Specification
    with ReductionLib[M]
    with EvaluatorTestSupport[M] {

  import dag._
  import instructions._

  "DAG rewriting" should {
    "compute identities given a relative path" in {
      val line = Line(0, "")

      val input = dag.LoadLocal(line, Const(line, CString("/numbers")))

      val ctx = EvaluationContext("testAPIKey", Path.Root, new DateTime())
      val result = rewriteDAG(true, ctx)(input)

      result.identities mustEqual Identities.Specs(Vector(LoadIds("/numbers")))
    }

    "rewrite to have constant" in {
      /*
       * foo := //foo
       * foo.a + count(foo) + foo.c
       */

      val line = Line(0, "")

      val t1 = dag.LoadLocal(line, Const(line, CString("/hom/pairs")))

      val input =
        Join(line, Add, IdentitySort,
          Join(line, Add, CrossLeftSort,
            Join(line, DerefObject, CrossLeftSort,
              t1,
              Const(line, CString("first"))),
            dag.Reduce(line, Count, t1)),
          Join(line, DerefObject, CrossLeftSort,
            t1,
            Const(line, CString("second"))))

      val ctx = EvaluationContext("testAPIKey", Path.Root, new DateTime())
      val optimize = true

      // The should be a MegaReduce for the Count reduction
      val optimizedDAG = rewriteDAG(optimize, ctx)(input)
      val megaReduce = optimizedDAG.foldDown(true) {
        case m@MegaReduce(_, _, _) => Tag(Some(m)): FirstOption[DepGraph]
      }

      megaReduce must beSome

      val rewritten = inlineNodeValue(
        optimizedDAG,
        megaReduce.get,
        CNum(42)
      )

      val hasMegaReduce = rewritten.foldDown(false) {
        case m@MegaReduce(_, _, _) => true
      }(disjunction)
      val hasConst = rewritten.foldDown(false) {
        case m@Const(_, CNum(n)) if n == 42 => true
      }(disjunction)

      // Must be turned into a Const node
      hasMegaReduce must beFalse
      hasConst must beTrue
    }
  }
}

object DAGRewriterSpecs extends DAGRewriterSpecs[test.YId] with test.YIdInstances
