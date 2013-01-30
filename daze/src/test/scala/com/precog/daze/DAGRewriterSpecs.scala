package com.precog.daze

import org.specs2.mutable._

import com.precog.common.Path
import com.precog.yggdrasil._
import org.joda.time.DateTime

import blueeyes.json._

import scalaz.{ FirstOption, NaturalTransformation, Tag }
import scalaz.std.anyVal.booleanInstance.disjunction
import scalaz.std.option.optionFirst
import scalaz.syntax.copointed._

trait DAGRewriterSpecs[M[+_]] extends Specification
    with EvaluatorTestSupport[M] {

  import dag._
  import instructions._

  implicit val nt = NaturalTransformation.refl[M]

  val evaluator = Evaluator(M)
  import evaluator._
  import library._

  "DAG rewriting" should {
    /*"compute identities given a relative path" in {
      val line = Line(1, 1, "")

      val input = dag.LoadLocal(Const(JString("/numbers"))(line))(line)

      val ctx = EvaluationContext("testAPIKey", Path.Root, new DateTime())
      val result = fullRewriteDAG(true, ctx)(input)

      result.identities mustEqual Identities.Specs(Vector(LoadIds("/numbers")))
    }

    "rewrite to have constant" in {
      /*
       * foo := //foo
       * foo.a + count(foo) + foo.c
       */

      val line = Line(1, 1, "")

      val t1 = dag.LoadLocal(Const(JString("/hom/pairs"))(line))(line)

      val input =
        Join(Add, IdentitySort,
          Join(Add, CrossLeftSort,
            Join(DerefObject, CrossLeftSort,
              t1,
              Const(JString("first"))(line))(line),
            dag.Reduce(Count, t1)(line))(line),
          Join(DerefObject, CrossLeftSort,
            t1,
            Const(JString("second"))(line))(line))(line)

      val ctx = EvaluationContext("testAPIKey", Path.Root, new DateTime())
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
        CNum(42),
        Set.empty
      )

      val hasMegaReduce = rewritten.foldDown(false) {
        case m@MegaReduce(_, _) => true
      }(disjunction)
      val hasConst = rewritten.foldDown(false) {
        case m@Const(CNum(n)) if n == 42 => true
      }(disjunction)

      // Must be turned into a Const node
      hasMegaReduce must beFalse
      hasConst must beTrue
    }*/

    "BLAH" in {
      /*
       * clicks := //clicks
       * solve 'time = clicks.time
       *   clicks where clicks.time >= 'time - 5 & clicks.time <= 'time + 5
       */

      val line = Line(1, 1, "")

      val clicks = dag.LoadLocal(Const(JString("/clicks"))(line))(line)

      val clicksTime =
        Join(DerefObject, CrossLeftSort,
          clicks,
          Const(JString("time"))(line)
        )(line)

      lazy val input: dag.Split =
        dag.Split(
          dag.Group(0,
            clicksTime,
            UnfixedSolution(1,
              clicksTime
            )
          ),
          Filter(IdentitySort,
            clicks,
            Join(And, IdentitySort,
              Join(GtEq, CrossLeftSort,
                clicksTime,
                Join(Sub, CrossLeftSort,
                  SplitParam(1)(input)(line),
                  Const(JNumLong(5))(line)
                )(line)
              )(line),
              Join(LtEq, CrossLeftSort,
                clicksTime,
                Join(Add, CrossLeftSort,
                  SplitParam(1)(input)(line),
                  Const(JNumLong(5))(line)
                )(line)
              )(line)
            )(line)
          )(line)
        )(line)

      val ctx = EvaluationContext("testAPIKey", Path.Root, new DateTime())
      val optimize = true

      //val optimizedDAG = fullRewriteDAG(optimize, ctx)(input)

      val result: M[Table] = eval(input, ctx, optimize)
      result.copoint.toJson.copoint foreach { println(_) }

      failure
    }
  }
}

object DAGRewriterSpecs extends DAGRewriterSpecs[test.YId] with test.YIdInstances
