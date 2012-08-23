package com.precog.daze

import org.specs2.mutable._

import com.precog.yggdrasil._

trait DAGRewriterSpecs[M[+_]] extends Specification 
    with Evaluator[M]
    with TestConfigComponent[M] {

  import dag._
  import instructions._

  "DAG rewriting" should {
    "compute identities given a relative path" in {
      val line = Line(0, "")

      val input = dag.LoadLocal(line, Root(line, PushString("/numbers")))

      val result = rewriteDAG(true)(input)

      result.identities mustEqual Vector(LoadIds("/numbers"))
    }
  }
}

object DAGRewriterSpecs extends DAGRewriterSpecs[test.YId] {
  val M = test.YId.M
  val coM = test.YId.M
}
