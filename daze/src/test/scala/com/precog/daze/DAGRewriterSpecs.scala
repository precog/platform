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

      val input = dag.LoadLocal(Const(CString("/numbers"))(line))(line)

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

      val t1 = dag.LoadLocal(Const(CString("/hom/pairs"))(line))(line)

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
