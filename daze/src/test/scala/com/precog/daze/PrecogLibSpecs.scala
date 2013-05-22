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
import com.precog.common.Path

import scalaz._
import scalaz.std.list._

import com.precog.util.IdGen
import com.precog.util.IOUtils

trait PrecogLibSpecs[M[+_]] extends Specification
    with EvaluatorTestSupport[M] 
    with LongIdMemoryDatasetConsumer[M] { self =>

  import Function._

  import dag._
  import instructions._
  import library._

  import TableModule.CrossOrder._

  val testAPIKey = "testAPIKey"

  def testEval(graph: DepGraph): Set[SEvent] = {
    consumeEval(graph, defaultEvaluationContext) match {
      case Success(results) => results
      case Failure(error) => throw error
    }
  }

  private val line = Line(1, 1, "")
  private def const[A: CValueType](a: A) = Const(CValueType[A](a))(line)

  val echo = Join(WrapObject, Cross(None), const("url"), const("http://echo"))(line)
  // { "url": "http://wrapper", "options": { "field": "abc" } }
  val wrapper = Join(JoinObject, Cross(None),
    Join(WrapObject, Cross(None), const("url"), const("http://wrapper"))(line),
    Join(WrapObject, Cross(None),
      const("options"),
      Join(WrapObject, Cross(None), const("field"), const("abc"))(line))(line))(line)
  val misbehave = Join(WrapObject, Cross(None), const("url"), const("http://misbehave"))(line)
  val empty = Join(WrapObject, Cross(None), const("url"), const("http://empty"))(line)
  val serverError = Join(WrapObject, Cross(None), const("url"), const("http://server-error"))(line)

  "enrichment" should {
    "enrich a homogenous set" in {
      val input = Join(BuiltInFunction2Op(Enrichment), Cross(None),
          dag.LoadLocal(const("/hom/numbers4"))(line),
          echo)(line)

      val result = testEval(input)
      result must haveSize(6)
      val numbers = result collect { case (_, SDecimal(n)) => n }
      numbers must_== Set(0, -1, 1, 42, 1, -23)
    }

    "enrich a homogenous set by wrapping" in {
      val input = Join(BuiltInFunction2Op(Enrichment), Cross(None),
          dag.LoadLocal(const("/hom/numbers4"))(line),
          wrapper)(line)

      val result = testEval(input)
      result must haveSize(6)
      val numbers = result flatMap { case (_, SObject(fields)) =>
        fields get "abc" collect { case SDecimal(n) => n }
      }
      numbers must_== Set(0, -1, 1, 42, 1, -23)
    }

    "enrich a heterogeneous set" in {
      val input = Join(BuiltInFunction2Op(Enrichment), Cross(None),
          dag.LoadLocal(const("/het/numbers6"))(line),
          echo)(line)

      val result = testEval(input)
      result must haveSize(18)
      val data = result map { case (_, x) => x }
      data must contain(
        SDecimal(-10),
        SArray(Vector(9, 10, 11) map (SDecimal(_))),
        SString("alissa"),
        SNull,
        SFalse,
        STrue,
        SDecimal(5),
        SObject(Map.empty))
    }

    "misbehaving enricher fails" in {
      val input = Join(BuiltInFunction2Op(Enrichment), Cross(None),
          dag.LoadLocal(const("/hom/numbers4"))(line),
          misbehave)(line)

      testEval(input) must throwA[Throwable]
    }

    "empty enricher fails" in {
      val input = Join(BuiltInFunction2Op(Enrichment), Cross(None),
          dag.LoadLocal(const("/hom/numbers4"))(line),
          empty)(line)

      testEval(input) must throwA[Throwable]
    }

    "failing enricher fails" in {
      val input = Join(BuiltInFunction2Op(Enrichment), Cross(None),
          dag.LoadLocal(const("/hom/numbers4"))(line),
          serverError)(line)

      testEval(input) must throwA[Throwable]
    }
  }
}

object PrecogLibSpecs extends PrecogLibSpecs[test.YId] with test.YIdInstances
