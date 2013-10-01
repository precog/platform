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
package com.precog.mimir

import org.specs2.mutable._

import com.precog.common._
import com.precog.yggdrasil._

import scalaz._

trait NormalizationSpecs[M[+_]] extends Specification
    with EvaluatorTestSupport[M]
    with LongIdMemoryDatasetConsumer[M] { self =>

  import dag._
  import instructions._
  import library._

  def testEval(graph: DepGraph): Set[SEvent] = {
    consumeEval(graph, defaultEvaluationContext) match {
      case Success(results) => results
      case Failure(error) => throw error
    }
  }

  private val line = Line(1, 1, "")
  private def load(path: String) = 
    dag.AbsoluteLoad(Const(CString(path))(line))(line)

  // note: more comprehensive `summary` and `normalization` tests found in muspelheim

  "summary" should {
    "work with heterogeneous numeric types" in {
      val input = dag.Morph1(Summary, load("/hom/numbersHet"))(line)

      val result = testEval(input)

      result must haveSize(1)

      result must haveAllElementsLike {
        case (ids, SObject(obj)) => {
          ids must haveSize(0)
          obj.keySet mustEqual Set("model1")

          obj("model1") must beLike { case SObject(summary) =>
            summary.keySet mustEqual Set("count", "stdDev", "min", "max", "mean")

            summary("count") must beLike { case SDecimal(d) => 
              d.toDouble mustEqual(13)
            }
            summary("mean") must beLike { case SDecimal(d) => 
              d.toDouble mustEqual(-37940.51855769231)
            }
            summary("stdDev") must beLike { case SDecimal(d) => 
              d.toDouble mustEqual(133416.18997644997)
            }
            summary("min") must beLike { case SDecimal(d) => 
              d.toDouble mustEqual(-500000)
            }
            summary("max") must beLike { case SDecimal(d) => 
              d.toDouble mustEqual(9999)
            }
          }
        }

        case _ => ko
      }
    }
  }

  "normalization" should {
    "denormalized normalized data with two summaries" in {
      val summary1 = dag.Morph1(Summary, load("/hom/numbersHet"))(line)
      val summary2 = dag.Morph1(Summary, load("/hom/numbers"))(line)

      val model1 = dag.Join(DerefObject, Cross(None),
        summary1,
        Const(CString("model1"))(line))(line)
      val model2 = dag.Join(DerefObject, Cross(None),
        summary2,
        Const(CString("model1"))(line))(line)

      val summaries = dag.IUI(true, model1, model2)(line)

      def makeNorm(summary: DepGraph) = {
        dag.Morph2(Normalization,
          load("hom/numbers"),
          summary)(line)
      }

      val input1 = makeNorm(model1)
      val input2 = makeNorm(model2)

      val expected = dag.IUI(true, input1, input2)(line)

      val input = makeNorm(summaries)

      val result = testEval(input)
      result must haveSize(10)

      val resultValue = result collect {
        case (ids, value) if ids.size == 1 => value
      }

      val expectedValue = testEval(expected) collect {
        case (ids, value) if ids.size == 1 => value
      }

      expectedValue mustEqual resultValue
    }
  }
}

object NormalizationSpecs extends NormalizationSpecs[test.YId] with test.YIdInstances
