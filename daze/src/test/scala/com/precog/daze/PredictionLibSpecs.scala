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

import com.precog.common.Path
import com.precog.yggdrasil._

import scalaz._

trait PredictionLibSpecs[M[+_]] extends Specification
    with EvaluatorTestSupport[M]
    with PredictionLib[M]
    with LongIdMemoryDatasetConsumer[M]{ self =>

  import dag._
  import instructions._

  val testAPIKey = "testAPIKey"

  def testEval(graph: DepGraph): Set[SEvent] = {
    consumeEval(testAPIKey, graph, Path.Root) match {
      case Success(results) => results
      case Failure(error) => throw error
    }
  }
  
  "linear prediction" should {
    "predict simple case" in {
      val line = Line(0, "")

      val input = dag.Morph2(line, LinearPrediction,
        dag.LoadLocal(line, Const(line, CString("/hom/model1"))),
        dag.LoadLocal(line, Const(line, CString("/hom/model1data"))))

      val result0 = testEval(input)

      result0 must haveSize(19)

      val result = result0 collect { case (ids, value) if ids.size == 2 => value }

      result mustEqual Set(
        (SObject(Map("Model2" -> SDecimal(42.5), "Model1" -> SDecimal(48.5)))),
        (SObject(Map("Model2" -> SDecimal(41.0)))),
        (SObject(Map("Model2" -> SDecimal(8.0), "Model1" -> SDecimal(12.0)))), 
        (SObject(Map("Model2" -> SDecimal(17.0), "Model1" -> SDecimal(6.6)))), 
        (SObject(Map("Model2" -> SDecimal(26.0), "Model1" -> SDecimal(24.0)))), 
        (SObject(Map("Model2" -> SDecimal(23.0), "Model1" -> SDecimal(35.0)))), 
        (SObject(Map("Model2" -> SDecimal(29.0), "Model1" -> SDecimal(39.0)))), 
        (SObject(Map("Model2" -> SDecimal(2.0), "Model1" -> SDecimal(0.0)))), 
        (SObject(Map("Model2" -> SDecimal(-16.0), "Model1" -> SDecimal(-18.0)))), 
        (SObject(Map("Model3" -> SDecimal(9.5)))), 
        (SObject(Map("Model3" -> SDecimal(7.0)))), 
        (SObject(Map("Model3" -> SDecimal(-11.0)))), 
        (SObject(Map("Model3" -> SDecimal(19.75)))), 
        (SObject(Map("Model3" -> SDecimal(-0.5)))), 
        (SObject(Map("Model3" -> SDecimal(17.0)))), 
        (SObject(Map("Model3" -> SDecimal(14.5)))), 
        (SObject(Map("Model3" -> SDecimal(-0.5)))), 
        (SObject(Map("Model3" -> SDecimal(24.5)))), 
        (SObject(Map("Model3" -> SDecimal(-0.5)))))
    }

    "predict case with repeated model names and arrays" in {
      val line = Line(0, "")

      val input = dag.Morph2(line, LinearPrediction,
        dag.LoadLocal(line, Const(line, CString("/hom/model2"))),
        dag.LoadLocal(line, Const(line, CString("/hom/model2data"))))

      val result0 = testEval(input)

      result0 must haveSize(14)

      val result = result0 collect { case (ids, value) if ids.size == 2 => value }

      result mustEqual Set(
        (SObject(Map("Model1" -> SDecimal(8.0), "Model3" -> SDecimal(18.0)))), 
        (SObject(Map("Model1" -> SDecimal(17.0)))), 
        (SObject(Map("Model1" -> SDecimal(23.0)))),
        (SObject(Map("Model1" -> SDecimal(2.0)))), 
        (SObject(Map("Model2" -> SDecimal(14.0), "Model1" -> SDecimal(9.0)))), 
        (SObject(Map("Model1" -> SDecimal(18.0)))), 
        (SObject(Map("Model1" -> SDecimal(24.0)))),
        (SObject(Map("Model1" -> SDecimal(3.0)))), 
        (SObject(Map("Model3" -> SDecimal(0.0)))),
        (SObject(Map("Model3" -> SDecimal(7.2)))),
        (SObject(Map("Model3" -> SDecimal(-5.1)))), 
        (SObject(Map("Model2" -> SDecimal(36.0)))),
        (SObject(Map("Model3" -> SDecimal(-4.0)))),
        (SObject(Map("Model2" -> SDecimal(77.0)))))
    }
  }

  "logistic prediction" should {
    "predict simple case" in {
      val line = Line(0, "")

      val input = dag.Morph2(line, LogisticPrediction,
        dag.LoadLocal(line, Const(line, CString("/hom/model1"))),
        dag.LoadLocal(line, Const(line, CString("/hom/model1data"))))

      val result0 = testEval(input)

      result0 must haveSize(19)

      val result = result0 collect { case (ids, value) if ids.size == 2 => value }

      result mustEqual Set(
        (SObject(Map("Model2" -> SDecimal(3.487261531994447E-19), "Model1" -> SDecimal(8.644057113036095E-22)))),
        (SObject(Map("Model2" -> SDecimal(1.5628821893349888E-18)))),
        (SObject(Map("Model2" -> SDecimal(0.0003353501304664781), "Model1" -> SDecimal(0.000006144174602214718)))),
        (SObject(Map("Model2" -> SDecimal(4.1399375473943306E-8), "Model1" -> SDecimal(0.0013585199504289591)))),
        (SObject(Map("Model2" -> SDecimal(5.109089028037222E-12), "Model1" -> SDecimal(3.7751345441365816E-11)))), 
        (SObject(Map("Model2" -> SDecimal(1.0261879630648827E-10), "Model1" -> SDecimal(6.305116760146985E-16)))),
        (SObject(Map("Model2" -> SDecimal(2.543665647376276E-13), "Model1" -> SDecimal(1.1548224173015786E-17)))), 
        (SObject(Map("Model2" -> SDecimal(0.11920292202211755), "Model1" -> SDecimal(0.5)))), 
        (SObject(Map("Model2" -> SDecimal(0.9999998874648379), "Model1" -> SDecimal(0.9999999847700205)))), 
        (SObject(Map("Model3" -> SDecimal(0.00007484622751061124)))),
        (SObject(Map("Model3" -> SDecimal(0.0009110511944006454)))),
        (SObject(Map("Model3" -> SDecimal(0.999983298578152)))),
        (SObject(Map("Model3" -> SDecimal(2.646573631904765E-9)))),
        (SObject(Map("Model3" -> SDecimal(0.6224593312018546)))),
        (SObject(Map("Model3" -> SDecimal(4.1399375473943306E-8)))),
        (SObject(Map("Model3" -> SDecimal(5.043474082014517E-7)))),
        (SObject(Map("Model3" -> SDecimal(0.6224593312018546)))),
        (SObject(Map("Model3" -> SDecimal(2.289734845593124E-11)))),
        (SObject(Map("Model3" -> SDecimal(0.6224593312018546)))))
    }

    "predict case with repeated model names and arrays" in {
      val line = Line(0, "")

      val input = dag.Morph2(line, LogisticPrediction,
        dag.LoadLocal(line, Const(line, CString("/hom/model2"))),
        dag.LoadLocal(line, Const(line, CString("/hom/model2data"))))

      val result0 = testEval(input)

      result0 must haveSize(14)

      val result = result0 collect { case (ids, value) if ids.size == 2 => value }

      result mustEqual Set(
        (SObject(Map("Model1" -> SDecimal(0.0003353501304664781), "Model3" -> SDecimal(1.522997951276035E-8)))),
        (SObject(Map("Model1" -> SDecimal(4.1399375473943306E-8)))),
        (SObject(Map("Model1" -> SDecimal(1.0261879630648827E-10)))),
        (SObject(Map("Model1" -> SDecimal(0.11920292202211755)))),
        (SObject(Map("Model2" -> SDecimal(8.315280276641321E-7), "Model1" -> SDecimal(0.00012339457598623172)))),
        (SObject(Map("Model1" -> SDecimal(1.522997951276035E-8)))), 
        (SObject(Map("Model1" -> SDecimal(3.7751345441365816E-11)))), 
        (SObject(Map("Model1" -> SDecimal(0.04742587317756678)))),
        (SObject(Map("Model3" -> SDecimal(0.5)))),
        (SObject(Map("Model3" -> SDecimal(0.000746028833836697)))), 
        (SObject(Map("Model3" -> SDecimal(0.9939401985084158)))), 
        (SObject(Map("Model2" -> SDecimal(2.319522830243569E-16)))),
        (SObject(Map("Model3" -> SDecimal(0.9820137900379085)))), 
        (SObject(Map("Model2" -> SDecimal(3.625140919143559E-34)))))
    }
  }
}

object PredictionLibSpecs extends PredictionLibSpecs[test.YId] with test.YIdInstances
