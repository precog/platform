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
package com.precog
package muspelheim

import com.precog.yggdrasil._

trait LinearRegressionSpecs extends EvalStackSpecs {
  "linear regression" should {
    "throw an exception with constant x-values" in {
      val input = """
        | medals := //summer_games/london_medals
        | medals' := medals with { const: 3 }
        | 
        | std::stats::linearRegression(medals'.Weight, medals'.const)
      """.stripMargin

      evalE(input) must throwA[IllegalArgumentException]
    }

    "throw an exception with constant x-values and more than column" in {
      val input = """
        | medals := //summer_games/london_medals
        | medals' := medals with { const: 3 }
        |
        | data := { const: medals'.const, height: medals'.HeightIncm }
        | 
        | std::stats::linearRegression(medals'.Weight, data)
      """.stripMargin

      evalE(input) must throwA[IllegalArgumentException]
    }

    "accept a constant dependent variable" in {
      val input = """
        | medals := //summer_games/london_medals
        | medals' := medals with { const: 3 }
        | 
        | std::stats::linearRegression(medals'.const, medals'.Weight)
      """.stripMargin

      val result = evalE(input)
      result must haveSize(1)  

      result must haveAllElementsLike {
        case (ids, SObject(elems)) => {
          ids must haveSize(0)
          elems.keys mustEqual Set("model1")

          val SObject(fields) = elems("model1")

          val SArray(arr) = fields("coefficients")
          val rSquared = fields("RSquared")

          rSquared must beLike { case SDecimal(_) => ok }
        }
      }
    }

    "accept a previous problematic case regarding slice boundaries" in {
      val input = """
        | conversions := //conversions
        | conversions' := conversions with
        | 
        |  {
        |    female : if conversions.customer.gender = "female" then 1 else 0
        |  }
        |
        | independentVars := {
        |   income: conversions'.customer.income, 
        |   age: conversions'.customer.age,
        |   female: conversions'.female
        |   }
        | 
        | std::stats::linearRegression(conversions'.product.price, independentVars)
      """.stripMargin

      val result = evalE(input)
      result must haveSize(1)  

      result must haveAllElementsLike {
        case (ids, SObject(elems)) => {
          ids must haveSize(0)
          elems.keys mustEqual Set("model1")

          val SObject(fields) = elems("model1")

          val SArray(arr) = fields("coefficients")
          val rSquared = fields("RSquared")

          rSquared must beLike { case SDecimal(_) => ok }
        }
      }
    }

    "linearly dependent variables" in {
      val input = """
        | clicks := //clicks2
        |
        | vars := {
        |   rate: clicks.marketing.bounceRate,
        |   rate2: clicks.marketing.bounceRate * 2
        |   }
        | 
        | std::stats::linearRegression(clicks.product.price, vars)
      """.stripMargin

      evalE(input) must throwA[IllegalArgumentException]
    }

    "accept case with problematic R^2 value" in {
      val input = """
        | conversions := //conversions
        | conversions' := conversions with
        |   {
        |     female : if conversions.customer.gender = "female" then 1 else 0
        |   }
        | 
        | std::stats::linearRegression(conversions'.female, conversions'.product.price)
      """.stripMargin

      val result = evalE(input)
      result must haveSize(1)  

      result must haveAllElementsLike {
        case (ids, SObject(elems)) => {
          ids must haveSize(0)
          elems.keys mustEqual Set("model1")

          val SObject(fields) = elems("model1")

          val SArray(arr) = fields("coefficients")
          val rSquared = fields("RSquared")

          rSquared must beLike { case SDecimal(_) => ok }
        }
      }
    }

    "return correctly structured results in a simple case of linear regression" in {
      val input = """
        | medals := //summer_games/london_medals
        | 
        | std::stats::linearRegression(medals.Weight, { height: medals.HeightIncm })
      """.stripMargin

      val results = evalE(input)

      results must haveSize(1)  

      results must haveAllElementsLike {
        case (ids, SObject(elems)) =>
          ids must haveSize(0)
          elems.keys mustEqual Set("model1")

          val SObject(fields) = elems("model1")

          val SArray(arr) = fields("coefficients")
          val rSquared = fields("RSquared")

          arr(0) must beLike { case SObject(obj) => 
            obj.keys mustEqual Set("height")

            obj("height") must beLike {
              case SObject(height) => 
                height.keys mustEqual Set("estimate", "standardError")

                height("estimate") must beLike { case SDecimal(d) => ok }
                height("standardError") must beLike { case SDecimal(d) => ok }
            }
          }

          arr(1) must beLike { case SObject(obj) =>
            obj.keys mustEqual Set("estimate", "standardError")

            obj("estimate") must beLike { case SDecimal(d) => ok }
            obj("standardError") must beLike { case SDecimal(d) => ok }
          }

          rSquared must beLike { case SDecimal(_) => ok }
      }
    }

    "predict linear regression" in {
      val input = """
        | medals := //summer_games/london_medals
        | 
        | model := std::stats::linearRegression(medals.Weight, { height: medals.HeightIncm })
        | std::stats::predictLinear({height: 34, other: 35}, model)
      """.stripMargin

      val results = evalE(input)

      results must haveSize(1)  

      results must haveAllElementsLike {
        case (ids, SObject(elems)) =>
          ids must haveSize(0)
          elems.keys mustEqual Set("model1")

          elems("model1") must beLike { case SDecimal(d) => ok }
      }
    }

    def testJoinLinear(input: String, input2: String, idJoin: Boolean) = {
      val results = evalE(input)
      val resultsCount = evalE(input2)

      val count = resultsCount.collectFirst { case (_, SDecimal(d)) => d.toInt }.get
      results must haveSize(count)

      results must haveAllElementsLike {
        case (ids, SObject(elems)) =>
          if (idJoin) ids must haveSize(2)
          else ids must haveSize(1)

          elems.keys must contain("predictedWeight")

          elems("predictedWeight") must beLike { case SObject(obj) =>
            obj.keys mustEqual Set("model1")
            obj("model1") must beLike { case SDecimal(_) => ok }
          }
      }
    }

    "join predicted results with original dataset" in {
      val input = """
        | medals := //summer_games/london_medals
        | 
        | model := std::stats::linearRegression(medals.Weight, { HeightIncm: medals.HeightIncm })
        | predictions := std::stats::predictLinear(medals, model)
        |
        | medals with { predictedWeight: predictions }
      """.stripMargin

      val input2 = """ 
        | medals := //summer_games/london_medals
        |
        | h := medals where std::type::isNumber(medals.HeightIncm)
        | count(h)
      """.stripMargin

      testJoinLinear(input, input2, false)
    }

    "join predicted results with original dataset when model is `new`ed" in {
      val input = """
        | medals := //summer_games/london_medals
        | 
        | model := new std::stats::linearRegression(medals.Weight, { HeightIncm: medals.HeightIncm })
        |
        | model ~ medals
        | predictions := std::stats::predictLinear(medals, model)
        |
        | medals with { predictedWeight: predictions }
      """.stripMargin

      val input2 = """ 
        | medals := //summer_games/london_medals
        |
        | h := medals where std::type::isNumber(medals.HeightIncm)
        | count(h)
      """.stripMargin

      testJoinLinear(input, input2, true)
    }

    "join predicted results with model when model is `new`ed" in {
      val input = """
        | medals := //summer_games/london_medals
        | 
        | model := new std::stats::linearRegression(medals.Weight, { HeightIncm: medals.HeightIncm })
        |
        | model ~ medals
        | predictions := std::stats::predictLinear(medals, model)
        |
        | model with { predictedWeight: predictions }
      """.stripMargin

      val input2 = """ 
        | medals := //summer_games/london_medals
        |
        | h := medals where std::type::isNumber(medals.HeightIncm)
        | count(h)
      """.stripMargin

      val results = evalE(input)
      val resultsCount = evalE(input2)

      val count = resultsCount.collectFirst { case (_, SDecimal(d)) => d.toInt }.get
      results must haveSize(count)

      results must haveAllElementsLike {
        case (ids, SObject(elems)) =>
          ids must haveSize(2)
          elems.keys mustEqual Set("predictedWeight", "model1")

          elems("predictedWeight") must beLike { case SObject(obj) =>
            obj.keys mustEqual Set("model1")
            obj("model1") must beLike { case SDecimal(_) => ok }
          }
      }
    }

    "predict linear regression when no field names in model are present in data" in {
      val input = """
        | medals := //summer_games/london_medals
        | 
        | model := std::stats::linearRegression(medals.Weight, { height: medals.HeightIncm })
        | std::stats::predictLinear({weight: 34, other: 35}, model)
      """.stripMargin

      val results = evalE(input)

      results must haveSize(0)
    }

    "return correct number of results in more complex case of linear regression" in {
      val input = """
        | medals := //summer_games/london_medals
        | 
        | std::stats::linearRegression(medals.S, medals)
      """.stripMargin

      val results = evalE(input)

      results must haveSize(1)

      results must haveAllElementsLike {
        case (ids, SObject(elems)) =>
          ids must haveSize(0)
          elems.keys mustEqual Set("model1", "model2", "model3", "model4")
      }
    }

    "throw exception when fed rank deficient data" in {
      val input = """
        std::stats::linearRegression(0, 4)
      """

      evalE(input) must throwA[IllegalArgumentException]
    }

    "return empty set when the dependent variable is not at the root path" in {
      val input = """
        | medals := //summer_games/london_medals
        | 
        | std::stats::linearRegression({weight: medals.Weight}, {height: medals.HeightIncm})
      """.stripMargin

      evalE(input) must beEmpty
    }

    "return empty set when given feature values of wrong type" in {
      val input = """
        | medals := //summer_games/london_medals
        | 
        | std::stats::linearRegression(medals.WeightIncm, medals.Country)
      """.stripMargin

      evalE(input) must beEmpty
    }

    "return empty set when given dependent values of wrong type" in {
      val input = """
        | medals := //summer_games/london_medals
        | 
        | std::stats::linearRegression(medals.Country, medals.WeightIncm)
      """.stripMargin

      evalE(input) must beEmpty
    }
  }
}
