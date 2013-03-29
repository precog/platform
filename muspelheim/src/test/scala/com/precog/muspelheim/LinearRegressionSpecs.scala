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
  def handleCoeffs(obj: Map[String, SValue]) = {
    obj.keys mustEqual Set("estimate", "standardError")

    obj("estimate") must beLike { case SDecimal(_) => ok }
    obj("standardError") must beLike { case SDecimal(_) => ok }
  }

  def handleNull(obj: Map[String, SValue]) = {
    obj.keys mustEqual Set("estimate", "standardError")

    obj("estimate") mustEqual SNull
    obj("standardError") mustEqual SNull
  }

  "linear regression" should {
    "handle a constant x-values" in {
      val input = """
        | medals := //summer_games/london_medals
        | medals' := medals with { const: 3 }
        | 
        | std::stats::linearRegression(medals'.Weight, medals'.const)
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

          arr(0) must beLike { case SObject(obj) => handleNull(obj) }

          arr(1) must beLike { case SObject(obj) => handleCoeffs(obj) }

          rSquared must beLike { case SDecimal(_) => ok }
        }
      }
    }

    "handle multiple indep vars, one of which is a constant" in {
      val input = """
        | medals := //summer_games/london_medals
        | medals' := medals where std::type::isNumber(medals.HeightIncm) & std::type::isNumber(medals.Weight)
        | medals'' := medals' with { const: 3 }
        |
        | data := { const: medals''.const, height: medals''.HeightIncm }
        | 
        | std::stats::linearRegression(medals''.Weight, data)
      """.stripMargin

      val input2 = """
        | medals := //summer_games/london_medals
        | medals' := medals where std::type::isNumber(medals.HeightIncm) & std::type::isNumber(medals.Weight)
        |
        | data := { height: medals'.HeightIncm }
        | 
        | std::stats::linearRegression(medals'.Weight, data)
      """.stripMargin

      val result = evalE(input)
      val result2 = evalE(input2)

      result must haveSize(1)
      result2 must haveSize(1)

      val res1Values = new Array[BigDecimal](5)
      val res2Values = new Array[BigDecimal](5)

      result must haveAllElementsLike {
        case (ids, SObject(elems)) => {
          ids must haveSize(0)
          elems.keys mustEqual Set("model1")

          val SObject(fields) = elems("model1")

          val SArray(arr) = fields("coefficients")
          val rSquared = fields("RSquared")

          arr(0) must beLike { case SObject(obj) => 
            obj.keys mustEqual Set("height", "const")

            obj("height") must beLike {
              case SObject(height) => 
                height.keys mustEqual Set("estimate", "standardError")

                height("estimate") must beLike { case SDecimal(d) => res1Values(0) = d; ok }
                height("standardError") must beLike { case SDecimal(d) => res1Values(1) = d; ok }
            }
            obj("const") must beLike {
              case SObject(const) => 
                const.keys mustEqual Set("estimate", "standardError")

                const("estimate") mustEqual SNull
                const("standardError") mustEqual SNull
            }
          }

          arr(1) must beLike { case SObject(obj) =>
            obj.keys mustEqual Set("estimate", "standardError")

            obj("estimate") must beLike { case SDecimal(d) => res1Values(2) = d; ok }
            obj("standardError") must beLike { case SDecimal(d) => res1Values(3) = d; ok }
          }

          rSquared must beLike { case SDecimal(d) => res1Values(4) = d; ok }
        }
      }

      result2 must haveAllElementsLike {
        case (ids, SObject(elems)) => {
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

                height("estimate") must beLike { case SDecimal(d) => res2Values(0) = d; ok }
                height("standardError") must beLike { case SDecimal(d) => res2Values(1) = d; ok }
            }
          }

          arr(1) must beLike { case SObject(obj) =>
            obj.keys mustEqual Set("estimate", "standardError")

            obj("estimate") must beLike { case SDecimal(d) => res2Values(2) = d; ok }
            obj("standardError") must beLike { case SDecimal(d) => res2Values(3) = d; ok }
          }

          rSquared must beLike { case SDecimal(d) => res2Values(4) = d; ok }
        }
      }

      res1Values mustEqual res2Values
    }

    "predict when nulls are present in the regression model" in {
      val input = """
        | medals := //summer_games/london_medals
        | medals' := medals where std::type::isNumber(medals.HeightIncm) & std::type::isNumber(medals.Weight)
        | medals'' := medals' with { const: 3 }
        |
        | data := { const: medals''.const, height: medals''.HeightIncm }
        | model := std::stats::linearRegression(medals''.Weight, data)
        | input := { height: medals.HeightIncm + 4 }
        |
        | std::stats::predictLinear(input, model)
      """.stripMargin

      val input2 = """ 
        | medals := //summer_games/london_medals
        |
        | h := medals where std::type::isNumber(medals.HeightIncm)
        | count(h)
      """.stripMargin

      val result = evalE(input)
      val resultCount = evalE(input2)

      val count = resultCount.collectFirst { case (_, SDecimal(d)) => d.toInt }.get
      result must haveSize(count)

      result must haveAllElementsLike { case (ids, SObject(elems)) =>
        ids must haveSize(1)
        elems.keys mustEqual Set("model1")

        elems("model1") must beLike { case SDecimal(_) => ok }
      }
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

    "accept linearly dependent variables" in {
      val input = """
        | clicks := //clicks2
        |
        | vars := {
        |   rate: clicks.marketing.bounceRate,
        |   rate2: (clicks.marketing.bounceRate * 2) + 1
        | }
        | 
        | std::stats::linearRegression(clicks.product.price, vars)
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

          arr(0) must beLike { case SObject(obj) => 
            obj.keys mustEqual Set("rate", "rate2")

            obj("rate2") must beLike {
              case SObject(height) =>  handleCoeffs(height)
            }
            obj("rate") must beLike {
              case SObject(const) => handleNull(const)
            }
          }

          arr(1) must beLike { case SObject(obj) => handleCoeffs(obj) }

          rSquared must beLike { case SDecimal(_) => ok }
        }
      }
    }

    "accept linearly dependent variables in case with three columns" in {
      val input = """
        | clicks := //clicks2
        |
        | vars := {
        |   rate: clicks.marketing.bounceRate,
        |   rateprice: (clicks.marketing.bounceRate * 2) + (clicks.product.price * 3) + 5,
        |   price: clicks.product.price + 8
        | }
        | 
        | std::stats::linearRegression(clicks.product.price, vars)
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

          arr(0) must beLike { case SObject(obj) => 
            obj.keys mustEqual Set("rate", "rateprice", "price")

            obj("rate") must beLike {
              case SObject(rate) => handleCoeffs(rate)
            }
            obj("rateprice") must beLike {
              case SObject(rateprice) => handleCoeffs(rateprice)
            }
            obj("price") must beLike {
              case SObject(price) => handleNull(price)
            }
          }

          arr(1) must beLike { case SObject(obj) => handleCoeffs(obj) }

          rSquared must beLike { case SDecimal(_) => ok }
        }
      }
    }

    "accept linearly dependent variables in case with two dependent columns" in {
      val input = """
        | clicks := //clicks2
        |
        | vars := {
        |   rate: clicks.marketing.bounceRate,
        |   rateprice: (clicks.marketing.bounceRate * 2) + (clicks.product.price * 3) + 5,
        |   price: clicks.product.price + 8,
        |   zzz: clicks.product.price + 9
        | }
        | 
        | std::stats::linearRegression(clicks.product.price, vars)
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

          arr(0) must beLike { case SObject(obj) => 
            obj.keys mustEqual Set("rate", "rateprice", "price", "zzz")

            obj("price") must beLike {
              case SObject(price) => handleNull(price)
            }
            obj("rateprice") must beLike {
              case SObject(rateprice) => handleNull(rateprice)
            }
            obj("zzz") must beLike {
              case SObject(zzz) => handleCoeffs(zzz)
            }
            obj("rate") must beLike {
              case SObject(rate) => handleCoeffs(rate)
            }
          }

          arr(1) must beLike { case SObject(obj) => handleCoeffs(obj) }

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
              case SObject(height) => handleCoeffs(height)
            }
          }

          arr(1) must beLike { case SObject(obj) => handleCoeffs(obj) }

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

          elems("model1") must beLike { case SDecimal(_) => ok }
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
