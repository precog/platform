package com.precog
package muspelheim

import com.precog.yggdrasil._

trait LinearRegressionSpecs extends EvalStackSpecs {
  "linear regression" in {
    "return correctly structured results in a simple case of linear regression" in {
      val input = """
        medals := //summer_games/london_medals
        
        std::stats::linearRegression(medals.Weight, { height: medals.HeightIncm })
      """.stripMargin

      val results = evalE(input)

      results must haveSize(1)  

      results must haveAllElementsLike {
        case (ids, SObject(elems)) =>
          ids must haveSize(0)
          elems.keys mustEqual Set("Model1")

          val SObject(fields) = elems("Model1")

          val SArray(arr) = fields("Coefficients")
          val rSquared = fields("RSquared")

          arr(0) must beLike { case SObject(obj) => 
            obj.keys mustEqual Set("height")

            obj("height") must beLike {
              case SObject(height) => 
                height.keys mustEqual Set("Estimate", "StandardError")

                height("Estimate") must beLike { case SDecimal(d) => ok }
                height("StandardError") must beLike { case SDecimal(d) => ok }
            }
          }

          arr(1) must beLike { case SObject(obj) =>
            obj.keys mustEqual Set("Estimate", "StandardError")

            obj("Estimate") must beLike { case SDecimal(d) => ok }
            obj("StandardError") must beLike { case SDecimal(d) => ok }
          }

          rSquared must beLike { case SDecimal(_) => ok }
      }
    }

    "predict linear regression" in {
      val input = """
        medals := //summer_games/london_medals
        
        model := std::stats::linearRegression(medals.Weight, { height: medals.HeightIncm })
        std::stats::predictLinear({height: 34, other: 35}, model)
      """.stripMargin

      val results = evalE(input)

      results must haveSize(1)  

      results must haveAllElementsLike {
        case (ids, SObject(elems)) =>
          ids must haveSize(0)
          elems.keys mustEqual Set("Model1")

          elems("Model1") must beLike { case SDecimal(d) => ok }
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
            obj.keys mustEqual Set("Model1")
            obj("Model1") must beLike { case SDecimal(_) => ok }
          }
      }
    }

    "join predicted results with original dataset" in {
      val input = """
        medals := //summer_games/london_medals
        
        model := std::stats::linearRegression(medals.Weight, { HeightIncm: medals.HeightIncm })
        predictions := std::stats::predictLinear(medals, model)

        medals with { predictedWeight: predictions }
      """

      val input2 = """ 
        medals := //summer_games/london_medals

        h := medals where std::type::isNumber(medals.HeightIncm)
        count(h)
      """

      testJoinLinear(input, input2, false)
    }

    "join predicted results with original dataset when model is `new`ed" in {
      val input = """
        medals := //summer_games/london_medals
        
        model := new std::stats::linearRegression(medals.Weight, { HeightIncm: medals.HeightIncm })

        model ~ medals
        predictions := std::stats::predictLinear(medals, model)

        medals with { predictedWeight: predictions }
      """

      val input2 = """ 
        medals := //summer_games/london_medals

        h := medals where std::type::isNumber(medals.HeightIncm)
        count(h)
      """

      testJoinLinear(input, input2, true)
    }

    "join predicted results with model when model is `new`ed" in {
      val input = """
        medals := //summer_games/london_medals
        
        model := new std::stats::linearRegression(medals.Weight, { HeightIncm: medals.HeightIncm })

        model ~ medals
        predictions := std::stats::predictLinear(medals, model)

        model with { predictedWeight: predictions }
      """

      val input2 = """ 
        medals := //summer_games/london_medals

        h := medals where std::type::isNumber(medals.HeightIncm)
        count(h)
      """

      val results = evalE(input)
      val resultsCount = evalE(input2)

      val count = resultsCount.collectFirst { case (_, SDecimal(d)) => d.toInt }.get
      results must haveSize(count)

      results must haveAllElementsLike {
        case (ids, SObject(elems)) =>
          ids must haveSize(2)
          elems.keys mustEqual Set("predictedWeight", "Model1")

          elems("predictedWeight") must beLike { case SObject(obj) =>
            obj.keys mustEqual Set("Model1")
            obj("Model1") must beLike { case SDecimal(_) => ok }
          }
      }
    }

    "predict linear regression when no field names in model are present in data" in {
      val input = """
        medals := //summer_games/london_medals
        
        model := std::stats::linearRegression(medals.Weight, { height: medals.HeightIncm })
        std::stats::predictLinear({weight: 34, other: 35}, model)
      """.stripMargin

      val results = evalE(input)

      results must haveSize(0)
    }

    "return correct number of results in more complex case of linear regression" in {
      val input = """
          medals := //summer_games/london_medals
          
          std::stats::linearRegression(medals.S, medals)
        """.stripMargin

      val results = evalE(input)

      results must haveSize(1)

      results must haveAllElementsLike {
        case (ids, SObject(elems)) =>
          ids must haveSize(0)
          elems.keys mustEqual Set("Model1", "Model2", "Model3", "Model4")
      }
    }

    "return empty set when fed rank deficient data" in {
      val input = """
        std::stats::linearRegression(0, 4)
      """.stripMargin

      evalE(input) must throwA[IllegalArgumentException]
    }

    "return empty set when the dependent variable is not at the root path" in {
      val input = """
        medals := //summer_games/london_medals
        
        std::stats::linearRegression({weight: medals.Weight}, {height: medals.HeightIncm})
      """.stripMargin

      evalE(input) must beEmpty
    }

    "return empty set when given feature values of wrong type" in {
      val input = """
        medals := //summer_games/london_medals
        
        std::stats::linearRegression(medals.WeightIncm, medals.Country)
      """.stripMargin

      evalE(input) must beEmpty
    }

    "return empty set when given dependent values of wrong type" in {
      val input = """
        medals := //summer_games/london_medals
        
        std::stats::linearRegression(medals.Country, medals.WeightIncm)
      """.stripMargin

      evalE(input) must beEmpty
    }
  }
}
