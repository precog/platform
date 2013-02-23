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

          val SArray(arr) = elems("Model1")

          arr(0) must beLike { case SObject(obj) => 
            obj.keys mustEqual Set("height")

            obj("height") must beLike {
              case SObject(height) => 
                height.keys mustEqual Set("coefficient", "standard error")

                height("coefficient") must beLike { case SDecimal(d) => ok }
                height("standard error") must beLike { case SDecimal(d) => ok }
            }
          }

          arr(1) must beLike { case SObject(obj) =>
            obj.keys mustEqual Set("coefficient", "standard error")

            obj("coefficient") must beLike { case SDecimal(d) => ok }
            obj("standard error") must beLike { case SDecimal(d) => ok }
          }
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

    "join predicted results with original dataset" in {
      val input = """
        medals := //summer_games/london_medals
        
        model := std::stats::linearRegression(medals.Weight, { HeightIncm: medals.HeightIncm })
        predictions := std::stats::predictLinear(medals, model)

        { height: medals.HeightIncm, predictedWeight: predictions }
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
          ids must haveSize(1)
          elems.keys must contain("predictedWeight")

          elems("predictedWeight") must beLike { case SObject(obj) =>
            obj.keys mustEqual Set("Model1")
            obj("Model1") must beLike { case SDecimal(d) => ok }
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
