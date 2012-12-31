package com.precog
package muspelheim

import com.precog.yggdrasil._

trait LinearRegressionSpecs extends EvalStackSpecs {
  "linear regression" >> {
    "return correctly structured results in a simple case of linear regression" >> {
      val input = """
        medals := //summer_games/london_medals
        
        std::stats::linearRegression({ height: medals.HeightIncm }, medals.Weight)
      """.stripMargin

      val results = evalE(input)

      results must haveSize(1)  

      forall(results) {
        case (ids, SArray(elems)) =>
          ids must haveSize(0)
          elems must haveSize(2)
          elems(0) must beLike { case SObject(elems) => elems("height") match { case SDecimal(d) => elems must haveSize(1) } }
          elems(1) must beLike { case SDecimal(d) => ok }
      }
    }

    "return empty set when fed rank deficient data" >> {
      val input = """
        std::stats::linearRegression(4, 0)
      """.stripMargin

      val results = evalE(input)

      results must haveSize(0)  
    }

    "return empty set when the dependent variable is not at the root path" >> {
      val input = """
        medals := //summer_games/london_medals
        
        std::stats::linearRegression({height: medals.HeightIncm}, {weight: medals.Weight})
      """.stripMargin

      val results = evalE(input)

      results must haveSize(0)  
    }

    "return empty set when given feature values of wrong type" in {
      val input = """
        medals := //summer_games/london_medals
        
        std::stats::linearRegression(medals.Country, medals.WeightIncm)
      """.stripMargin

      val results = evalE(input)

      results must haveSize(0)  
    }

    "return empty set when given dependent values of wrong type" in {
      val input = """
        medals := //summer_games/london_medals
        
        std::stats::linearRegression(medals.WeightIncm, medals.Country)
      """.stripMargin

      val results = evalE(input)

      results must haveSize(0)  
    }
  }
}
