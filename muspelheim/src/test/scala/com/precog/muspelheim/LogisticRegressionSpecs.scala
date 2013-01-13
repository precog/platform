package com.precog
package muspelheim

import com.precog.yggdrasil._

trait LogisticRegressionSpecs extends EvalStackSpecs {
  "logistic regression" should {
    "return correctly structured results in simple case of logistic regression" >> {
      val input = """
          medals := //summer_games/london_medals
          gender := (1 where medals.Sex = "F") union (0 where medals.Sex = "M")
          
          std::stats::logisticRegression({ height: medals.HeightIncm }, gender)
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

    "return correct number of results in more complex case of logistic regression" >> {
      val input = """
          medals := //summer_games/london_medals
          
          std::stats::logisticRegression(medals, medals.S)
        """.stripMargin

      evalE(input) must haveSize(4)
    }

    "return something when fed constants" >> {
      val input = """
          std::stats::logisticRegression(4, 0)
        """.stripMargin

      val results = evalE(input)

      results must haveSize(1)
    }

    "return empty set when the classification variable is not at the root path" >> {
      val input = """
               medals := //summer_games/london_medals
               medals' := medals with { gender: (1 where medals.Sex = "F") union (0 where medals.Sex = "M") }
               
               std::stats::logisticRegression({height: medals'.HeightIncm}, {gender: medals'.gender})
               """.stripMargin

      evalE(input) must beEmpty
    }

    "return empty set when none of the classification values are 0 or 1" >> {
      val input = """
               medals := //summer_games/london_medals
               medals' := medals with { gender: (1 where medals.Sex = "F") union (0 where medals.Sex = "M") }
               
               std::stats::logisticRegression({height: medals'.HeightIncm}, 5)
               """.stripMargin

      evalE(input) must beEmpty
    }

    "return empty set when given feature values of wrong type" in {
      val input = """
          medals := //summer_games/london_medals
          
          std::stats::logisticRegression(medals.Country, medals.WeightIncm)
        """.stripMargin

      evalE(input) must beEmpty
    }

    "return empty set when given classication values of wrong type" in {
      val input = """
          medals := //summer_games/london_medals
          
          std::stats::logisticRegression(medals.WeightIncm, medals.Country)
        """.stripMargin

      evalE(input) must beEmpty
    }
  }
}
