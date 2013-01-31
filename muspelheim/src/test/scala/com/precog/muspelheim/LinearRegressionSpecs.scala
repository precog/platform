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
  "linear regression" >> {
    "return correctly structured results in a simple case of linear regression" >> {
      val input = """
        medals := //summer_games/london_medals
        
        std::stats::linearRegression({ height: medals.HeightIncm }, medals.Weight)
      """.stripMargin

      val results = evalE(input)

      results must haveSize(1)  

      forall(results) {
        case (ids, SObject(elems)) =>
          ids must haveSize(0)
          elems.keys mustEqual Set("Model1")

          val SArray(arr1) = elems("Model1")

          arr1(0) must beLike { case SObject(elems) => elems("height") match { case SDecimal(d) => elems must haveSize(1) } }
          arr1(1) must beLike { case SDecimal(d) => ok }
      }
    }    
    
    "predict linear regression" >> {
      val input = """
        medals := //summer_games/london_medals
        
        model := std::stats::linearRegression({ height: medals.HeightIncm }, medals.Weight)
        std::stats::predictLinear(model, {height: 34, other: 35})
      """.stripMargin

      val results = evalE(input)

      results must haveSize(1)  

      forall(results) {
        case (ids, SObject(elems)) =>
          ids must haveSize(0)
          elems.keys mustEqual Set("Model1")

          elems("Model1") must beLike { case SDecimal(d) => ok }
      }
    }

    "return correct number of results in more complex case of linear regression" >> {
      val input = """
          medals := //summer_games/london_medals
          
          std::stats::linearRegression(medals, medals.S)
        """.stripMargin

      val results = evalE(input)

      results must haveSize(1)

      forall(results) {
        case (ids, SObject(elems)) =>
          ids must haveSize(0)
          elems.keys mustEqual Set("Model1", "Model2", "Model3", "Model4")
      }
    }

    "return empty set when fed rank deficient data" >> {
      val input = """
        std::stats::linearRegression(4, 0)
      """.stripMargin

      evalE(input) must throwA[IllegalArgumentException]
    }

    "return empty set when the dependent variable is not at the root path" >> {
      val input = """
        medals := //summer_games/london_medals
        
        std::stats::linearRegression({height: medals.HeightIncm}, {weight: medals.Weight})
      """.stripMargin

      evalE(input) must beEmpty
    }

    "return empty set when given feature values of wrong type" in {
      val input = """
        medals := //summer_games/london_medals
        
        std::stats::linearRegression(medals.Country, medals.WeightIncm)
      """.stripMargin

      evalE(input) must beEmpty
    }

    "return empty set when given dependent values of wrong type" in {
      val input = """
        medals := //summer_games/london_medals
        
        std::stats::linearRegression(medals.WeightIncm, medals.Country)
      """.stripMargin

      evalE(input) must beEmpty
    }
  }
}
