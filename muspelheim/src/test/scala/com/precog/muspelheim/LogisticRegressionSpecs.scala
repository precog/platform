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

trait LogisticRegressionSpecs extends EvalStackSpecs {
  "logistic regression" should {
    "return correctly structured results in simple case of logistic regression" in {
      val input = """
          medals := //summer_games/london_medals
          gender := (1 where medals.Sex = "F") union (0 where medals.Sex = "M")
          
          std::stats::logisticRegression({ height: medals.HeightIncm }, gender)
        """.stripMargin

      val results = evalE(input)

      results must haveSize(1)

      results must haveAllElementsLike {
        case (ids, SObject(elems)) =>
          ids must haveSize(0)
          elems.keys mustEqual Set("Model1")
          
          val SArray(arr1) = elems("Model1")

          arr1(0) must beLike { case SObject(elems) =>
            elems("height") must beLike {
              case SDecimal(d) => elems must haveSize(1)
            }
          }
          arr1(1) must beLike { case SDecimal(d) => ok }
      }
    }

    "predict logistic regression" in {
      val input = """
        medals := //summer_games/london_medals
        
        gender := (1 where medals.Sex = "F") union (0 where medals.Sex = "M")
        model := std::stats::logisticRegression({ height: medals.HeightIncm }, gender)

        std::stats::predictLogistic({height: 34, other: 35}, model)
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
        
        gender := (1 where medals.Sex = "F") union (0 where medals.Sex = "M")
        model := std::stats::logisticRegression({ HeightIncm: medals.HeightIncm }, gender)

        predictions := std::stats::predictLogistic(medals, model)

        { height: medals.HeightIncm, predictedGender: predictions }
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
          elems.keys mustEqual Set("height", "predictedGender") 

          elems("predictedGender") must beLike { case SObject(obj) =>
            obj.keys mustEqual Set("Model1")
            obj("Model1") must beLike { case SDecimal(d) =>
              (d must be_>=(BigDecimal(0))) and (d must be_<=(BigDecimal(1)))
            }
          }
      }
    }

    "predict logistic regression when no field names in model are present in data" in {
      val input = """
        medals := //summer_games/london_medals
        
        gender := (1 where medals.Sex = "F") union (0 where medals.Sex = "M")
        model := std::stats::logisticRegression({ height: medals.HeightIncm }, gender)

        std::stats::predictLogistic({baz: 34, bar: 35}, model)
      """.stripMargin

      val results = evalE(input)

      results must haveSize(0)
    }

    "return correct number of results in more complex case of logistic regression" in {
      val input = """
          medals := //summer_games/london_medals
          
          std::stats::logisticRegression(medals, medals.S)
        """.stripMargin

      val results = evalE(input)

      results must haveSize(1)

      results must haveAllElementsLike {
        case (ids, SObject(elems)) =>
          ids must haveSize(0)
          elems.keys mustEqual Set("Model1", "Model2", "Model3", "Model4")
      }
    }

    "return something when fed constants" in {
      val input = """
          std::stats::logisticRegression(4, 0)
        """.stripMargin

      val results = evalE(input)

      results must haveSize(1)
    }

    "return empty set when the classification variable is not at the root path" in {
      val input = """
               medals := //summer_games/london_medals
               medals' := medals with { gender: (1 where medals.Sex = "F") union (0 where medals.Sex = "M") }
               
               std::stats::logisticRegression({height: medals'.HeightIncm}, {gender: medals'.gender})
               """.stripMargin

      evalE(input) must beEmpty
    }

    "return empty set when none of the classification values are 0 or 1" in {
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
