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
package pandora

import com.precog.muspelheim.EvalStackSpecs
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

      val results = evalE(input)

      results must haveSize(0)
    }

    "return empty set when none of the classification values are 0 or 1" >> {
      val input = """
               medals := //summer_games/london_medals
               medals' := medals with { gender: (1 where medals.Sex = "F") union (0 where medals.Sex = "M") }
               
               std::stats::logisticRegression({height: medals'.HeightIncm}, 5)
               """.stripMargin

      val results = evalE(input)

      results must haveSize(0)
    }

    "return empty set when given feature values of wrong type" in {
      val input = """
          medals := //summer_games/london_medals
          
          std::stats::logisticRegression(medals.Country, medals.WeightIncm)
        """.stripMargin

      val results = evalE(input)

      results must haveSize(0)
    }

    "return empty set when given classication values of wrong type" in {
      val input = """
          medals := //summer_games/london_medals
          
          std::stats::logisticRegression(medals.WeightIncm, medals.Country)
        """.stripMargin

      val results = evalE(input)

      results must haveSize(0)
    }
  }

}

class JDBMLogisticRegressionSpecs extends LogisticRegressionSpecs with JDBMPlatformSpecs
