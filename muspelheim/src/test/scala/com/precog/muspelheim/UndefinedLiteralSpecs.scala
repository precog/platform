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

trait UndefinedLiteralSpecs extends EvalStackSpecs {
  import stack._
  "undefined literals be handled properly in" >> {
    "binary operation on load with undefined" >> {
      val input = """
          medals := //summer_games/london_medals
          medals.Total + undefined
        """

      val results = evalE(input)

      results must beEmpty
    }

    "multiple binary operations on loads with undefined" >> {
      val input = """
          medals := //summer_games/london_medals
          campaigns := //campaigns
          medals ~ campaigns
            medals.Total + campaigns.cmp + undefined
        """

      val results = evalE(input)

      results must beEmpty
    }

    // note that `5 intersect undefined` is provably empty
    // and thus kicked out by the compiler
    "union load with undefined" >> {
      val input = """
          clicks := //clicks
          clicks union undefined
        """

      val results = evalE(input)

      results must not(beEmpty)
    }

    "multiple union on loads with undefined" >> {
      val input = """
          clicks := //clicks
          views := //views
          clickViews := clicks union views

          clickViews union undefined
        """

      val results = evalE(input)

      results must not(beEmpty)
    }
  }
}

