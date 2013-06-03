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

