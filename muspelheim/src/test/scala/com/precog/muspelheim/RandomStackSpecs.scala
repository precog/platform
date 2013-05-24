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

trait RandomStackSpecs extends EvalStackSpecs {
  import stack._
  "random functionality" should {
    "guarantee observe of uniform returns values between 0 and 1" in {
      val input = """
        | clicks := //clicks
        |
        | uniform := std::random::uniform(12)
        |
        | observe(clicks, uniform)
        | """.stripMargin

      val result = evalE(input)

      val clicks = """//clicks"""
      val resultClicks = evalE(clicks)

      result must haveSize(resultClicks.size)

      result must haveAllElementsLike {
        case (ids, SDecimal(d)) => 
          ids must haveSize(1)

          d must be_>=(BigDecimal(0))
          d must be_<(BigDecimal(1))
      }
    }

    "give error if distribution is returned unobserved" in {
      val input = """std::random::uniform(12)"""

      evalE(input) must throwAn[Exception]
    }

    "guarantee observe of uniform joins with original dataset" in {
      val input = """
        | clicks := //clicks
        |
        | uniform := std::random::uniform(12)
        | observations := observe(clicks, uniform)
        |
        | {pageId: clicks.pageId, rand: observations}
        | """.stripMargin

      val result = evalE(input)

      val clicks = """(//clicks).pageId"""
      val resultClicks = evalE(clicks)

      result must haveSize(resultClicks.size)

      val pageIds: Set[SValue] = (0 to 4).map { i => SString("page-" + i.toString) }.toSet

      result must haveAllElementsLike {
        case (ids, SObject(fields)) => 
          ids must haveSize(1)
          fields.keys mustEqual Set("pageId", "rand")

          pageIds must contain(fields("pageId"))

          fields("rand") must beLike {
            case SDecimal(d) => 
              d must be_>=(BigDecimal(0))
              d must be_<(BigDecimal(1))
          }
      }
    }

    "accept a query like Nathan's" in {
      val input = """
        | clicks := //clicks
        |
        | model1 := new {name: "foo", range: [0, 0.3]}
        | model2 := new {name: "bar", range: [0.3, 1.0]}
        | model := model1 union model2
        |
        | uniform := std::random::uniform(12)
        | obs := observe(clicks, uniform)
        |
        | clicks ~ model
        |   low := obs >= model.range[0] 
        |   high := obs < model.range[1]
        |  
        |   clicks with { predict: model.name where low & high }
        | """.stripMargin

      val result = evalE(input)

      val clicks = """(//clicks).pageId"""
      val resultClicks = evalE(clicks)

      result must haveSize(resultClicks.size)

      result must haveAllElementsLike {
        case (ids, SObject(fields)) => 
          ids must haveSize(2)

          fields.keys must contain("predict")

          fields("predict") must beLike {
            case SString(str) => 
              Set("foo", "bar") must contain(str)
          }
      }
    }

    "work" in {
      val input = """
        | clicks := //clicks
        |
        | uniform := std::random::uniform(12)
        | observe(clicks, uniform < 0.5)
        | """.stripMargin

      val result = evalE(input)

      val clicks = """//clicks"""
      val resultClicks = evalE(clicks)

      result must haveSize(resultClicks.size)

      result must haveAllElementsLike {
        case (ids, SDecimal(d)) => 
          ids must haveSize(1)

          d must be_>=(BigDecimal(0))
          d must be_<(BigDecimal(0.5))
      }
    }.pendingUntilFixed

    "work" in {
      val input = """
        | clicks := //clicks
        |
        | uniform := std::random::uniform(12)
        | observe(clicks, std::math::floor(uniform))
        | """.stripMargin

      val result = evalE(input)

      val clicks = """//clicks"""
      val resultClicks = evalE(clicks)

      result must haveSize(resultClicks.size)

      result must haveAllElementsLike {
        case (ids, SDecimal(d)) => 
          ids must haveSize(1)
          d mustEqual(0)
      }
    }.pendingUntilFixed

    "work" in {
      val input = """
        | clicks := //clicks
        |
        | uniform := std::random::uniform(12)
        | observe(clicks, uniform + 10)
        | """.stripMargin

      val result = evalE(input)

      val clicks = """//clicks"""
      val resultClicks = evalE(clicks)

      result must haveSize(resultClicks.size)

      result must haveAllElementsLike {
        case (ids, SDecimal(d)) => 
          ids must haveSize(1)

          d must be_>=(BigDecimal(10))
          d must be_<(BigDecimal(11))
      }
    }.pendingUntilFixed
  }
}
