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
package ragnarok
package test

trait ClicksLikePerfTestSuite extends PerfTestSuite {
  def data: String

  def q(qry: String) = query("data := %s\n%s" format (data, qry))

  def simpleQueries() {
    // Library functions.

    "math" := {
      q("data.customer.age + data.customer.income")
      q("data.customer.age * data.customer.income")
      q("data.customer.income / data.customer.age")
      q("data.customer.age - data.customer.income")
      q("data.customer.age < data.customer.income")
    }

    "standard" := {
      q("count(data.customer.ID)")
      q("stdDev(data.customer.income)")
      q("mean(data.customer.income)")
      q("max(data.customer.income)")
      q("min(data.customer.income)")
      q("sum(data.customer.income)")
      q("variance(data.customer.income)")
      q("sumSq(data.customer.income)")
      // q("geometricMean(data.customer.income)")
    }

    "stats" := {
      def sq(qry: String) = q("import std::stats::*\n%s" format qry)
      sq("corr(data.customer.age, data.customer.income)")
      sq("cov(data.customer.age, data.customer.income)")
      sq("linReg(data.customer.age, data.customer.income)")
      sq("logReg(data.customer.age, data.customer.income)")
      sq("rank(data.customer.income)")
      sq("denseRank(data.customer.income)")
    }

    "string" := {
      def sq(qry: String) = q("import std::string::*\n%s" format qry)
      sq("concat(data.customer.state, data.marketing.referral)")
      sq("length(data.customer.referral)")
      sq("""matches(data.customer.state, "(CA|TX|WA)")""")
      sq("indexOf(data.customer.ID, \"D\")")
    }

    "time" := {
      def sq(qry: String) = q("import std::time::*\n" + qry)
      sq("dayOfWeek(data.timeStamp)")
      sq("getMillis(data.timeStamp)")
      sq("hoursPlus(data.timeStamp, 12)")
      sq("monthsBetween(data.timeStamp, hoursPlus(data.timeStamp, 120))")
    }

    "augment" := {
      q("""import std::time::hourOfDay
          |data' := data with { hour: hourOfDay(data.timeStamp) }
          |data'""".stripMargin)
    }

    "constraints" := {
      q("data where data.customer.state = \"CA\"")
      q("data where data.customer.state = \"CA\" & data.product.ID = \"01B2BF6D\"")
      q("data where data.customer.age > 30 & data.customer.income < 50000")
    }

    "bad mean" := q("sum(data.product.price) / count(data.product.price)")

    "union and intersect" := {
      q("(data where data.customer.age < 25) union (data where data.customer.age > 50)")
      // q("""data' := new data
      //     |data ~ data'
      //     |highRollers := data.customer where data.product.price > 100.00
      //     |highEarners := data'.customer where data.customer.income > 50000
      //     |highRollers intersect highEarners""".stripMargin)
    }
  }


  def groupingQueries() {
    q("""solve 'product
        |{ productID: 'product,
        |  aveIncome: mean(data.customer.income where data.product.Id = 'product) }""".stripMargin)

    q("""states := solve 'state
      | {state: 'state,
      |  stateCount: count(data.customer.state where data.customer.state = 'state) }
      |
      |rank := std::stats::rank(neg states.stateCount)
      |
      |states where rank <= 5""".stripMargin)

    q("""salesByProduct := solve 'id
        |{product: 'id,
        |  sales: sum(data.product.price where data.product.ID = 'id)}
        |  rank := std::stats::rank(neg salesByProduct.sales)
        |  otherProducts := {product: "other",sales: sum(salesByProduct.sales where rank > 5)}
        |  top5Products := salesByProduct where rank <= 5)
        |allProducts := otherProducts union top5Products
        |allProducts""".stripMargin)

    q("""import std::time::*
        |data' := data with
        |{ month: monthOfYear(data.timeStamp) }
        |dataByMonth := solve 'month 
        |{ month: 'month,
        |  conversions: count(data' where data'.month = 'month),
        |}
        |dataByMonth""".stripMargin)
  }

  def advancedGroupingQueries() {
    q("""data' := new data
        |solve 'gender, 'gamer
        |data where data.customer.gender = 'gender | data.customer.isCasualGamer = 'gamer
        |data' where data'.customer.gender = 'gender | data'.customer.isCasualGamer = 'gamer
        |{ gender: 'gender, gamer: 'gamer }""".stripMargin)
  }
}


