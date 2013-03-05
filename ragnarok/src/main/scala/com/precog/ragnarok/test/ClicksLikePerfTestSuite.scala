//package com.precog
//package ragnarok
//package test
//
//trait ClicksLikePerfTestSuite extends PerfTestSuite {
//  def data: String
//
//  def q(qry: String) = query("data := %s\n%s" format (data, qry))
//
//  def simpleQueries() {
//    // Library functions.
//
//    "math" := {
//      q("data.customer.age + data.customer.income")
//      q("data.customer.age * data.customer.income")
//      q("data.customer.income / data.customer.age")
//      q("data.customer.age - data.customer.income")
//      q("data.customer.age < data.customer.income")
//    }
//
//    "standard" := {
//      q("count(data.customer.ID)")
//      q("stdDev(data.customer.income)")
//      q("mean(data.customer.income)")
//      q("max(data.customer.income)")
//      q("min(data.customer.income)")
//      q("sum(data.customer.income)")
//      q("variance(data.customer.income)")
//      q("sumSq(data.customer.income)")
//      // q("geometricMean(data.customer.income)")
//    }
//
//    "stats" := {
//      def sq(qry: String) = q("import std::stats::*\n%s" format qry)
//      sq("corr(data.customer.age, data.customer.income)")
//      sq("cov(data.customer.age, data.customer.income)")
//      sq("linReg(data.customer.age, data.customer.income)")
//      sq("logReg(data.customer.age, data.customer.income)")
//      sq("rank(data.customer.income)")
//      sq("denseRank(data.customer.income)")
//    }
//
//    "string" := {
//      def sq(qry: String) = q("import std::string::*\n%s" format qry)
//      sq("concat(data.customer.state, data.marketing.referral)")
//      sq("length(data.customer.referral)")
//      sq("""matches(data.customer.state, "(CA|TX|WA)")""")
//      sq("indexOf(data.customer.ID, \"D\")")
//    }
//
//    "time" := {
//      def sq(qry: String) = q("import std::time::*\n" + qry)
//      sq("dayOfWeek(data.timeStamp)")
//      sq("getMillis(data.timeStamp)")
//      sq("hoursPlus(data.timeStamp, 12)")
//      sq("monthsBetween(data.timeStamp, hoursPlus(data.timeStamp, 120))")
//    }
//
//    "augment" := {
//      q("""import std::time::hourOfDay
//          |data' := data with { hour: hourOfDay(data.timeStamp) }
//          |data'""".stripMargin)
//    }
//
//    "constraints" := {
//      q("data where data.customer.state = \"CA\"")
//      q("data where data.customer.state = \"CA\" & data.product.ID = \"01B2BF6D\"")
//      q("data where data.customer.age > 30 & data.customer.income < 50000")
//    }
//
//    "bad mean" := q("sum(data.product.price) / count(data.product.price)")
//
//    "union and intersect" := {
//      q("(data where data.customer.age < 25) union (data where data.customer.age > 50)")
//      // q("""data' := new data
//      //     |data ~ data'
//      //     |highRollers := data.customer where data.product.price > 100.00
//      //     |highEarners := data'.customer where data.customer.income > 50000
//      //     |highRollers intersect highEarners""".stripMargin)
//    }
//  }
//
//
//  def groupingQueries() {
//    q("""solve 'product
//        |{ productID: 'product,
//        |  aveIncome: mean(data.customer.income where data.product.Id = 'product) }""".stripMargin)
//
//    q("""states := solve 'state
//      | {state: 'state,
//      |  stateCount: count(data.customer.state where data.customer.state = 'state) }
//      |
//      |rank := std::stats::rank(neg states.stateCount)
//      |
//      |states where rank <= 5""".stripMargin)
//
//    // q("""salesByProduct := solve 'id
//    //     |  { product: 'id,
//    //     |    sales: sum(data.product.price where data.product.ID = 'id) }
//    //     |rank := std::stats::rank(neg salesByProduct.sales)
//    //     |otherProducts :=
//    //     |  { product: "other",
//    //     |    sales: sum(salesByProduct.sales where rank > 5) }
//    //     |top5Products := salesByProduct where rank <= 5
//    //     |allProducts := otherProducts union top5Products
//    //     |allProducts""".stripMargin)
//
//    // Weird problem w/ different columns / column refs given (ColumnRef is
//    // CString, Column is CLong).
//
//    //q("""import std::time::*
//    //    |data' := data with
//    //    |{ month: monthOfYear(data.timeStamp) }
//    //    |dataByMonth := solve 'month 
//    //    |{ month: 'month,
//    //    |  conversions: count(data' where data'.month = 'month),
//    //    |}
//    //    |dataByMonth""".stripMargin)
//  }
//
//  def advancedGroupingQueries() {
//
//    // Pathological, as many universes as there are ages.
//    q("""solve 'age = data.age
//        |{ age: 'age,
//        |  count: count(data where data.age < 'age) }""".stripMargin)
//
//    q("""solve 'gender, 'gamer
//        |{ gender: 'gender,
//        |  gamer: 'gamer,
//        |  count: count(data where data.customer.gender = 'gender |
//        |                          data.customer.isCasualGamer = 'gamer) }""".stripMargin)
//  }
//}
//
//
