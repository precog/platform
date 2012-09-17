package com.precog
package ragnarok
package test


object ConversionsTestSuite extends ClicksLikePerfTestSuite {
  val data = "//conversions"

  "simple" := {
    simpleQueries()

    "cross" := {
      q("""data' = new data
          |data ~ data' where std::time::dayOfYear(data.timeStamp) = std::time::dayOfYear(data'.timeStamp) & data.customer.ID != data'.customer.ID""".stripMargin)
    }
  }

  "grouping" := groupingQueries()

  "advanced grouping" := {
    advancedGroupingQueries()

    // Pathological, as many universes as there are ages.
    q("""solve 'age = data.age
        |{ count: count(data where data.age < 'age),
        |  value: 'age }""".stripMargin)
  }
}
