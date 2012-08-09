package com.precog.ragnarok


object PlatformPerfTests extends PerfTestSuite {
  "arithmetic" := {
    query("1 + 2")
    query("123 * 321")
    query("144 / 12")
    query("1.23412 * 2.3")
    query("1.219912838481e1000 + 1")
  }

  "objects" := {
    // TODO: When cross is available.
    //query("""{ name: "John", age: 29, gender: "male" }""")
    //query("""{ name: "John", age: 29, gender: null }""")
  }

  "datasets" := {
    query("count(//clicks)")
    query("""//campaigns where //campaigns.gender != "female" """)
    query("""clicks := //clicks
            |count(clicks where clicks.pageId = "page-0")""".stripMargin)

    "empty" := {
      "nonexistant" := query("//foo.bar")
      query("//campaigns.cpm + //campaigns.gender")
      query("//campaigns.cpm + //campaigns.ageRange")
      query("//campaigns.gender + //campaigns.ageRange")
    }

    query("//richie1/test")

    query("""mean(//movie_ratings)""")
    query("""stdDev(//movie_ratings)""")
  }
}

