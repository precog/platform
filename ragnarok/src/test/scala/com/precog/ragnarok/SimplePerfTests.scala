package com.precog.ragnarok


object SimplePerfTests extends PerfTestSuite {
  "arithmetic" := {
    "addition" := query("1 + 2")
    query("123 * 321")
    query("144 / 12")
    query("1.23412 * 2.3")
  }

  query("count(//clicks)")
  query("""clicks := //clicks
          |count(clicks where clicks.pageId = "page-0")""".stripMargin)
}

