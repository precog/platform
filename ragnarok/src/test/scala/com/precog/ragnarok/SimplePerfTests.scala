package com.precog.ragnarok


object SimplePerfTests extends PerfTestSuite {
  "simple arithmetic" := {
    query("1 + 2")
    query("123 * 321")
    query("144 / 12")
    query("1.23412 * 2.3")
  }
}

