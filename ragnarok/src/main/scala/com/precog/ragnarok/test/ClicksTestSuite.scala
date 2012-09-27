package com.precog
package ragnarok
package test

object ClicksTestSuite extends ClicksLikePerfTestSuite {
  val data = "//clicks"

  "simple" := simpleQueries()
  "grouping" := groupingQueries()
  "advanced grouping" := advancedGroupingQueries()
}


