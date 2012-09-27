package com.precog
package ragnarok
package test


object ConversionsTestSuite extends ClicksLikePerfTestSuite {
  val data = "//conversions"

  "simple" := simpleQueries()
  "grouping" := groupingQueries()
  "advanced grouping" := advancedGroupingQueries()
}
