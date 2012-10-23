package com.precog
package ragnarok
package test

object ImpressionsTestSuite extends ClicksLikePerfTestSuite {
  val data = "//impressions"

  "simple" := simpleQueries()
  "grouping" := groupingQueries()
  "advanced grouping" := advancedGroupingQueries()
}


