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
