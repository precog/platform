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

