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
package com.precog.muspelheim


import com.precog.yggdrasil._
import com.precog.yggdrasil.execution.EvaluationContext
import com.precog.common._

trait EnrichmentSpecs extends EvalStackSpecs {
  "enrichment" should {
    "enrich data" in {
      val input = """
          medals := //summer_games/london_medals
          precog::enrichment(medals, {
              url: "http://wrapper",
              options: { field: "crap" }
            })
        """.stripMargin

      val input2 = """
          medals := //summer_games/london_medals
          { crap: medals }
        """.stripMargin

      val results = stack.evalE(input)
      val results2 = stack.evalE(input2)

      val data = results map { case (_, x) => x }
      val data2 = results2 map { case (_, x) => x }

      data must haveSize(1019)
      data must_== data2
    }

    "enriched data is related to original data" in {
      val input = """
          medals := //summer_games/london_medals
          medals' := precog::enrichment(medals.Age, {
              url: "http://wrapper",
              options: { field: "crap" }
            })
          { orig: medals, enriched: medals' }
        """.stripMargin

      val results = stack.evalE(input)

      results must haveSize(1019)
    }

    "server error causes enrichment to fail" in {
      val input = """
          medals := //summer_games/london_medals
          precog::enrichment(medals, { url: "http://server-error" })
        """.stripMargin
      stack.evalE(input) must throwA[Throwable]
    }

    "misbehaving enricher causes enrichment to fail" in {
      val input = """
          medals := //summer_games/london_medals
          precog::enrichment(medals, { url: "http://misbehave" })
        """.stripMargin
      stack.evalE(input) must throwA[Throwable]
    }

    "empty response causes enrichment to fail" in {
      val input = """
          medals := //summer_games/london_medals
          precog::enrichment(medals, { url: "http://empty" })
        """.stripMargin
      stack.evalE(input) must throwA[Throwable]
    }

    "options are passed through to enricher" in {
      val input = """
          data := "Monkey"
          precog::enrichment(data, {
              url: "http://options",
              options: {
                name: "Tom",
                age: 27,
                mission: "Write tons of code.",
                status: false
              }
            })
        """.stripMargin

      val results = stack.evalE(input)
      results must haveSize(1)

      val (_, actual) = results.head

      val expected = SObject(Map(
        "email" -> SString("nobody@precog.com"),
        "accountId" -> SString("dummyAccount"),
        "name" -> SString("Tom"),
        "age" -> SDecimal(27),
        "mission" -> SString("Write tons of code."),
        "status" -> SFalse))

      actual must_== expected
    }

    "email/accountId cannot be overriden in options" in {
      val input = """
          data := "Monkey"
          precog::enrichment(data, {
              url: "http://options",
              options: {
                email: "haha@evil.com",
                accountId: "so evil"
              }
            })
        """.stripMargin

      val results = stack.evalE(input)
      results must haveSize(1)

      val (_, actual) = results.head

      val expected = SObject(Map(
        "email" -> SString("nobody@precog.com"),
        "accountId" -> SString("dummyAccount")))

      actual must_== expected
    }
  }
}
