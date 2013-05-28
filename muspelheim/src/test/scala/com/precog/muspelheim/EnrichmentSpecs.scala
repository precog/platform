package com.precog.muspelheim


import com.precog.yggdrasil._
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

      val results = evalE(input)
      val results2 = evalE(input2)

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

      val results = evalE(input)

      results must haveSize(1019)
    }

    "server error causes enrichment to fail" in {
      val input = """
          medals := //summer_games/london_medals
          precog::enrichment(medals, { url: "http://server-error" })
        """.stripMargin
      evalE(input) must throwA[Throwable]
    }

    "misbehaving enricher causes enrichment to fail" in {
      val input = """
          medals := //summer_games/london_medals
          precog::enrichment(medals, { url: "http://misbehave" })
        """.stripMargin
      evalE(input) must throwA[Throwable]
    }

    "empty response causes enrichment to fail" in {
      val input = """
          medals := //summer_games/london_medals
          precog::enrichment(medals, { url: "http://empty" })
        """.stripMargin
      evalE(input) must throwA[Throwable]
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

      val results = evalE(input)
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

      val results = evalE(input)
      results must haveSize(1)

      val (_, actual) = results.head

      val expected = SObject(Map(
        "email" -> SString("nobody@precog.com"),
        "accountId" -> SString("dummyAccount")))

      actual must_== expected
    }
  }
}
