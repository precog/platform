package com.reportgrid.analytics

import org.specs2.mutable.Specification

class TokenSpec extends Specification {
  val TestToken = Token(
    tokenId        = "C7A18C95-3619-415B-A89B-4CE47693E4CC",
    parentTokenId  = Some(Token.Root.tokenId),
    accountTokenId = "C7A18C95-3619-415B-A89B-4CE47693E4CC",
    path           = "unittest",
    permissions    = Permissions(true, true, true, true),
    expires        = Token.Never,
    limits         = Limits(order = 2, depth = 5, limit = 20, tags = 2, rollup = 2)
  )

  "relativeTo" should {
    "correctly limit path visibility at the root" in {
      TestToken.relativeTo(Token.Root).path must_== Path("unittest")
    }

    "correctly limit path visibility" in {
      val child = TestToken.issue(relativePath = "/child")
      (child.relativeTo(TestToken).path must_== Path("child")) and 
      (child.relativeTo(Token.Root).path must_== Path("unittest/child"))
    }
  }
}

// vim: set ts=4 sw=4 et:
