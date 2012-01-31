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
package com.reportgrid.analytics

import org.specs2.mutable.Specification

class TokenSpec extends Specification {
  val TestToken = Token(
    tokenId        = "C7A18C95-3619-415B-A89B-4CE47693E4CC",
    parentTokenId  = Some(Token.Root.tokenId),
    accountTokenId = "C7A18C95-3619-415B-A89B-4CE47693E4CC",
    path           = Path("unittest"),
    permissions    = Permissions(true, true, true, true),
    expires        = Token.Never,
    limits         = Limits(order = 2, depth = 5, limit = 20, tags = 2, rollup = 2)
  )

  "relativeTo" should {
    "correctly limit path visibility at the root" in {
      TestToken.relativeTo(Token.Root).path must_== Path("unittest")
    }

    "correctly limit path visibility" in {
      val child = TestToken.issue(relativePath = Path("/child"))
      (child.relativeTo(TestToken).path must_== Path("child")) and 
      (child.relativeTo(Token.Root).path must_== Path("unittest/child"))
    }
  }
}

// vim: set ts=4 sw=4 et:
