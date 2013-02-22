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
package com.precog.niflheim

import com.precog.common.Path

import org.scalacheck.{Arbitrary, Gen}

import org.specs2.ScalaCheck
import org.specs2.mutable.Specification

class NIHDBActorSpecs extends Specification with ScalaCheck {
  import Gen._

  val hasSuffixGen: Gen[String] = resultOf[String, String] {
    case prefix => prefix + NIHDBActor.escapeSuffix
  }(Arbitrary(alphaStr))

  val componentGen: Gen[String] = Gen.oneOf(Gen.value(NIHDBActor.cookedSubdir), Gen.value(NIHDBActor.rawSubdir), hasSuffixGen, alphaStr)

  implicit val pathGen: Arbitrary[Path] =
    Arbitrary(for {
      componentCount <- chooseNum(0, 200)
      components     <- listOfN(componentCount, componentGen)
    } yield Path(components))

  "NIHDBActor path escaping" should {
    import NIHDBActor._

    "Handle arbitrary paths with components needing escaping" in check {
      (p: Path) => unescapePath(escapePath(p)) mustEqual p
    }
  }
}
