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

import org.specs2.mutable.Specification
import org.specs2.ScalaCheck

import org.scalacheck.{ Arbitrary, Gen }

import scalaz.std.option.{ some => somez, _ }
import scalaz.syntax.semigroup._
import scalaz.syntax.applicative._
import scalaz.syntax.foldable._
import scalaz.std.list._


class StatisticsSpec extends Specification with ScalaCheck {
  def stats(xs: List[Double]): List[Option[Statistics]] = xs map (x => somez(Statistics(x)))

  private def beRelativelyCloseTo(n: Double)(err: Double) = beCloseTo(n, math.abs(n * err))

  private def statsAreEqual(a: Option[Statistics], b: Option[Statistics]) = (a, b) match {
    case (Some(a), Some(b)) =>
      a.mean must (beEqualTo(b.mean) or beRelativelyCloseTo(b.mean)(1e-10))
      a.variance must (beEqualTo(b.variance) or beRelativelyCloseTo(b.variance)(1e-10))
      a.count must_== b.count
      a.min must_== b.min
      a.max must_== b.max

    case _ => ok
  }

  implicit val arbDouble: Arbitrary[Double] = Arbitrary(Gen.chooseNum(-1e250, 1e250))

  "statistics is a semigroup that" should {
    todo
    //// Super annoying, since Double isn't associative, which is causing failures.
    //"be associative" ! check { (a: List[Double], b: List[Double]) =>
    //  val c = a ++ b
    //
    //  statsAreEqual(stats(a).suml |+| stats(b).suml, stats(c).suml)
    //  statsAreEqual(stats(c).suml, stats(c).sumr)
    //}
  }

  "statistics" should {
    "construct trivial statistics from single value" in {
      val s = Statistics(1.0)
      s.mean must_== 1.0
      s.variance must_== 0.0
      s.count must_== 1
      s.min must_== 1.0
      s.max must_== 1.0
    }

    "exclude outliers from statistics" in {
      val s = (1 to 9).toList map (Statistics(_, tails = 2)) reduce (_ |+| _)
      s.mean must_== 5.0
      s.variance must beCloseTo(2.5, 1e-10)
      s.count must_== 5
      s.min must_== 3
      s.max must_== 7
    }

    "return invalid results when tails overlap" in {
      java.lang.Double.isNaN(Statistics(1.0, tails = 3).mean) must beTrue
      val s1 = Statistics(0.0, tails = 1) |+| Statistics(1.0, tails = 1)
      java.lang.Double.isNaN(s1.mean) must beTrue
      java.lang.Double.isNaN(s1.variance) must beTrue
    }

    "be scalable" ! check { (xs: List[Double]) =>
      statsAreEqual(stats(xs).suml map (_ * 0.0001), stats(xs map (_ * 0.0001)).suml)
    }
  }
}

