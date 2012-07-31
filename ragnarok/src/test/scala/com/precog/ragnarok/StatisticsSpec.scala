package com.precog.ragnarok

import org.specs2.mutable.Specification

import scalaz.std.option._
import scalaz.syntax.semigroup._
import scalaz.syntax.applicative._


class StatisticsSpec extends Specification {
  "statistics is a semigroup that" should {
    "be associative" in {
      val x: List[Option[Statistics]] =
        List(1, 2, 3, 4, 5, 6) map (x => Some(Statistics(x, tails = 1)))

      val left = x.foldLeft(None: Option[Statistics])(_ |+| _)
      val right = x.foldRight(None: Option[Statistics])(_ |+| _)
      val split = (x take 3).foldLeft(None: Option[Statistics])(_ |+| _) |+|
        (x drop 3).foldLeft(None: Option[Statistics])(_ |+| _)
     
      (left |@| right |@| split) { (left, right, split) =>
        left.mean must beCloseTo(right.mean, 1e-10)
        left.variance must beCloseTo(right.variance, 1e-10)
        left.count must_== right.count

        left.mean must beCloseTo(split.mean, 1e-10)
        left.variance must beCloseTo(split.variance, 1e-10)
        left.count must_== split.count
      } must beSome
    }.pendingUntilFixed
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
  }
}

