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
package com.precog.util

import org.specs2.mutable._
import org.specs2.matcher.MatchResult

import org.specs2.mutable._
import org.specs2.ScalaCheck
import org.scalacheck._

import scala.math._

object LevenshteinSpecs extends Specification with ScalaCheck {
  "levenshtein algorithm" should {
    "be correct for given cases" in {
      Levenshtein.distance("foo", "foo") mustEqual 0

      Levenshtein.distance("foo", "fo") mustEqual 1
      Levenshtein.distance("foo", "fou") mustEqual 1
      Levenshtein.distance("foo", "foox") mustEqual 1

      Levenshtein.distance("foo", "o") mustEqual 2
      Levenshtein.distance("foo", "quo") mustEqual 2
      Levenshtein.distance("foo", "ggfoo") mustEqual 2

      Levenshtein.distance("foo", "q") mustEqual 3
      Levenshtein.distance("foo", "bz") mustEqual 3
      Levenshtein.distance("foo", "bar") mustEqual 3
      Levenshtein.distance("foo", "baro") mustEqual 3
      Levenshtein.distance("foo", "zaro") mustEqual 3
      Levenshtein.distance("foo", "zzro") mustEqual 3
    }

    "be 0 iff s == t" in check { (s: String, t: String) =>
      val d = Levenshtein.distance(s, t)
      if (s == t) d mustEqual 0 else d mustNotEqual 0
    }

    "be at least |len(s) - len(t)|" in check { (s: String, t: String) =>
      val x = abs(s.length - t.length)
      Levenshtein.distance(s, t) must beGreaterThanOrEqualTo(x)
    }

    "be at most max(len(s), len(t))" in check { (s: String, t: String) =>
      val x = max(s.length, t.length)
      Levenshtein.distance(s, t) must beLessThanOrEqualTo(x)
    }

    "with equal lengths, be at most the hamming distance" in check { (u: String, v: String) =>
      // make s and t the same length by padding with spaces
      val x = u.length - v.length
      val p = " " * abs(x)
      val (s, t) = if (x == 0) (u, v) else if (x > 0) (u, v + p)  else (u + p, v)

      // find hammming distance
      var h = 0
      for (i <- 0 until s.length) {
        if (s.charAt(i) != t.charAt(i)) h += 1
      }
      Levenshtein.distance(s, t) must beLessThanOrEqualTo(h)
    }

    "satisfy triangle inequality" in check { (s: String, t: String, u: String) =>
      val d = Levenshtein.distance(s, t)
      val d1 = Levenshtein.distance(s, u)
      val d2 = Levenshtein.distance(t, u)
      d must beLessThanOrEqualTo(d1 + d2)
    }
  }
} 
