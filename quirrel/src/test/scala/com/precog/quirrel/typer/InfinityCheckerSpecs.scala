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
package quirrel
package typer

import com.codecommit.gll.LineStream

import org.specs2.mutable.Specification

import parser._

object InfinityCheckerSpecs extends Specification
    with StubPhases
    with CompilerUtils
    with Compiler
    with ProvenanceChecker
    with StaticLibrarySpec {
  
  import ast._
  import library._

  private def parseSingle(str: LineStream): Expr = {
    val set = parse(str)
    set must haveSize(1)
    set.head
  }
  
  private def parseSingle(str: String): Expr = parseSingle(LineStream(str))

  "do something right" should {
    "reject a distribution" in {
      val expr @ Dispatch(_, _, _) =
        parseSingle("std::random::foobar(5)")
      expr.errors mustEqual Set(CannotUseDistributionWithoutSampling)
    }

    "accept an observation of a distribution" in {
      val expr @ Observe(_, _, _) =
        parseSingle("observe(5, std::random::foobar(5))")
      expr.errors must beEmpty
    }

    "reject an observed count of a distribution" in {
      val expr @ Observe(_, _, _) =
        parseSingle("observe(5, count(std::random::foobar(5)))")
      expr.errors mustEqual Set(CannotUseDistributionWithoutSampling)
    }

    "accept an op1 of a distribution" in {
      val expr @ Observe(_, _, _) =
        parseSingle("observe(5, std::math::floor(std::random::foobar(5)))")
      expr.errors must beEmpty
    }

    "reject an unobserved op1 of a distribution" in {
      val expr @ Dispatch(_, _, _) =
        parseSingle("std::math::floor(std::random::foobar(5))")
      expr.errors mustEqual Set(CannotUseDistributionWithoutSampling)
    }

    "reject a count of a distribution" in {
      val expr @ Dispatch(_, _, _) =
        parseSingle("count(std::random::foobar(5))")
      expr.errors mustEqual Set(CannotUseDistributionWithoutSampling)
    }

    "reject a distinct of a distribution" in {
      val expr @ Dispatch(_, _, _) =
        parseSingle("distinct(std::random::foobar(5))")
      expr.errors mustEqual Set(CannotUseDistributionWithoutSampling)
    }

    "reject a distinct of a distribution through an observe" in {
      val expr @ Observe(_, _, _) =
        parseSingle("observe(4, distinct(std::random::foobar(5)))")
      expr.errors mustEqual Set(CannotUseDistributionWithoutSampling)
    }

    "accept a distribution inside a let" in {
      val expr @ Let(_, _, _, _, _) =
        parseSingle("""
          f(x) := observe(5, std::random::foobar(x))
          f(12)
        """)
      expr.errors must beEmpty
    }

    "reject unobserved distribution inside a let" in {
      val expr @ Let(_, _, _, _, _) =
        parseSingle("""
          f(x) := std::random::foobar(x)
          f(12)
        """)
      expr.errors mustEqual Set(CannotUseDistributionWithoutSampling)
    }

    "reject unobserved distribution actual" in {
      val expr @ Let(_, _, _, _, _) =
        parseSingle("""
          f(x) := x
          f(std::random::foobar(12))
        """)
      expr.errors mustEqual Set(CannotUseDistributionWithoutSampling)
    }

    "accept observed distribution actual" in {
      val expr @ Let(_, _, _, _, _) =
        parseSingle("""
          f(x) := x
          f(observe(5, std::random::foobar(12)))
        """)
      expr.errors must beEmpty
    }

    "reject unobserved distribution in summand" in {
      val expr @ Let(_, _, _, _, _) =
        parseSingle("""
          f(x) := x
          f(observe(5, std::random::foobar(12))) + f(std::random::foobar(12))
        """)
      expr.errors mustEqual Set(CannotUseDistributionWithoutSampling)
    }

    "reject unobserved actual as solve condition" in {
      val expr @ Solve(_, _, _) =
        parseSingle("""
          solve 'a = std::random::foobar(2) 'a
        """)
      expr.errors mustEqual Set(CannotUseDistributionWithoutSampling)
    }

    "reject observation of a solve" in {
      val expr @ Let(_, _, _, _, _) =
        parseSingle("""
          z := solve 'a = std::random::foobar(2) 'a
          observe(5, z)
        """)
      expr.errors mustEqual Set(CannotUseDistributionWithoutSampling)
    }

    "reject another observation of a solve" in {
      val expr @ Observe(_, _, _) =
        parseSingle("""
          observe(5, solve 'a = std::random::foobar(2) 'a)
        """)
      expr.errors mustEqual Set(CannotUseDistributionWithoutSampling)
    }

    "reject a yet another observation of a solve" in {
      val expr @ Observe(_, _, _) =
        parseSingle("""
          observe(5, solve 'a = 8 'a + std::random::foobar(12))
        """)
      expr.errors mustEqual Set(CannotUseDistributionWithoutSampling)
    }

    "reject count of distribution" in {
      val expr @ Let(_, _, _, _, _) =
        parseSingle("""
          foo := //foo
          solve 'a = foo.a
          'a + count(std::math::floor(std::random::foobar(12)))
        """)
      expr.errors mustEqual Set(CannotUseDistributionWithoutSampling)
    }

    "reject union" in {
      val expr @ Observe(_, _, _) =
        parseSingle("""
          observe(5, std::random::foobar(12) union std::math::floor(5))
        """)
      expr.errors mustEqual Set(CannotUseDistributionWithoutSampling)
    }

    "reject intersect" in {
      val expr @ Observe(_, _, _) =
        parseSingle("""
          observe(5, count(std::random::foobar(12)) intersect std::math::floor(5))
        """)
      expr.errors mustEqual Set(CannotUseDistributionWithoutSampling)
    }

    "accept sum" in {
      val expr @ Observe(_, _, _) =
        parseSingle("""
          observe(5, std::random::foobar(12) + std::math::floor(5))
        """)
      expr.errors must beEmpty
    }

    "reject object concat" in {
      val expr @ Observe(_, _, _) =
        parseSingle("""
          observe(5, {a: std::random::foobar(12)} with {b: std::math::floor(5)})
        """)
      expr.errors mustEqual Set(CannotUseDistributionWithoutSampling)
    }

    "accept array creation" in {
      val expr @ Observe(_, _, _) =
        parseSingle("""
          observe(5, [std::random::foobar(12), std::math::floor(5)])
        """)
      expr.errors must beEmpty
    }

    "reject unobserved in object" in {
      val expr @ With(_, _, _) =
        parseSingle("""
          {a: std::random::foobar(12)} with {b: std::math::floor(5)}
        """)
      expr.errors mustEqual Set(CannotUseDistributionWithoutSampling)
    }

    "reject another unobserved in object" in {
      val expr @ ObjectDef(_, _) =
        parseSingle("""
          {a: std::random::foobar(23)}
        """)
      expr.errors mustEqual Set(CannotUseDistributionWithoutSampling)
    }

    "reject unobserved in array" in {
      val expr @ ArrayDef(_, _) =
        parseSingle("""
          [std::random::foobar(12), 7]
        """)
      expr.errors mustEqual Set(CannotUseDistributionWithoutSampling)
    }

    "accept observe of sum" in {
      val expr @ Let(_, _, _, _, _) =
        parseSingle("""
          rand := std::random::foobar(12)
          observe(3, rand + 2)
        """)
      expr.errors must beEmpty
    }

    "reject unobserved filter" in {
      val expr @ Let(_, _, _, _, _) =
        parseSingle("""
          rand := std::random::foobar(12)
          rand where true
        """)
      expr.errors mustEqual Set(CannotUseDistributionWithoutSampling)
    }

    "reject filter" in {
      val expr @ Let(_, _, _, _, _) =
        parseSingle("""
          rand := std::random::foobar(12)
          observe(7, rand where rand > 0.5)
        """)
      expr.errors mustEqual Set(CannotUseDistributionWithoutSampling)
    }

    "reject unobserved filter" in {
      val expr @ Let(_, _, _, _, _) =
        parseSingle("""
          rand := std::random::foobar(12)
          rand where rand > 0.5
        """)
      expr.errors mustEqual Set(CannotUseDistributionWithoutSampling)
    }

    "accept observe through import" in {
      val expr @ Observe(_, _, _) =
        parseSingle("""
          observe(5,
            import std::lib::* 
            std::random::foobar(12))
        """)
      expr.errors must beEmpty
    }

    "reject through import" in {
      val expr @ Import(_, _, _) =
        parseSingle("""
          import std::lib::* 
          std::random::foobar(12)
        """)
      expr.errors mustEqual Set(CannotUseDistributionWithoutSampling)
    }

    "reject through assert" in {
      val expr @ Observe(_, _, _) =
        parseSingle("""
          observe(5,
            assert std::random::foobar(12) "baz")
        """)
      expr.errors mustEqual Set(CannotUseDistributionWithoutSampling)
    }

    "reject new" in {
      val expr @ New(_, _) =
        parseSingle("""
          new std::random::foobar(12)
        """)
      expr.errors mustEqual Set(CannotUseDistributionWithoutSampling)
    }

    "accept observed new" in {
      val expr @ Observe(_, _, _) =
        parseSingle("""
          observe(5,
            new std::random::foobar(12))
        """)
      expr.errors must beEmpty
    }

    "accept observed op1" in {
      val expr @ Observe(_, _, _) =
        parseSingle("""
          observe(5,
            new std::math::floor(std::random::foobar(12)))
        """)
      expr.errors must beEmpty
    }

    "reject through relate" in {
      val expr @ Observe(_, _, _) =
        parseSingle("""
          observe(5,
            std::random::foobar(12) ~ "beta" false)
        """)
      expr.errors mustEqual Set(CannotUseDistributionWithoutSampling)
    }

    "accept as 'in' of relate" in {
      val expr @ Observe(_, _, _) =
        parseSingle("""
          observe(5,
            (new "gamma") ~ (new "beta") std::random::foobar(12))
        """)
      expr.errors must beEmpty
    }

    "reject in cond" in {
      val expr @ Observe(_, _, _) =
        parseSingle("""
          observe(5,
            if true then false else std::random::foobar(12))
        """)
      expr.errors mustEqual Set(CannotUseDistributionWithoutSampling)
    }

    "reject again in cond" in {
      val expr @ Observe(_, _, _) =
        parseSingle("""
          observe(5,
            if true then std::random::foobar(12) else false)
        """)
      expr.errors mustEqual Set(CannotUseDistributionWithoutSampling)
    }

    "accept as RHS of observe" in {
      val expr @ Observe(_, _, _) =
        parseSingle("""
          observe(std::random::foobar(12), true)
        """)
      expr.errors mustEqual Set(CannotUseDistributionWithoutSampling)
    }

    "accept through sum" in {
      val expr @ Observe(_, _, _) =
        parseSingle("""
          observe(
            observe(8, std::random::foobar(12) + 9),
            std::random::foobar(13))
        """)
      expr.errors must beEmpty
    }

    "both sides of observe infinite" in {
      val expr @ Observe(_, _, _) =
        parseSingle("""
          observe(std::random::foobar(12), std::random::foobar(13))
        """)
      expr.errors mustEqual Set(CannotUseDistributionWithoutSampling)
    }

    "accept through object concat" in {
      val input = """
        | clicks := //clicks
        |
        | uniform := std::random::foobar(12)
        | observations := observe(clicks, uniform)
        |
        | {pageId: clicks.pageId, rand: observations}
        | """.stripMargin

      val expr @ Let(_, _, _, _, _) = parseSingle(input)

      expr.errors must beEmpty
    }
  }
}
