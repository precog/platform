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
package daze

import bytecode._
import org.specs2.mutable._

import scalaz.{NonEmptyList => NEL, _}

object ReductionFinderSpecs extends Specification with ReductionFinder with StaticLibrary {
  import instructions._
  import dag._

  "mega reduce" should {
    "in a load, rewrite to itself" >> {
      val line = Line(0, "")
      val input = dag.LoadLocal(line, Root(line, PushString("/foo")))

      megaReduce(input, findReductions(input)) mustEqual input
    }

    "in a reduction of a singleton" >> {
      val line = Line(0, "")

      val input = dag.Reduce(line, Reduction(Vector(), "count", 0x0000), Root(line, PushString("alpha")))
      val megaR = dag.MegaReduce(line, NEL(input), Root(line, PushString("alpha")))

      val expected = dag.Join(line, DerefArray, CrossLeftSort, 
        megaR,
        Root(line, PushNum("0")))

      megaReduce(input, findReductions(input)) mustEqual expected
    }

    "in a single reduction" >> {
      val line = Line(0, "")

      val input = dag.Reduce(line, Reduction(Vector(), "count", 0x0000), 
        dag.LoadLocal(line, Root(line, PushString("/foo"))))


      val parent = dag.LoadLocal(line, Root(line, PushString("/foo")))
      val red = Reduction(Vector(), "count", 0x0000)
      val megaR = dag.MegaReduce(line, NEL(dag.Reduce(line, red, parent)), parent)

      val expected = dag.Join(line, DerefArray, CrossLeftSort, 
        megaR,
        Root(line, PushNum("0")))

      megaReduce(input, findReductions(input)) mustEqual expected
    }   

    "in a join of two reductions on the same dataset" >> {
      val line = Line(0, "")

      val parent = dag.LoadLocal(line, Root(line, PushString("/foo")))

      val input = Join(line, Add, CrossLeftSort, 
        dag.Reduce(line, Reduction(Vector(), "count", 0x0000), parent),
        dag.Reduce(line, Reduction(Vector(), "stdDev", 0x0007), parent))

      val red1 = Reduction(Vector(), "count", 0x0000)
      val red2 = Reduction(Vector(), "stdDev", 0x0007)
      val reductions = NEL(dag.Reduce(line, red1, parent), dag.Reduce(line, red2, parent))
      val megaR = dag.MegaReduce(line, reductions, parent)

      val expected = Join(line, Add, CrossLeftSort,
        Join(line, DerefArray, CrossLeftSort,
          megaR,
          Root(line, PushNum("0"))),
        Join(line, DerefArray, CrossLeftSort,
          megaR,
          Root(line, PushNum("1"))))

      megaReduce(input, findReductions(input)) mustEqual expected
    }

    "in a join where only one side is a reduction" >> {
      "right" >> {
        val line = Line(0, "")

        val input = Join(line, Add, CrossLeftSort,
          dag.Operate(line, Neg, 
            dag.LoadLocal(line, Root(line, PushString("/foo")))),
          dag.Reduce(line, Reduction(Vector(), "stdDev", 0x0007), 
            dag.LoadLocal(line, Root(line, PushString("/foo")))))

        val parent = dag.LoadLocal(line, Root(line, PushString("/foo")))
        val red = Reduction(Vector(), "stdDev", 0x0007)
        val megaR = dag.MegaReduce(line, NEL(dag.Reduce(line, red, parent)), parent)

        val expected = Join(line, Add, CrossLeftSort,
          dag.Operate(line, Neg, parent),
          dag.Join(line, DerefArray, CrossLeftSort, 
            megaR,
            Root(line, PushNum("0"))))

        megaReduce(input, findReductions(input)) mustEqual expected
      }
      "left" >> {
        val line = Line(0, "")

        val input = Join(line, Add, CrossLeftSort,
          dag.Reduce(line, Reduction(Vector(), "count", 0x0000), 
            dag.LoadLocal(line, Root(line, PushString("/foo")))),
          dag.Operate(line, Neg, 
            dag.LoadLocal(line, Root(line, PushString("/foo")))))


        val parent = dag.LoadLocal(line, Root(line, PushString("/foo")))
        val red = Reduction(Vector(), "count", 0x0000)
        val megaR = dag.MegaReduce(line, NEL(dag.Reduce(line, red, parent)), parent)

        val expected = Join(line, Add, CrossLeftSort,
          dag.Join(line, DerefArray, CrossLeftSort, 
            megaR,
            Root(line, PushNum("0"))),
          dag.Operate(line, Neg, parent))

        megaReduce(input, findReductions(input)) mustEqual expected
      }
    }

    "where two different sets are being reduced" >> {
      val line = Line(0, "")

      val input = Join(line, Add, CrossRightSort,
        dag.Reduce(line, Reduction(Vector(), "count", 0x0000),
          dag.LoadLocal(line, Root(line, PushString("/foo")))),
        dag.Reduce(line, Reduction(Vector(), "count", 0x0000),
          dag.LoadLocal(line, Root(line, PushString("/bar")))))

      val red = Reduction(Vector(), "count", 0x0000)
      val parent1 = dag.LoadLocal(line, Root(line, PushString("/foo")))
      val parent2 = dag.LoadLocal(line, Root(line, PushString("/bar")))

      val megaR1 = dag.MegaReduce(line, NEL(dag.Reduce(line, red, parent1)), parent1)
      val megaR2 = dag.MegaReduce(line, NEL(dag.Reduce(line, red, parent2)), parent2)

      val expected = Join(line, Add, CrossRightSort,
        Join(line, DerefArray, CrossLeftSort,
          megaR1,
          Root(line, PushNum("0"))),
        Join(line, DerefArray, CrossLeftSort,
          megaR2,
          Root(line, PushNum("0"))))

      megaReduce(input, findReductions(input)) mustEqual expected
    }

    "where a single set is being reduced three times" >> {
      val line = Line(0, "")

      val parent = dag.LoadLocal(line, Root(line, PushString("/foo")))
      val r1 = dag.Reduce(line, Reduction(Vector(), "count", 0x0000), parent)
      val r3 = dag.Reduce(line, Reduction(Vector(), "stdDev", 0x0007), parent)
      
      val input = Join(line, Add, CrossRightSort,
        r1,
        Join(line, Sub, CrossRightSort, r1, r3))

      val megaR = MegaReduce(line, NEL(r1, r1, r3), parent)

      val expected = Join(line, Add, CrossRightSort,
        Join(line, DerefArray, CrossLeftSort,
          megaR,
          Root(line, PushNum("0"))),
        Join(line, Sub, CrossRightSort,
          Join(line, DerefArray, CrossLeftSort,
            megaR,
            Root(line, PushNum("0"))),
          Join(line, DerefArray, CrossLeftSort,
            megaR,
            Root(line, PushNum("2")))))

      megaReduce(input, findReductions(input)) mustEqual expected
    }

    "in a split" >> {
      val line = Line(0, "")
      // 
      // nums := dataset(//hom/numbers)
      // sums('n) :=
      //   m := max(nums where nums < 'n)
      //   (nums where nums = 'n) + m     -- actually, we used split root, but close enough
      // sums
      // 
       
      val nums = dag.LoadLocal(line, Root(line, PushString("/hom/numbers")))
      
      lazy val input: dag.Split = dag.Split(line,
        dag.Group(1, nums, UnfixedSolution(0, nums)),
        Join(line, Add, CrossLeftSort,
          SplitGroup(line, 1, nums.identities)(input),
          dag.Reduce(line, Reduction(Vector(), "max", 0x0001),
            Filter(line, IdentitySort,
              nums,
              Join(line, Lt, CrossLeftSort,
                nums,
                SplitParam(line, 0)(input))))))

      val parent = Filter(line, IdentitySort,
        nums,
        Join(line, Lt, CrossLeftSort,
          nums,
          SplitParam(line, 0)(input)))  //TODO need a window function

      val red = Reduction(Vector(), "max", 0x0001)

      val expected = dag.Split(line,
        dag.Group(1, nums, UnfixedSolution(0, nums)),
        Join(line, Add, CrossLeftSort,
          SplitGroup(line, 1, nums.identities)(input),
          Join(line, DerefArray, CrossLeftSort,
            MegaReduce(line, NEL(dag.Reduce(line, red, parent)), parent),
            Root(line, PushNum("0")))))

      megaReduce(input, findReductions(input)) mustEqual expected
    }

    "in a split that contains two reductions of the same dataset" >> {
      val line = Line(0, "")
      
      // 
      // clicks := dataset(//clicks)
      // histogram('user) :=
      //   { user: 'user, min: min(clicks.foo where clicks.user = 'user), max: max(clicks.foo where clicks.user = 'user) }  
      //  
      //  --if max is taken instead of clicks.bar, the change in the DAG not show up inside the Reduce, and so is hard to track the reductions
      // histogram
      // 
      // 
      
      val clicks = dag.LoadLocal(line, Root(line, PushString("/clicks")))
       
      lazy val input: dag.Split =  dag.Split(line,
        dag.Group(1,
          Join(line, DerefObject, CrossLeftSort, clicks, Root(line, PushString("foo"))),
          UnfixedSolution(0,
            Join(line, DerefObject, CrossLeftSort,
              clicks,
              Root(line, PushString("user"))))),
        Join(line, JoinObject, CrossLeftSort,
          Join(line, WrapObject, CrossLeftSort,
            Root(line, PushString("user")),
            SplitParam(line, 0)(input)),
          Join(line, JoinObject, CrossLeftSort,
            Join(line, WrapObject, CrossLeftSort,
              Root(line, PushString("min")),
              dag.Reduce(line, Reduction(Vector(), "min", 0x0004),
                SplitGroup(line, 1, Vector(LoadIds("/clicks")))(input))),
            Join(line, WrapObject, CrossLeftSort,
              Root(line, PushString("max")),
              dag.Reduce(line, Reduction(Vector(), "max", 0x0001),
                SplitGroup(line, 1, Vector(LoadIds("/clicks")))(input))))))


      val parent = SplitGroup(line, 1, clicks.identities)(input)
      val red1 = dag.Reduce(line, Reduction(Vector(), "min", 0x0004), parent)
      val red2 = dag.Reduce(line, Reduction(Vector(), "max", 0x0001), parent)
      val megaR = MegaReduce(line, NEL(red1, red2), parent)

      val expected = dag.Split(line,
        dag.Group(1,
          Join(line, DerefObject, CrossLeftSort, clicks, Root(line, PushString("foo"))),
          UnfixedSolution(0,
            Join(line, DerefObject, CrossLeftSort,
              clicks,
              Root(line, PushString("user"))))),
        Join(line, JoinObject, CrossLeftSort,
          Join(line, WrapObject, CrossLeftSort,
            Root(line, PushString("user")),
            SplitParam(line, 0)(input)),
          Join(line, JoinObject, CrossLeftSort,
            Join(line, WrapObject, CrossLeftSort,
              Root(line, PushString("min")),
              Join(line, DerefArray, CrossLeftSort,
                megaR,
                Root(line, PushNum("0")))),
            Join(line, WrapObject, CrossLeftSort,
              Root(line, PushString("max")),
              Join(line, DerefArray, CrossLeftSort,
                megaR,
                Root(line, PushNum("1")))))))

      megaReduce(input, findReductions(input)) mustEqual expected
    }
  }

  "reduction finder" should {
    "in a load, find no reductions when there aren't any" >> {
      val line = Line(0, "")

      val input = dag.LoadLocal(line, Root(line, PushString("/foo")))
      val expected = Map.empty[DepGraph, Vector[dag.Reduce]]

      findReductions(input) mustEqual expected
    }

    "in a single reduction" >> {
      val line = Line(0, "")

      val input = dag.Reduce(line, Reduction(Vector(), "count", 0x0000), 
        dag.LoadLocal(line, Root(line, PushString("/foo"))))


      val parent = dag.LoadLocal(line, Root(line, PushString("/foo")))
      val red = Reduction(Vector(), "count", 0x0000)
      val expected = Map(parent -> NEL(dag.Reduce(line, red, parent)))

      findReductions(input) mustEqual expected
    }   

    "in a join of two reductions on the same dataset" >> {
      val line = Line(0, "")

      val input = Join(line, Add, CrossLeftSort, 
        dag.Reduce(line, Reduction(Vector(), "count", 0x0000), 
          dag.LoadLocal(line, Root(line, PushString("/foo")))),
        dag.Reduce(line, Reduction(Vector(), "stdDev", 0x0007),
          dag.LoadLocal(line, Root(line, PushString("/foo")))))


      val parent = dag.LoadLocal(line, Root(line, PushString("/foo")))
      val red1 = Reduction(Vector(), "count", 0x0000)
      val red2 = Reduction(Vector(), "stdDev", 0x0007)
      val expected = Map(parent -> NEL(dag.Reduce(line, red1, parent), dag.Reduce(line, red2, parent)))

      findReductions(input) mustEqual expected
    }

    "in a join where only one side is a reduction" >> {
      "right" >> {
        val line = Line(0, "")

        val input = Join(line, Add, CrossLeftSort,
          dag.Operate(line, Neg, 
            dag.LoadLocal(line, Root(line, PushString("/foo")))),
          dag.Reduce(line, Reduction(Vector(), "stdDev", 0x0007), 
            dag.LoadLocal(line, Root(line, PushString("/foo")))))


        val parent = dag.LoadLocal(line, Root(line, PushString("/foo")))
        val red = Reduction(Vector(), "stdDev", 0x0007)
        val expected = Map(parent -> NEL(dag.Reduce(line, red, parent)))

        findReductions(input) mustEqual expected
      }
      "left" >> {
        val line = Line(0, "")

        val input = Join(line, Add, CrossRightSort,
          dag.Reduce(line, Reduction(Vector(), "count", 0x0000), 
            dag.LoadLocal(line, Root(line, PushString("/foo")))),
          dag.Operate(line, Neg, 
            dag.LoadLocal(line, Root(line, PushString("/foo")))))


        val parent = dag.LoadLocal(line, Root(line, PushString("/foo")))
        val red = Reduction(Vector(), "count", 0x0000)
        val expected = Map(parent -> NEL(dag.Reduce(line, red, parent)))

        findReductions(input) mustEqual expected
      }
    }
    
    "in a split" >> {
      val line = Line(0, "")
      // 
      // nums := dataset(//hom/numbers)
      // sums('n) :=
      //   m := max(nums where nums < 'n)
      //   (nums where nums = 'n) + m     -- actually, we used split root, but close enough
      // sums
      // 
       
      val nums = dag.LoadLocal(line, Root(line, PushString("/hom/numbers")))
      
      lazy val input: dag.Split = dag.Split(line,
        dag.Group(1, nums, UnfixedSolution(0, nums)),
        Join(line, Add, CrossLeftSort,
          SplitGroup(line, 1, nums.identities)(input),
          dag.Reduce(line, Reduction(Vector(), "max", 0x0001),
            Filter(line, IdentitySort,
              nums,
              Join(line, Lt, CrossLeftSort,
                nums,
                SplitParam(line, 0)(input))))))

      val parent = Filter(line, IdentitySort,
        nums,
        Join(line, Lt, CrossLeftSort,
          nums,
          SplitParam(line, 0)(input)))  //TODO should this still require only one pass over /hom/numbers ?
      val red = Reduction(Vector(), "max", 0x0001)
      val expected = Map(parent -> NEL(dag.Reduce(line, red, parent)))

      findReductions(input) mustEqual expected
    }

    "in a split that contains two reductions of the same dataset" >> {
      val line = Line(0, "")
      
      // 
      // clicks := dataset(//clicks)
      // histogram('user) :=
      //   { user: 'user, min: min(clicks.foo where clicks.user = 'user), max: max(clicks.foo where clicks.user = 'user) }  
      //  
      //  --if max is taken instead of clicks.bar, the change in the DAG not show up inside the Reduce, and so is hard to track the reductions
      // histogram
      // 
      // 
      
      val clicks = dag.LoadLocal(line, Root(line, PushString("/clicks")))
       
      lazy val input: dag.Split =  dag.Split(line,
        dag.Group(1,
          Join(line, DerefObject, CrossLeftSort, clicks, Root(line, PushString("foo"))),
          UnfixedSolution(0,
            Join(line, DerefObject, CrossLeftSort,
              clicks,
              Root(line, PushString("user"))))),
        Join(line, JoinObject, CrossLeftSort,
          Join(line, WrapObject, CrossLeftSort,
            Root(line, PushString("user")),
            SplitParam(line, 0)(input)),
          Join(line, JoinObject, CrossLeftSort,
            Join(line, WrapObject, CrossLeftSort,
              Root(line, PushString("min")),
              dag.Reduce(line, Reduction(Vector(), "min", 0x0004),
                SplitGroup(line, 1, Vector(LoadIds("/clicks")))(input))),
            Join(line, WrapObject, CrossLeftSort,
              Root(line, PushString("max")),
              dag.Reduce(line, Reduction(Vector(), "max", 0x0001),
                SplitGroup(line, 1, Vector(LoadIds("/clicks")))(input))))))


      val parent = SplitGroup(line, 1, clicks.identities)(input)
      val red1 = Reduction(Vector(), "min", 0x0004)
      val red2 = Reduction(Vector(), "max", 0x0001)

      val expected = Map(parent -> NEL(dag.Reduce(line, red1, parent), dag.Reduce(line, red2, parent)))

      findReductions(input) mustEqual expected
    }
  }
}
