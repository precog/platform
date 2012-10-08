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
import com.precog.yggdrasil._
import com.precog.common.json._

import scala.collection.mutable

trait ReductionFinderSpecs[M[+_]] extends Specification
    with EvaluatorTestSupport[M]
    with StdLib[M]
    with MathLib[M]
    with ReductionFinder[M] {

  import instructions._
  import dag._

  "mega reduce" should {

    "in a load, rewrite to itself" in {
      val line = Line(0, "")
      val input = dag.LoadLocal(line, Root(line, CString("/foo")))

      megaReduce(input, findReductions(input)) mustEqual input
    }

    "in a reduction of a singleton" in {
      val line = Line(0, "")

      val input = dag.Reduce(line, Count, Root(line, CString("alpha")))
      val megaR = dag.MegaReduce(line, List((trans.Leaf(trans.Source), List(input.red))), Root(line, CString("alpha")))

      val expected = joinDeref(megaR, 0, 0, line)

      megaReduce(input, findReductions(input)) mustEqual expected
    }

    "in a single reduction" in {
      val line = Line(0, "")

      val input = dag.Reduce(line, Count, 
        dag.LoadLocal(line, Root(line, CString("/foo"))))


      val parent = dag.LoadLocal(line, Root(line, CString("/foo")))
      val red = Count 
      val megaR = dag.MegaReduce(line, List((trans.Leaf(trans.Source), List(red))), parent)

      val expected = joinDeref(megaR, 0, 0, line)

      megaReduce(input, findReductions(input)) mustEqual expected
    } 

    "in joins where transpecs are eq, wrap object, operate, filter" in {
      val line = Line(0, "")

      val clicks = dag.LoadLocal(line, Root(line, CString("/clicks")))

      val notEq = Join(line, NotEq, CrossLeftSort,
        Join(line, DerefObject, CrossLeftSort,
          clicks,
          Root(line, CString("foo"))), 
        Root(line, CNum(5)))

      val obj = Join(line, WrapObject, CrossLeftSort,
        Root(line, CString("bar")),
        clicks)

      val op = Operate(line, Neg, 
        Join(line, DerefArray, CrossLeftSort,
          clicks,
          Root(line, CNum(1))))

      val filter = Filter(line, IdentitySort, 
        clicks,
        Join(line, Eq, CrossLeftSort,
          Join(line, DerefObject, CrossLeftSort,
            clicks,
            Root(line, CString("baz"))), 
          Root(line, CNum(12))))

      val nonEqTrans = trans.EqualLiteral(trans.DerefObjectStatic(trans.Leaf(trans.Source), CPathField("foo")), CNum(5), true)
      val objTrans = trans.WrapObject(trans.Leaf(trans.Source), "bar")
      val opTrans = trans.Map1(trans.DerefArrayStatic(trans.Leaf(trans.Source), CPathIndex(1)), op1(Neg).f1)
      val filterTrans = trans.Filter(trans.Leaf(trans.Source), trans.EqualLiteral(trans.DerefObjectStatic(trans.Leaf(trans.Source), CPathField("baz")), CNum(12), false)) 

      val reductions: List[(trans.TransSpec1, List[Reduction])] = List((filterTrans, List(StdDev)), (opTrans, List(Max)), (objTrans, List(Max, Sum)), (nonEqTrans, List(Min))).reverse
      val megaR = MegaReduce(line, reductions, clicks)

      val input = Join(line, Sub, CrossLeftSort,
        dag.Reduce(line, Min, notEq),
        Join(line, Sub, CrossLeftSort,
          dag.Reduce(line, Max, obj),
          Join(line, Sub, CrossLeftSort,
            dag.Reduce(line, Max, op),
            Join(line, Sub, CrossLeftSort,
              dag.Reduce(line, StdDev, filter),
              dag.Reduce(line, Sum, obj)))))

      val expected = Join(line, Sub, CrossLeftSort,
        joinDeref(megaR, 3, 0, line),
        Join(line, Sub, CrossLeftSort,
          joinDeref(megaR, 2, 1, line),
          Join(line, Sub, CrossLeftSort,
            joinDeref(megaR, 1, 0, line),
            Join(line, Sub, CrossLeftSort,
              joinDeref(megaR, 0, 0, line),
              joinDeref(megaR, 2, 0, line)))))

      megaReduce(input, findReductions(input)) mustEqual expected
    }
    
    "in a join of two reductions on the same dataset" in {
      val line = Line(0, "")

      val parent = dag.LoadLocal(line, Root(line, CString("/foo")))
      val red1 = Count
      val red2 = StdDev
      val left = dag.Reduce(line, red1, parent)
      val right = dag.Reduce(line, red2, parent)

      val input = Join(line, Add, CrossLeftSort, left, right)

      val reductions = List((trans.Leaf(trans.Source), List(red1, red2)))
      val megaR = dag.MegaReduce(line, reductions, parent)

      val expected = Join(line, Add, CrossLeftSort,
        joinDeref(megaR, 0, 1, line),
        joinDeref(megaR, 0, 0, line))

      val expectedReductions = MegaReduceState(
        mutable.Map(left -> parent, right -> parent),
        mutable.Map(parent -> List(parent)),
        mutable.Map(parent -> List(left, right)),
        mutable.Map(parent -> trans.Leaf(trans.Source))
      )

      findReductions(input) mustEqual expectedReductions

      megaReduce(input, findReductions(input)) mustEqual expected


    }

    "in a join where only one side is a reduction" in {
      val line = Line(0, "")
      val load = dag.LoadLocal(line, Root(line, CString("/foo")))
      val reduction = StdDev
      val r = dag.Reduce(line, reduction, load)

      val spec = trans.Leaf(trans.Source)
      val megaR = dag.MegaReduce(line, List((spec, List(reduction))), load)

      "right" in {
        val input = Join(line, Add, CrossLeftSort, dag.Operate(line, Neg, load), r)
        val expected = Join(line, Add, CrossLeftSort,
          dag.Operate(line, Neg, load),
          joinDeref(megaR, 0, 0, line))

        megaReduce(input, findReductions(input)) mustEqual expected
      }
      "left" in {
        val input = Join(line, Add, CrossLeftSort, r, dag.Operate(line, Neg, load))
        val expected = Join(line, Add, CrossLeftSort,
          joinDeref(megaR, 0, 0, line),
          dag.Operate(line, Neg, load))

        megaReduce(input, findReductions(input)) mustEqual expected
      }
    }


    "where two different sets are being reduced" in {
      val line = Line(0, "")

      val load1 = dag.LoadLocal(line, Root(line, CString("/foo")))
      val load2 = dag.LoadLocal(line, Root(line, CString("/bar")))

      val red = Count
      val r1 = dag.Reduce(line, red, load1)
      val r2 = dag.Reduce(line, red, load2)

      val input = Join(line, Add, CrossRightSort, r1, r2)
      val spec = trans.Leaf(trans.Source)

      val megaR1 = dag.MegaReduce(line, List((spec, List(red))), load1)
      val megaR2 = dag.MegaReduce(line, List((spec, List(red))), load2)

      val expected = Join(line, Add, CrossRightSort,
        joinDeref(megaR1, 0, 0, line),
        joinDeref(megaR2, 0, 0, line))

      megaReduce(input, findReductions(input)) mustEqual expected
    }    
    
    "where two different sets are being reduced" in {
      val line = Line(0, "")

      val load1 = dag.LoadLocal(line, Root(line, CString("/foo")))
      val load2 = dag.LoadLocal(line, Root(line, CString("/bar")))

      val r1 = dag.Reduce(line, Sum, load1)
      val r2 = dag.Reduce(line, Max, load1)
      val r3 = dag.Reduce(line, Count, load2)
      val r4 = dag.Reduce(line, Max, load2)
      val r5 = dag.Reduce(line, Min, load2)

      val input = Join(line, Add, CrossRightSort, 
        r1, 
        Join(line, Add, CrossRightSort, 
          r2,
          Join(line, Add, CrossRightSort,
            r3,
            Join(line, Add, CrossRightSort,
              r4,
              r5))))
            
      val spec = trans.Leaf(trans.Source)

      val megaR1 = dag.MegaReduce(line, List((spec, List(Sum, Max))), load1)
      val megaR2 = dag.MegaReduce(line, List((spec, List(Count, Max, Min))), load2)

      val expected = Join(line, Add, CrossRightSort,
        joinDeref(megaR1, 0, 1, line),
        Join(line, Add, CrossRightSort,
          joinDeref(megaR1, 0, 0, line),
          Join(line, Add, CrossRightSort,
            joinDeref(megaR2, 0, 2, line),
            Join(line, Add, CrossRightSort,
              joinDeref(megaR2, 0, 1, line),
              joinDeref(megaR2, 0, 0, line)))))

      megaReduce(input, findReductions(input)) mustEqual expected
    }

    "where a single set is being reduced three times" in {
      val line = Line(0, "")
      val load = dag.LoadLocal(line, Root(line, CString("/foo")))
    
      val red1 = Count
      val r1 = dag.Reduce(line, red1, load)
    
      val red3 = StdDev
      val r3 = dag.Reduce(line, red3, load)
      
      val input = Join(line, Add, CrossRightSort, r1, Join(line, Sub, CrossRightSort, r1, r3))
    
      val leaf = trans.Leaf(trans.Source)
      val megaR = MegaReduce(line, List((leaf, List(red1, red3))), load)
    
      val expected = Join(line, Add, CrossRightSort,
        joinDeref(megaR, 0, 1, line),
        Join(line, Sub, CrossRightSort,
          joinDeref(megaR, 0, 1, line), 
          joinDeref(megaR, 0, 0, line))) 

      megaReduce(input, findReductions(input)) mustEqual expected
    }

    "where three reductions use three different trans specs" in {
      import trans._

      val line = Line(0, "")

      val load = dag.LoadLocal(line, Root(line, CString("/hom/heightWeightAcrossSlices")))

      val min = Min
      val max = Max
      val mean = Mean

      val id = Join(line, DerefObject, CrossLeftSort, load, Root(line, CString("userId")))
      val height = Join(line, DerefObject, CrossLeftSort, load, Root(line, CString("height")))
      val weight = Join(line, DerefObject, CrossLeftSort, load, Root(line, CString("weight")))

      val r1 = dag.Reduce(line, min, id)
      val r2 = dag.Reduce(line, max, height)
      val r3 = dag.Reduce(line, mean, weight)

      val input = Join(line, Add, CrossLeftSort, r1, Join(line, Add, CrossLeftSort, r2, r3))

      val mega = dag.MegaReduce(
        line,
        List(
          (DerefObjectStatic(Leaf(Source), CPathField("userId")), List(r1.red)),
          (DerefObjectStatic(Leaf(Source), CPathField("height")), List(r2.red)),
          (DerefObjectStatic(Leaf(Source), CPathField("weight")), List(r3.red))
        ),
        load
      )

      val expected = Join(line, Add, CrossLeftSort,
        joinDeref(mega, 2, 0, line),
        Join(line, Add, CrossLeftSort,
          joinDeref(mega, 1, 0, line),
          joinDeref(mega, 0, 0, line)))

      megaReduce(input, findReductions(input)) mustEqual expected
    }

    "where three reductions use two trans specs" in {
      import trans._

      val line = Line(0, "")

      val load = dag.LoadLocal(line, Root(line, CString("/hom/heightWeightAcrossSlices")))

      val min = Min
      val max = Max
      val mean = Mean

      val height = Join(line, DerefObject, CrossLeftSort, load, Root(line, CString("height")))
      val weight = Join(line, DerefObject, CrossLeftSort, load, Root(line, CString("weight")))

      val r1 = dag.Reduce(line, min, height)
      val r2 = dag.Reduce(line, max, height)
      val r3 = dag.Reduce(line, mean, weight)

      val input = Join(line, Add, CrossLeftSort, r1, Join(line, Add, CrossLeftSort, r2, r3))

      val mega = dag.MegaReduce(
        line,
        List(
          (DerefObjectStatic(Leaf(Source), CPathField("height")), List(r1.red, r2.red)),
          (DerefObjectStatic(Leaf(Source), CPathField("weight")), List(r3.red))
        ),
        load
      )

      val expected = Join(line, Add, CrossLeftSort,
        joinDeref(mega, 1, 1, line),
        Join(line, Add, CrossLeftSort,
          joinDeref(mega, 1, 0, line),
          joinDeref(mega, 0, 0, line)))

      megaReduce(input, findReductions(input)) mustEqual expected
    }

    "where three reductions use one trans spec" in {
      import trans._

      val line = Line(0, "")

      val load = dag.LoadLocal(line, Root(line, CString("/hom/heightWeightAcrossSlices")))

      val min = Min
      val max = Max
      val mean = Mean

      val weight = Join(line, DerefObject, CrossLeftSort, load, Root(line, CString("weight")))

      val r1 = dag.Reduce(line, min, weight)
      val r2 = dag.Reduce(line, max, weight)
      val r3 = dag.Reduce(line, mean, weight)

      val input = Join(line, Add, CrossLeftSort, r1, Join(line, Add, CrossLeftSort, r2, r3))

      val mega = dag.MegaReduce(
        line,
        List(
          (DerefObjectStatic(Leaf(Source), CPathField("weight")), List(r1.red, r2.red, r3.red))
        ),
        load
      )

      val expected = Join(line, Add, CrossLeftSort,
        joinDeref(mega, 0, 2, line),
        Join(line, Add, CrossLeftSort,
          joinDeref(mega, 0, 1, line),
          joinDeref(mega, 0, 0, line)))

      megaReduce(input, findReductions(input)) mustEqual expected
    }

    "where one reduction uses three trans spec" in {
      import trans._

      val line = Line(0, "")

      val load = dag.LoadLocal(line, Root(line, CString("/hom/heightWeightAcrossSlices")))

      val mean = Mean

      val id = Join(line, DerefObject, CrossLeftSort, load, Root(line, CString("userId")))
      val height = Join(line, DerefObject, CrossLeftSort, load, Root(line, CString("height")))
      val weight = Join(line, DerefObject, CrossLeftSort, load, Root(line, CString("weight")))

      val r1 = dag.Reduce(line, mean, id)
      val r2 = dag.Reduce(line, mean, height)
      val r3 = dag.Reduce(line, mean, weight)

      val input = Join(line, Add, CrossLeftSort, r1, Join(line, Add, CrossLeftSort, r2, r3))

      val mega = dag.MegaReduce(
        line,
        List(
          (DerefObjectStatic(Leaf(Source), CPathField("userId")), List(mean)),
          (DerefObjectStatic(Leaf(Source), CPathField("height")), List(mean)),
          (DerefObjectStatic(Leaf(Source), CPathField("weight")), List(mean))
        ),
        load
      )

      val expected = Join(line, Add, CrossLeftSort,
        joinDeref(mega, 2, 0, line),
        Join(line, Add, CrossLeftSort,
          joinDeref(mega, 1, 0, line),
          joinDeref(mega, 0, 0, line)))

      megaReduce(input, findReductions(input)) mustEqual expected
    }

    "in a split" in {
      val line = Line(0, "")
      // 
      // nums := dataset(//hom/numbers)
      // sums('n) :=
      //   m := max(nums where nums < 'n)
      //   (nums where nums = 'n) + m     -- actually, we used split root, but close enough
      // sums
      // 
       
      val nums = dag.LoadLocal(line, Root(line, CString("/hom/numbers")))
      
      val reduction = Max

      lazy val input: dag.Split = dag.Split(line,
        dag.Group(1, nums, UnfixedSolution(0, nums)),
        Join(line, Add, CrossLeftSort,
          SplitGroup(line, 1, nums.identities)(input),
          dag.Reduce(line, reduction, 
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

      val megaR = MegaReduce(line, List((trans.Leaf(trans.Source), List(reduction))), parent)

      val expected = dag.Split(line,
        dag.Group(1, nums, UnfixedSolution(0, nums)),
        Join(line, Add, CrossLeftSort,
          SplitGroup(line, 1, nums.identities)(input),
          joinDeref(megaR, 0, 0, line)))

      megaReduce(input, findReductions(input)) mustEqual expected
    }

    "in a split that contains two reductions of the same dataset" in {
      val line = Line(0, "")
      
      // 
      // clicks := dataset(//clicks)
      // histogram := solve 'user
      //   { user: 'user, min: min(clicks.foo where clicks.user = 'user), max: max(clicks.foo where clicks.user = 'user) }  
      //  
      //  --if max is taken instead of clicks.bar, the change in the DAG not show up inside the Reduce, and so is hard to track the reductions
      // histogram
      
      val clicks = dag.LoadLocal(line, Root(line, CString("/clicks")))
       
      lazy val input: dag.Split = dag.Split(line,
        dag.Group(1,
          Join(line, DerefObject, CrossLeftSort, clicks, Root(line, CString("foo"))),
          UnfixedSolution(0,
            Join(line, DerefObject, CrossLeftSort,
              clicks,
              Root(line, CString("user"))))),
        Join(line, JoinObject, CrossLeftSort,
          Join(line, WrapObject, CrossLeftSort,
            Root(line, CString("user")),
            SplitParam(line, 0)(input)),
          Join(line, JoinObject, CrossLeftSort,
            Join(line, WrapObject, CrossLeftSort,
              Root(line, CString("min")),
              dag.Reduce(line, Min,
                SplitGroup(line, 1, Vector(LoadIds("/clicks")))(input))),
            Join(line, WrapObject, CrossLeftSort,
              Root(line, CString("max")),
              dag.Reduce(line, Max,
                SplitGroup(line, 1, Vector(LoadIds("/clicks")))(input))))))

      val parent = SplitGroup(line, 1, clicks.identities)(input)
      val red1 = dag.Reduce(line, Min, parent)
      val red2 = dag.Reduce(line, Max, parent)
      val megaR = MegaReduce(line, List((trans.Leaf(trans.Source), List(red1.red, red2.red))), parent)

      val expected = dag.Split(line,
        dag.Group(1,
          Join(line, DerefObject, CrossLeftSort, clicks, Root(line, CString("foo"))),
          UnfixedSolution(0,
            Join(line, DerefObject, CrossLeftSort,
              clicks,
              Root(line, CString("user"))))),
        Join(line, JoinObject, CrossLeftSort,
          Join(line, WrapObject, CrossLeftSort,
            Root(line, CString("user")),
            SplitParam(line, 0)(input)),
          Join(line, JoinObject, CrossLeftSort,
            Join(line, WrapObject, CrossLeftSort,
              Root(line, CString("min")),
              joinDeref(megaR, 0, 1, line)),
            Join(line, WrapObject, CrossLeftSort,
              Root(line, CString("max")),
              joinDeref(megaR, 0, 0, line)))))

      megaReduce(input, findReductions(input)) mustEqual expected
    }
  }

  "reduction finder" should {
    "in a load, find no reductions when there aren't any" in {
      val line = Line(0, "")

      val input = dag.LoadLocal(line, Root(line, CString("/foo")))
      val expected = MegaReduceState(
        mutable.Map(),
        mutable.Map(),
        mutable.Map(),
        mutable.Map()
      )

      findReductions(input) mustEqual expected
    }

    "in a single reduction" in {
      val line = Line(0, "")

      val load = dag.LoadLocal(line, Root(line, CString("/foo")))
      val reduction = Count
      val r = dag.Reduce(line, reduction, load)

      val expected = MegaReduceState(
        mutable.Map(r -> load),
        mutable.Map(load -> List(load)),
        mutable.Map(load -> List(r)),
        mutable.Map(load -> trans.Leaf(trans.Source))
      )

      findReductions(r) mustEqual expected
    }   

    "in a join of two reductions on the same dataset #2" in {
      val line = Line(0, "")

      val load = dag.LoadLocal(line, Root(line, CString("/foo")))
      val r1 = dag.Reduce(line, Count, load)
      val r2 = dag.Reduce(line, StdDev, load)

      val input = Join(line, Add, CrossLeftSort, r1, r2)

      val expected = MegaReduceState(
        mutable.Map(r1 -> load, r2 -> load),
        mutable.Map(load -> List(load)),
        mutable.Map(load -> List(r1, r2)),
        mutable.Map(load -> trans.Leaf(trans.Source))
      )

      findReductions(input) mustEqual expected
    }

    "findReductions given a reduction inside a reduction" in {
      val line = Line(0, "")

      val load = dag.LoadLocal(line, Root(line, CString("/foo")))
      val r1 = dag.Reduce(line, Mean, load)
      val r2 = dag.Reduce(line, Count, r1)

      val expected = MegaReduceState(
        mutable.Map(r2 -> r1, r1 -> load),
        mutable.Map(load -> List(load), r1 -> List(r1)),
        mutable.Map(load -> List(r1), r1 -> List(r2)),
        mutable.Map(load -> trans.Leaf(trans.Source), r1 -> trans.Leaf(trans.Source))
      )

      findReductions(r2) mustEqual expected
    }     

    "findReductions given two reductions inside a reduction" in {
      val line = Line(0, "")

      val foo = dag.LoadLocal(line, Root(line, CString("/foo")))
      val mean = dag.Reduce(line, Mean, foo)
      val stdDev = dag.Reduce(line, StdDev, foo)
      val parentCount = dag.Join(line, Add, CrossLeftSort, mean, stdDev)

      val input = dag.Reduce(line, Count, parentCount)

      val expected = MegaReduceState(
        mutable.Map(
          mean -> foo,
          stdDev -> foo,
          input -> parentCount
        ),
        mutable.Map(
          foo -> List(foo),
          parentCount -> List(parentCount)
        ),
        mutable.Map(
          foo -> List(mean, stdDev),
          parentCount -> List(input)
        ),
        mutable.Map(
          foo -> trans.Leaf(trans.Source),
          parentCount -> trans.Leaf(trans.Source)
        )
      )

      findReductions(input) mustEqual expected
    }

    // TODO: need to test reductions whose parents are splits
    "findReductions inside a Split" in {
      val line = Line(0, "")

      val clicks = dag.LoadLocal(line, Root(line, CString("/clicks")))
      val red = Count
      val count = dag.Reduce(line, red, clicks)

      lazy val input: dag.Split = dag.Split(line,
        dag.Group(1,
        clicks,
        UnfixedSolution(0, count)),
        SplitParam(line, 1)(input))
        
      val expected = MegaReduceState(
        mutable.Map(count -> clicks),
        mutable.Map(clicks -> List(clicks)),
        mutable.Map(clicks -> List(count)),
        mutable.Map(clicks -> trans.Leaf(trans.Source))
      )

      findReductions(input) mustEqual expected
    }


    "in a join where only one side is a reduction" in {
      val line = Line(0, "")
      val load = dag.LoadLocal(line, Root(line, CString("/foo")))

      "right" in {
        val r = dag.Reduce(line, StdDev, load)
        val input = Join(line, Add, CrossLeftSort, dag.Operate(line, Neg, load), r)

        val expected = MegaReduceState(
          mutable.Map(r -> load),
          mutable.Map(load -> List(load)),
          mutable.Map(load -> List(r)),
          mutable.Map(load -> trans.Leaf(trans.Source))
        )

        findReductions(input) mustEqual expected
      }
      "left" in {
        val r = dag.Reduce(line, Count, load)
        val input = Join(line, Add, CrossRightSort, r, dag.Operate(line, Neg, load))

        val expected = MegaReduceState(
          mutable.Map(r -> load),
          mutable.Map(load -> List(load)),
          mutable.Map(load -> List(r)),
          mutable.Map(load -> trans.Leaf(trans.Source))
        )

        findReductions(input) mustEqual expected
      }
    }
    
    "in a split" in {
      val line = Line(0, "")

      // nums := dataset(//hom/numbers)
      // sums('n) :=
      //   m := max(nums where nums < 'n)
      //   (nums where nums = 'n) + m     -- actually, we used split root, but close enough
      // sums

      val nums = dag.LoadLocal(line, Root(line, CString("/hom/numbers")))

      lazy val j = Join(line, Lt, CrossLeftSort, nums, SplitParam(line, 0)(input))
      lazy val parent = Filter(line, IdentitySort, nums, j)

      lazy val splitGroup = SplitGroup(line, 1, nums.identities)(input)
      lazy val r = dag.Reduce(line, Max, parent)

      lazy val group = dag.Group(1, nums, UnfixedSolution(0, nums))
      lazy val join = Join(line, Add, CrossLeftSort, splitGroup, r)
      lazy val input: dag.Split = dag.Split(line, group, join)

      lazy val expected = MegaReduceState(
        mutable.Map(r -> parent),
        mutable.Map(parent -> List(parent)),
        mutable.Map(parent -> List(r)),
        mutable.Map(parent -> trans.Leaf(trans.Source))
      )

      findReductions(input) mustEqual expected
    }

    "in a split that contains two reductions of the same dataset #2" in {
      val line = Line(0, "")
      
      // clicks := dataset(//clicks)
      // histogram('user) :=
      //   { user: 'user,
      //     min: min(clicks.foo where clicks.user = 'user),
      //     max: max(clicks.foo where clicks.user = 'user) }
      // histogram
      //  
      // -- if max is taken instead of clicks.bar, the change in the DAG does
      // -- not show up inside the Reduce, so it's hard to track the reductions
      
      val clicks = dag.LoadLocal(line, Root(line, CString("/clicks")))

      val fooRoot = Root(line, CString("foo"))
      val userRoot = Root(line, CString("user"))
      val minRoot = Root(line, CString("min"))
      val maxRoot = Root(line, CString("max"))

      val clicksFoo = Join(line, DerefObject, CrossLeftSort, clicks, fooRoot)
      val clicksUser = Join(line, DerefObject, CrossLeftSort, clicks, userRoot)
      val group1 = dag.Group(1, clicksFoo, UnfixedSolution(0, clicksUser))

      lazy val parent = SplitGroup(line, 1, Vector(LoadIds("/clicks")))(input)
      lazy val r1 = dag.Reduce(line, Min, parent)
      lazy val r2 = dag.Reduce(line, Max, parent)

      lazy val input: dag.Split = dag.Split(line,
        group1,
        Join(line, JoinObject, CrossLeftSort,
          Join(line, WrapObject, CrossLeftSort,
            userRoot,
            SplitParam(line, 0)(input)),
          Join(line, JoinObject, CrossLeftSort,
            Join(line, WrapObject, CrossLeftSort, minRoot, r1),
            Join(line, WrapObject, CrossLeftSort, maxRoot, r2))))

      val expected = MegaReduceState(
        mutable.Map(r1 -> parent, r2 -> parent),
        mutable.Map(parent -> List(parent)),
        mutable.Map(parent -> List(r1, r2)),
        mutable.Map(parent -> trans.Leaf(trans.Source))
      )

      findReductions(input) mustEqual expected
    }
  }

  def joinDeref(left: DepGraph, first: Int, second: Int, line: Line): DepGraph = 
    Join(line, DerefArray, CrossLeftSort,
      Join(line, DerefArray, CrossLeftSort,
        left,
        Root(line, CLong(first))),
      Root(line, CLong(second)))
}


object ReductionFinderSpecs extends ReductionFinderSpecs[test.YId] with test.YIdInstances
