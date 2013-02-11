package com.precog
package daze

import com.precog.common._
import bytecode._
import org.specs2.mutable._
import com.precog.yggdrasil._
import com.precog.common.json._

import scala.collection.mutable

trait ReductionFinderSpecs[M[+_]] extends Specification
    with EvaluatorTestSupport[M] {

  import instructions._
  import dag._
  import library._

  val ctx = defaultEvaluationContext

  "mega reduce" should {

    "in a load, rewrite to itself" in {
      val line = Line(1, 1, "")
      val input = dag.LoadLocal(Const(CString("/foo"))(line))(line)

      megaReduce(input, findReductions(input, ctx)) mustEqual input
    }

    "in a reduction of a singleton" in {
      val line = Line(1, 1, "")

      val input = dag.Reduce(Count, Const(CString("alpha"))(line))(line)
      val megaR = dag.MegaReduce(List((trans.Leaf(trans.Source), List(input.red))), Const(CString("alpha"))(line))

      val expected = joinDeref(megaR, 0, 0, line)

      megaReduce(input, findReductions(input, ctx)) mustEqual expected
    }

    "in a single reduction" in {
      val line = Line(1, 1, "")

      val input = dag.Reduce(Count, 
        dag.LoadLocal(Const(CString("/foo"))(line))(line))(line)


      val parent = dag.LoadLocal(Const(CString("/foo"))(line))(line)
      val red = Count
      val megaR = dag.MegaReduce(List((trans.Leaf(trans.Source), List(red))), parent)

      val expected = joinDeref(megaR, 0, 0, line)

      megaReduce(input, findReductions(input, ctx)) mustEqual expected
    } 

    "in joins where transpecs are eq, wrap object, operate, filter" in {
      val line = Line(1, 1, "")

      val clicks = dag.LoadLocal(Const(CString("/clicks"))(line))(line)

      val notEq = Join(NotEq, CrossLeftSort,
        Join(DerefObject, CrossLeftSort,
          clicks,
          Const(CString("foo"))(line))(line), 
        Const(CNum(5))(line))(line)

      val obj = Join(WrapObject, CrossLeftSort,
        Const(CString("bar"))(line),
        clicks)(line)

      val op = Operate(Neg, 
        Join(DerefArray, CrossLeftSort,
          clicks,
          Const(CNum(1))(line))(line))(line)

      val filter = Filter(IdentitySort, 
        clicks,
        Join(Eq, CrossLeftSort,
          Join(DerefObject, CrossLeftSort,
            clicks,
            Const(CString("baz"))(line))(line), 
          Const(CNum(12))(line))(line))(line)

      val fooDerefTrans = trans.DerefObjectStatic(trans.Leaf(trans.Source), CPathField("foo"))
      val nonEqTrans = trans.Map1(trans.Equal(fooDerefTrans, trans.ConstLiteral(CNum(5), fooDerefTrans)), Unary.Comp.f1(ctx))
      val objTrans = trans.WrapObject(trans.Leaf(trans.Source), "bar")
      val opTrans = op1ForUnOp(Neg).spec(ctx)(trans.DerefArrayStatic(trans.Leaf(trans.Source), CPathIndex(1)))
      val bazDerefTrans = trans.DerefObjectStatic(trans.Leaf(trans.Source), CPathField("baz"))
      val filterTrans = trans.Filter(trans.Leaf(trans.Source), trans.Equal(bazDerefTrans, trans.ConstLiteral(CNum(12), bazDerefTrans)))

      val reductions: List[(trans.TransSpec1, List[Reduction])] = List((filterTrans, List(StdDev)), (opTrans, List(Max)), (objTrans, List(Max, Sum)), (nonEqTrans, List(Min))).reverse
      val megaR = MegaReduce(reductions, clicks)

      val input = Join(Sub, CrossLeftSort,
        dag.Reduce(Min, notEq)(line),
        Join(Sub, CrossLeftSort,
          dag.Reduce(Max, obj)(line),
          Join(Sub, CrossLeftSort,
            dag.Reduce(Max, op)(line),
            Join(Sub, CrossLeftSort,
              dag.Reduce(StdDev, filter)(line),
              dag.Reduce(Sum, obj)(line))(line))(line))(line))(line)

      val expected = Join(Sub, CrossLeftSort,
        joinDeref(megaR, 3, 0, line),
        Join(Sub, CrossLeftSort,
          joinDeref(megaR, 2, 1, line),
          Join(Sub, CrossLeftSort,
            joinDeref(megaR, 1, 0, line),
            Join(Sub, CrossLeftSort,
              joinDeref(megaR, 0, 0, line),
              joinDeref(megaR, 2, 0, line))(line))(line))(line))(line)

      megaReduce(input, findReductions(input, ctx)) mustEqual expected
    }
    
    "in a join of two reductions on the same dataset" in {
      val line = Line(1, 1, "")

      val parent = dag.LoadLocal(Const(CString("/foo"))(line))(line)
      val red1 = Count
      val red2 = StdDev
      val left = dag.Reduce(red1, parent)(line)
      val right = dag.Reduce(red2, parent)(line)

      val input = Join(Add, CrossLeftSort, left, right)(line)

      val reductions = List((trans.Leaf(trans.Source), List(red1, red2)))
      val megaR = dag.MegaReduce(reductions, parent)

      val expected = Join(Add, CrossLeftSort,
        joinDeref(megaR, 0, 1, line),
        joinDeref(megaR, 0, 0, line))(line)

      val expectedReductions = MegaReduceState(
        Map(left -> parent, right -> parent),
        Map(parent -> List(parent)),
        Map(parent -> List(left, right)),
        Map(parent -> trans.Leaf(trans.Source))
      )

      findReductions(input, ctx) mustEqual expectedReductions

      megaReduce(input, findReductions(input, ctx)) mustEqual expected


    }

    "in a join where only one side is a reduction" in {
      val line = Line(1, 1, "")
      val load = dag.LoadLocal(Const(CString("/foo"))(line))(line)
      val reduction = StdDev
      val r = dag.Reduce(reduction, load)(line)

      val spec = trans.Leaf(trans.Source)
      val megaR = dag.MegaReduce(List((spec, List(reduction))), load)

      "right" in {
        val input = Join(Add, CrossLeftSort, dag.Operate(Neg, load)(line), r)(line)
        val expected = Join(Add, CrossLeftSort,
          dag.Operate(Neg, load)(line),
          joinDeref(megaR, 0, 0, line))(line)

        megaReduce(input, findReductions(input, ctx)) mustEqual expected
      }
      "left" in {
        val input = Join(Add, CrossLeftSort, r, dag.Operate(Neg, load)(line))(line)
        val expected = Join(Add, CrossLeftSort,
          joinDeref(megaR, 0, 0, line),
          dag.Operate(Neg, load)(line))(line)

        megaReduce(input, findReductions(input, ctx)) mustEqual expected
      }
    }


    "where two different sets are being reduced" in {
      val line = Line(1, 1, "")

      val load1 = dag.LoadLocal(Const(CString("/foo"))(line))(line)
      val load2 = dag.LoadLocal(Const(CString("/bar"))(line))(line)

      val red = Count
      val r1 = dag.Reduce(red, load1)(line)
      val r2 = dag.Reduce(red, load2)(line)

      val input = Join(Add, CrossRightSort, r1, r2)(line)
      val spec = trans.Leaf(trans.Source)

      val megaR1 = dag.MegaReduce(List((spec, List(red))), load1)
      val megaR2 = dag.MegaReduce(List((spec, List(red))), load2)

      val expected = Join(Add, CrossRightSort,
        joinDeref(megaR1, 0, 0, line),
        joinDeref(megaR2, 0, 0, line))(line)

      megaReduce(input, findReductions(input, ctx)) mustEqual expected
    }    
    
    "where two different sets are being reduced" in {
      val line = Line(1, 1, "")

      val load1 = dag.LoadLocal(Const(CString("/foo"))(line))(line)
      val load2 = dag.LoadLocal(Const(CString("/bar"))(line))(line)

      val r1 = dag.Reduce(Sum, load1)(line)
      val r2 = dag.Reduce(Max, load1)(line)
      val r3 = dag.Reduce(Count, load2)(line)
      val r4 = dag.Reduce(Max, load2)(line)
      val r5 = dag.Reduce(Min, load2)(line)

      val input = Join(Add, CrossRightSort, 
        r1, 
        Join(Add, CrossRightSort, 
          r2,
          Join(Add, CrossRightSort,
            r3,
            Join(Add, CrossRightSort,
              r4,
              r5)(line))(line))(line))(line)
            
      val spec = trans.Leaf(trans.Source)

      val megaR1 = dag.MegaReduce(List((spec, List(Sum, Max))), load1)
      val megaR2 = dag.MegaReduce(List((spec, List(Count, Max, Min))), load2)

      val expected = Join(Add, CrossRightSort,
        joinDeref(megaR1, 0, 1, line),
        Join(Add, CrossRightSort,
          joinDeref(megaR1, 0, 0, line),
          Join(Add, CrossRightSort,
            joinDeref(megaR2, 0, 2, line),
            Join(Add, CrossRightSort,
              joinDeref(megaR2, 0, 1, line),
              joinDeref(megaR2, 0, 0, line))(line))(line))(line))(line)

      megaReduce(input, findReductions(input, ctx)) mustEqual expected
    }

    "where a single set is being reduced three times" in {
      val line = Line(1, 1, "")
      val load = dag.LoadLocal(Const(CString("/foo"))(line))(line)
    
      val red1 = Count
      val r1 = dag.Reduce(red1, load)(line)
    
      val red3 = StdDev
      val r3 = dag.Reduce(red3, load)(line)
      
      val input = Join(Add, CrossRightSort, r1, Join(Sub, CrossRightSort, r1, r3)(line))(line)
    
      val leaf = trans.Leaf(trans.Source)
      val megaR = MegaReduce(List((leaf, List(red1, red3))), load)
    
      val expected = Join(Add, CrossRightSort,
        joinDeref(megaR, 0, 1, line),
        Join(Sub, CrossRightSort,
          joinDeref(megaR, 0, 1, line), 
          joinDeref(megaR, 0, 0, line))(line))(line) 

      megaReduce(input, findReductions(input, ctx)) mustEqual expected
    }

    "where three reductions use three different trans specs" in {
      import trans._

      val line = Line(1, 1, "")

      val load = dag.LoadLocal(Const(CString("/hom/heightWeightAcrossSlices"))(line))(line)

      val min = Min
      val max = Max
      val mean = Mean

      val id = Join(DerefObject, CrossLeftSort, load, Const(CString("userId"))(line))(line)
      val height = Join(DerefObject, CrossLeftSort, load, Const(CString("height"))(line))(line)
      val weight = Join(DerefObject, CrossLeftSort, load, Const(CString("weight"))(line))(line)

      val r1 = dag.Reduce(min, id)(line)
      val r2 = dag.Reduce(max, height)(line)
      val r3 = dag.Reduce(mean, weight)(line)

      val input = Join(Add, CrossLeftSort, r1, Join(Add, CrossLeftSort, r2, r3)(line))(line)

      val mega = dag.MegaReduce(
        List(
          (DerefObjectStatic(Leaf(Source), CPathField("userId")), List(r1.red)),
          (DerefObjectStatic(Leaf(Source), CPathField("height")), List(r2.red)),
          (DerefObjectStatic(Leaf(Source), CPathField("weight")), List(r3.red))
        ),
        load
      )

      val expected = Join(Add, CrossLeftSort,
        joinDeref(mega, 2, 0, line),
        Join(Add, CrossLeftSort,
          joinDeref(mega, 1, 0, line),
          joinDeref(mega, 0, 0, line))(line))(line)

      megaReduce(input, findReductions(input, ctx)) mustEqual expected
    }

    "where three reductions use two trans specs" in {
      import trans._

      val line = Line(1, 1, "")

      val load = dag.LoadLocal(Const(CString("/hom/heightWeightAcrossSlices"))(line))(line)

      val min = Min
      val max = Max
      val mean = Mean

      val height = Join(DerefObject, CrossLeftSort, load, Const(CString("height"))(line))(line)
      val weight = Join(DerefObject, CrossLeftSort, load, Const(CString("weight"))(line))(line)

      val r1 = dag.Reduce(min, height)(line)
      val r2 = dag.Reduce(max, height)(line)
      val r3 = dag.Reduce(mean, weight)(line)

      val input = Join(Add, CrossLeftSort, r1, Join(Add, CrossLeftSort, r2, r3)(line))(line)

      val mega = dag.MegaReduce(
        List(
          (DerefObjectStatic(Leaf(Source), CPathField("height")), List(r1.red, r2.red)),
          (DerefObjectStatic(Leaf(Source), CPathField("weight")), List(r3.red))
        ),
        load
      )

      val expected = Join(Add, CrossLeftSort,
        joinDeref(mega, 1, 1, line),
        Join(Add, CrossLeftSort,
          joinDeref(mega, 1, 0, line),
          joinDeref(mega, 0, 0, line))(line))(line)

      megaReduce(input, findReductions(input, ctx)) mustEqual expected
    }

    "where three reductions use one trans spec" in {
      import trans._

      val line = Line(1, 1, "")

      val load = dag.LoadLocal(Const(CString("/hom/heightWeightAcrossSlices"))(line))(line)

      val min = Min
      val max = Max
      val mean = Mean

      val weight = Join(DerefObject, CrossLeftSort, load, Const(CString("weight"))(line))(line)

      val r1 = dag.Reduce(min, weight)(line)
      val r2 = dag.Reduce(max, weight)(line)
      val r3 = dag.Reduce(mean, weight)(line)

      val input = Join(Add, CrossLeftSort, r1, Join(Add, CrossLeftSort, r2, r3)(line))(line)

      val mega = dag.MegaReduce(
        List(
          (DerefObjectStatic(Leaf(Source), CPathField("weight")), List(r1.red, r2.red, r3.red))
        ),
        load
      )

      val expected = Join(Add, CrossLeftSort,
        joinDeref(mega, 0, 2, line),
        Join(Add, CrossLeftSort,
          joinDeref(mega, 0, 1, line),
          joinDeref(mega, 0, 0, line))(line))(line)

      megaReduce(input, findReductions(input, ctx)) mustEqual expected
    }

    "where one reduction uses three trans spec" in {
      import trans._

      val line = Line(1, 1, "")

      val load = dag.LoadLocal(Const(CString("/hom/heightWeightAcrossSlices"))(line))(line)

      val mean = Mean

      val id = Join(DerefObject, CrossLeftSort, load, Const(CString("userId"))(line))(line)
      val height = Join(DerefObject, CrossLeftSort, load, Const(CString("height"))(line))(line)
      val weight = Join(DerefObject, CrossLeftSort, load, Const(CString("weight"))(line))(line)

      val r1 = dag.Reduce(mean, id)(line)
      val r2 = dag.Reduce(mean, height)(line)
      val r3 = dag.Reduce(mean, weight)(line)

      val input = Join(Add, CrossLeftSort, r1, Join(Add, CrossLeftSort, r2, r3)(line))(line)

      val mega = dag.MegaReduce(
        List(
          (DerefObjectStatic(Leaf(Source), CPathField("userId")), List(mean)),
          (DerefObjectStatic(Leaf(Source), CPathField("height")), List(mean)),
          (DerefObjectStatic(Leaf(Source), CPathField("weight")), List(mean))
        ),
        load
      )

      val expected = Join(Add, CrossLeftSort,
        joinDeref(mega, 2, 0, line),
        Join(Add, CrossLeftSort,
          joinDeref(mega, 1, 0, line),
          joinDeref(mega, 0, 0, line))(line))(line)

      megaReduce(input, findReductions(input, ctx)) mustEqual expected
    }

    "in a split" in {
      val line = Line(1, 1, "")
      // 
      // nums := dataset(//hom/numbers)
      // sums('n) :=
      //   m := max(nums where nums < 'n)
      //   (nums where nums = 'n) + m     -- actually, we used split root, but close enough
      // sums
      // 
       
      val nums = dag.LoadLocal(Const(CString("/hom/numbers"))(line))(line)
      
      val reduction = Max

      lazy val input: dag.Split = dag.Split(
        dag.Group(1, nums, UnfixedSolution(0, nums)),
        Join(Add, CrossLeftSort,
          SplitGroup(1, nums.identities)(input)(line),
          dag.Reduce(reduction, 
            Filter(IdentitySort,
              nums,
              Join(Lt, CrossLeftSort,
                nums,
                SplitParam(0)(input)(line))(line))(line))(line))(line))(line)

      val parent = Filter(IdentitySort,
        nums,
        Join(Lt, CrossLeftSort,
          nums,
          SplitParam(0)(input)(line))(line))(line)  //TODO need a window function

      val megaR = MegaReduce(List((trans.Leaf(trans.Source), List(reduction))), parent)

      val expected = dag.Split(
        dag.Group(1, nums, UnfixedSolution(0, nums)),
        Join(Add, CrossLeftSort,
          SplitGroup(1, nums.identities)(input)(line),
          joinDeref(megaR, 0, 0, line))(line))(line)

      megaReduce(input, findReductions(input, ctx)) mustEqual expected
    }

    "in a split that contains two reductions of the same dataset" in {
      val line = Line(1, 1, "")
      
      // 
      // clicks := dataset(//clicks)
      // histogram := solve 'user
      //   { user: 'user, min: min(clicks.foo where clicks.user = 'user), max: max(clicks.foo where clicks.user = 'user) }  
      //  
      //  --if max is taken instead of clicks.bar, the change in the DAG not show up inside the Reduce, and so is hard to track the reductions
      // histogram
      
      val clicks = dag.LoadLocal(Const(CString("/clicks"))(line))(line)
       
      lazy val input: dag.Split = dag.Split(
        dag.Group(1,
          Join(DerefObject, CrossLeftSort, clicks, Const(CString("foo"))(line))(line),
          UnfixedSolution(0,
            Join(DerefObject, CrossLeftSort,
              clicks,
              Const(CString("user"))(line))(line))),
        Join(JoinObject, CrossLeftSort,
          Join(WrapObject, CrossLeftSort,
            Const(CString("user"))(line),
            SplitParam(0)(input)(line))(line),
          Join(JoinObject, CrossLeftSort,
            Join(WrapObject, CrossLeftSort,
              Const(CString("min"))(line),
              dag.Reduce(Min,
                SplitGroup(1, Identities.Specs(Vector(LoadIds("/clicks"))))(input)(line))(line))(line),
            Join(WrapObject, CrossLeftSort,
              Const(CString("max"))(line),
              dag.Reduce(Max,
                SplitGroup(1, Identities.Specs(Vector(LoadIds("/clicks"))))(input)(line))(line))(line))(line))(line))(line)

      val parent = SplitGroup(1, clicks.identities)(input)(line)
      val red1 = dag.Reduce(Min, parent)(line)
      val red2 = dag.Reduce(Max, parent)(line)
      val megaR = MegaReduce(List((trans.Leaf(trans.Source), List(red1.red, red2.red))), parent)

      val expected = dag.Split(
        dag.Group(1,
          Join(DerefObject, CrossLeftSort, clicks, Const(CString("foo"))(line))(line),
          UnfixedSolution(0,
            Join(DerefObject, CrossLeftSort,
              clicks,
              Const(CString("user"))(line))(line))),
        Join(JoinObject, CrossLeftSort,
          Join(WrapObject, CrossLeftSort,
            Const(CString("user"))(line),
            SplitParam(0)(input)(line))(line),
          Join(JoinObject, CrossLeftSort,
            Join(WrapObject, CrossLeftSort,
              Const(CString("min"))(line),
              joinDeref(megaR, 0, 1, line))(line),
            Join(WrapObject, CrossLeftSort,
              Const(CString("max"))(line),
              joinDeref(megaR, 0, 0, line))(line))(line))(line))(line)

      megaReduce(input, findReductions(input, ctx)) mustEqual expected
    }
  }

  "reduction finder" should {
    "in a load, find no reductions when there aren't any" in {
      val line = Line(1, 1, "")

      val input = dag.LoadLocal(Const(CString("/foo"))(line))(line)
      val expected = MegaReduceState(
        Map(),
        Map(),
        Map(),
        Map()
      )

      findReductions(input, ctx) mustEqual expected
    }

    "in a single reduction" in {
      val line = Line(1, 1, "")

      val load = dag.LoadLocal(Const(CString("/foo"))(line))(line)
      val reduction = Count
      val r = dag.Reduce(reduction, load)(line)

      val expected = MegaReduceState(
        Map(r -> load),
        Map(load -> List(load)),
        Map(load -> List(r)),
        Map(load -> trans.Leaf(trans.Source))
      )

      findReductions(r, ctx) mustEqual expected
    }   

    "in a join of two reductions on the same dataset #2" in {
      val line = Line(1, 1, "")

      val load = dag.LoadLocal(Const(CString("/foo"))(line))(line)
      val r1 = dag.Reduce(Count, load)(line)
      val r2 = dag.Reduce(StdDev, load)(line)

      val input = Join(Add, CrossLeftSort, r1, r2)(line)

      val expected = MegaReduceState(
        Map(r1 -> load, r2 -> load),
        Map(load -> List(load)),
        Map(load -> List(r1, r2)),
        Map(load -> trans.Leaf(trans.Source))
      )

      findReductions(input, ctx) mustEqual expected
    }

    "findReductions given a reduction inside a reduction" in {
      val line = Line(1, 1, "")

      val load = dag.LoadLocal(Const(CString("/foo"))(line))(line)
      val r1 = dag.Reduce(Mean, load)(line)
      val r2 = dag.Reduce(Count, r1)(line)

      val expected = MegaReduceState(
        Map(r2 -> r1, r1 -> load),
        Map(load -> List(load), r1 -> List(r1)),
        Map(load -> List(r1), r1 -> List(r2)),
        Map(load -> trans.Leaf(trans.Source), r1 -> trans.Leaf(trans.Source))
      )

      findReductions(r2, ctx) mustEqual expected
    }     

    "findReductions given two reductions inside a reduction" in {
      val line = Line(1, 1, "")

      val foo = dag.LoadLocal(Const(CString("/foo"))(line))(line)
      val mean = dag.Reduce(Mean, foo)(line)
      val stdDev = dag.Reduce(StdDev, foo)(line)
      val parentCount = dag.Join(Add, CrossLeftSort, mean, stdDev)(line)

      val input = dag.Reduce(Count, parentCount)(line)

      val expected = MegaReduceState(
        Map(
          mean -> foo,
          stdDev -> foo,
          input -> parentCount
        ),
        Map(
          foo -> List(foo),
          parentCount -> List(parentCount)
        ),
        Map(
          foo -> List(mean, stdDev),
          parentCount -> List(input)
        ),
        Map(
          foo -> trans.Leaf(trans.Source),
          parentCount -> trans.Leaf(trans.Source)
        )
      )

      findReductions(input, ctx) mustEqual expected
    }

    // TODO: need to test reductions whose parents are splits
    "findReductions inside a Split" in {
      val line = Line(1, 1, "")

      val clicks = dag.LoadLocal(Const(CString("/clicks"))(line))(line)
      val red = Count
      val count = dag.Reduce(red, clicks)(line)

      lazy val input: dag.Split = dag.Split(
        dag.Group(1,
          clicks,
          UnfixedSolution(0, count)),
        SplitParam(1)(input)(line))(line)
        
      val expected = MegaReduceState(
        Map(count -> clicks),
        Map(clicks -> List(clicks)),
        Map(clicks -> List(count)),
        Map(clicks -> trans.Leaf(trans.Source))
      )

      findReductions(input, ctx) mustEqual expected
    }


    "in a join where only one side is a reduction" in {
      val line = Line(1, 1, "")
      val load = dag.LoadLocal(Const(CString("/foo"))(line))(line)

      "right" in {
        val r = dag.Reduce(StdDev, load)(line)
        val input = Join(Add, CrossLeftSort, dag.Operate(Neg, load)(line), r)(line)

        val expected = MegaReduceState(
          Map(r -> load),
          Map(load -> List(load)),
          Map(load -> List(r)),
          Map(load -> trans.Leaf(trans.Source))
        )

        findReductions(input, ctx) mustEqual expected
      }
      "left" in {
        val r = dag.Reduce(Count, load)(line)
        val input = Join(Add, CrossRightSort, r, dag.Operate(Neg, load)(line))(line)

        val expected = MegaReduceState(
          Map(r -> load),
          Map(load -> List(load)),
          Map(load -> List(r)),
          Map(load -> trans.Leaf(trans.Source))
        )

        findReductions(input, ctx) mustEqual expected
      }
    }
    
    "in a split" in {
      val line = Line(1, 1, "")

      // nums := dataset(//hom/numbers)
      // sums('n) :=
      //   m := max(nums where nums < 'n)
      //   (nums where nums = 'n) + m     -- actually, we used split root, but close enough
      // sums

      val nums = dag.LoadLocal(Const(CString("/hom/numbers"))(line))(line)

      lazy val j = Join(Lt, CrossLeftSort, nums, SplitParam(0)(input)(line))(line)
      lazy val parent = Filter(IdentitySort, nums, j)(line)

      lazy val splitGroup = SplitGroup(1, nums.identities)(input)(line)
      lazy val r = dag.Reduce(Max, parent)(line)

      lazy val group = dag.Group(1, nums, UnfixedSolution(0, nums))
      lazy val join = Join(Add, CrossLeftSort, splitGroup, r)(line)
      lazy val input: dag.Split = dag.Split(group, join)(line)

      lazy val expected = MegaReduceState(
        Map(r -> parent),
        Map(parent -> List(parent)),
        Map(parent -> List(r)),
        Map(parent -> trans.Leaf(trans.Source))
      )

      findReductions(input, ctx) mustEqual expected
    }

    "in a split that contains two reductions of the same dataset #2" in {
      val line = Line(1, 1, "")
      
      // clicks := dataset(//clicks)
      // histogram('user) :=
      //   { user: 'user,
      //     min: min(clicks.foo where clicks.user = 'user),
      //     max: max(clicks.foo where clicks.user = 'user) }
      // histogram
      //  
      // -- if max is taken instead of clicks.bar, the change in the DAG does
      // -- not show up inside the Reduce, so it's hard to track the reductions
      
      val clicks = dag.LoadLocal(Const(CString("/clicks"))(line))(line)

      val fooRoot = Const(CString("foo"))(line)
      val userRoot = Const(CString("user"))(line)
      val minRoot = Const(CString("min"))(line)
      val maxRoot = Const(CString("max"))(line)

      val clicksFoo = Join(DerefObject, CrossLeftSort, clicks, fooRoot)(line)
      val clicksUser = Join(DerefObject, CrossLeftSort, clicks, userRoot)(line)
      val group1 = dag.Group(1, clicksFoo, UnfixedSolution(0, clicksUser))

      lazy val parent = SplitGroup(1, Identities.Specs(Vector(LoadIds("/clicks"))))(input)(line)
      lazy val r1 = dag.Reduce(Min, parent)(line)
      lazy val r2 = dag.Reduce(Max, parent)(line)

      lazy val input: dag.Split = dag.Split(
        group1,
        Join(JoinObject, CrossLeftSort,
          Join(WrapObject, CrossLeftSort,
            userRoot,
            SplitParam(0)(input)(line))(line),
          Join(JoinObject, CrossLeftSort,
            Join(WrapObject, CrossLeftSort, minRoot, r1)(line),
            Join(WrapObject, CrossLeftSort, maxRoot, r2)(line))(line))(line))(line)

      val expected = MegaReduceState(
        Map(r1 -> parent, r2 -> parent),
        Map(parent -> List(parent)),
        Map(parent -> List(r1, r2)),
        Map(parent -> trans.Leaf(trans.Source))
      )

      findReductions(input, ctx) mustEqual expected
    }
  }

  def joinDeref(left: DepGraph, first: Int, second: Int, line: Line): DepGraph = 
    Join(DerefArray, CrossLeftSort,
      Join(DerefArray, CrossLeftSort,
        left,
        Const(CLong(first))(line))(line),
      Const(CLong(second))(line))(line)
}


object ReductionFinderSpecs extends ReductionFinderSpecs[test.YId] with test.YIdInstances 
