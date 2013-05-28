package com.precog.quirrel
package emitter

import org.specs2.mutable._

import java.io.File
import scala.io.Source
import com.codecommit.gll.LineStream

import typer._

object GroupSolverSpecs extends Specification
    with StubPhases
    with CompilerUtils
    with Compiler
    with GroupSolver
    with ProvenanceChecker
    with RawErrors 
    with StaticLibrarySpec {
      
  import ast._
  import buckets._
  import library._
  
  "group solver" should {
    "identify and solve group set for trivial solve example" in {
      val input = "clicks := load(//clicks) solve 'day clicks where clicks.day = 'day"
      
      val Let(_, _, _, _,
        tree @ Solve(_, _, 
          origin @ Where(_, target, Eq(_, solution, _)))) = compileSingle(input)
          
      val btrace = List()
      val expected = Group(Some(origin), target, UnfixedSolution("'day", solution, btrace), btrace)
      tree.errors must beEmpty
      tree.buckets mustEqual Map(Set() -> expected)
    }
    
    "identify composite bucket for trivial solve example with conjunction" in {
      val input = "clicks := load(//clicks) solve 'day clicks where clicks.day = 'day & clicks.din = 'day"
      
      val Let(_, _, _, _,
        tree @ Solve(_, _, 
          origin @ Where(_, target, And(_, Eq(_, leftSol, _), Eq(_, rightSol, _))))) = compileSingle(input)
      
      val btrace = List()
      val expected = Group(Some(origin), target,
        IntersectBucketSpec(
          UnfixedSolution("'day", leftSol, btrace),
          UnfixedSolution("'day", rightSol, btrace)),
        btrace)
      
      tree.errors must beEmpty
      tree.buckets mustEqual Map(Set() -> expected)
    }

    "identify and fail to solve problematic case of nested solves" in {
      val input = """
        medals := //summer_games/london_medals
        
        solve 'gender
          solve 'weight
            medals where medals.Weight = 'weight & medals.Gender = 'gender
      """.stripMargin

      val Let(_, _, _, _,
        tree @ Solve(_, _, _)) = compileSingle(input)

      tree.errors mustEqual Set(ConstraintsWithinInnerSolve)
    }

    "accept a solve on a union with a `with`" in {
      val input = """
        | medals := //summer_games/london_medals
        | athletes := //summer_games/athletes
        | 
        | data := medals union (athletes with { gender: athletes.Sex })
        | 
        | solve 'gender 
        |   { gender: 'gender, num: count(data.gender where data.gender = 'gender) } 
      """.stripMargin

      val let @ Let(_, _, _, _, _) = compileSingle(input)

      let.errors must beEmpty
    }
    
    "accept acceptable case of nested solves" in {
      val input = """
        | medals := //summer_games/london_medals
        | 
        | solve 'gender
        |   medals' := medals where medals.Gender = 'gender
        | 
        |   solve 'weight
        |     medals' where medals'.Weight = 'weight
        | """.stripMargin

      val let @ Let(_, _, _, _,
        tree1 @ Solve(_, _, 
          Let(_, _, _, _, 
            tree2 @ Solve(_, _, _)))) = compileSingle(input)

      let.errors must beEmpty
      tree1.errors must beEmpty
      tree2.errors must beEmpty
    }
    
    "accept acceptable case when one solve contains a dispatch which contains tic variable from another solve" in {
      val input = """
       |  medals := //summer_games/london_medals
       |  
       |  medals' := solve 'gender
       |    medals where medals.Gender = 'gender
       | 
       |  solve 'weight
       |    medals' where medals'.Weight = 'weight & medals'.isAwesome
       | """.stripMargin

      val let @ Let(_, _, _, _,
        Let(_, _, _, _,
          tree @ Solve(_, _, _))) = compileSingle(input)

      let.errors must beEmpty
      tree.errors must beEmpty
    }
   
    "accept acceptable case when a dispatch in one solve contains, as an actual, a tic variable from another solve" in {
      val input = """
        medals := //summer_games/london_medals
        
        f(x) := x

        solve 'gender
          medals' := medals where medals.Gender = 'gender

          solve 'weight
            medals' where medals'.Weight = 'weight & f('gender)
      """.stripMargin

      val let @ Let(_, _, _, _,
        Let(_, _, _, _,
          tree @ Solve(_, _, _))) = compileSingle(input)

      let.errors must beEmpty
      tree.errors must beEmpty
    }.pendingUntilFixed

    "accept acceptable case when one solve contains a tic variable from another solve" in {
      val input = """
        medals := //summer_games/london_medals
        
        solve 'gender
          medals' := medals where medals.Gender = 'gender

          solve 'weight
            medals' where medals'.Weight = 'weight & 'gender
      """.stripMargin

      val let @ Let(_, _, _, _,
          tree @ Solve(_, _, _)) = compileSingle(input)

      let.errors must beEmpty
      tree.errors must beEmpty
    }.pendingUntilFixed 

    "accept a solve when the tic var is constrained in a let" in {
      val input = """
        medals := //summer_games/london_medals
        
        solve 'gender
          gender := medals where medals.Gender = 'gender
          gender + 1
      """.stripMargin

      val tree @ Let(_, _, _, _,
        solve @ Solve(_, _, _)) = compileSingle(input)

      solve.errors must beEmpty
      tree.errors must beEmpty
    }    

    "accept a solve when a tic var is used in the scope of a nested solve that doesn't bind it" in {
      val input = """
        medals := //summer_games/london_medals
        
        solve 'gender
          gender := medals where medals.Gender = 'gender
          
          solve 'weight
            weight := gender where gender.Weight = 'weight
            weight + 'gender
      """.stripMargin

      val tree @ Let(_, _, _, _,
        solve1 @ Solve(_, _,
          Let(_, _, _, _,
            solve2 @ Solve(_, _, _)))) = compileSingle(input)

      solve1.errors must beEmpty
      solve2.errors must beEmpty
      tree.errors must beEmpty
    }    

    "identify and fail to solve more complicated problematic case of nested solves" in {
      val input = """
        medals := //summer_games/london_medals
        
        solve 'gender
          histogram := solve 'weight
            medals' := medals where medals.Weight = 'weight & medals.Gender = 'gender
            {weight: 'weight, count: count(medals')}
          
          maxCount := max(histogram.count where histogram.weight != "")
          
          {gender: 'gender, optimalWeight: (histogram.weight where histogram.count = maxCount)}
      """.stripMargin

      val Let(_, _, _, _,
        tree @ Solve(_, _, _)) = compileSingle(input)

      tree.errors mustEqual Set(ConstraintsWithinInnerSolve)
    }

    "identify composite bucket for solve with constraint for 'a in constraints and body" in {
      val input = """
        | foo := //foo 
        | bar := //bar 
        | solve 'a = bar.a 
        |   count(foo where foo.a = 'a)
        """.stripMargin
      
      val Let(_, _, _, _,
        Let(_, _, _, _,
          tree @ Solve(_,
            Vector(Eq(_, _, constrSol)), count @ Dispatch(_, _, Vector(origin @ Where(_, target, Eq(_, bodySol, _))))))) = compileSingle(input)
          
      val btrace1 = List()
      val btrace2 = List()
      val expected = IntersectBucketSpec(
        Group(Some(origin), target,
          UnfixedSolution("'a", bodySol, btrace1),
          btrace1),
        Group(None, constrSol,
          UnfixedSolution("'a", constrSol, btrace2),
          btrace2))
      
      tree.errors must beEmpty
      tree.buckets mustEqual Map(Set() -> expected)
    }

    "identify composite bucket for solve with constraint for 'a in constraints and body with assertion" in {
      val input = """
        | foo := //foo 
        | bar := //bar 
        | assert true
        | solve 'a = bar.a 
        |   count(foo where foo.a = 'a)
        """.stripMargin
      
      val Let(_, _, _, _,
        Let(_, _, _, _,
          Assert(_, _, 
            tree @ Solve(_,
              Vector(Eq(_, _, constrSol)), count @ Dispatch(_, _, Vector(origin @ Where(_, target, Eq(_, bodySol, _)))))))) = compileSingle(input)
          
      val btrace1 = List()
      val btrace2 = List()
      val expected = IntersectBucketSpec(
        Group(Some(origin), target,
          UnfixedSolution("'a", bodySol, btrace1),
          btrace1),
        Group(None, constrSol,
          UnfixedSolution("'a", constrSol, btrace2),
          btrace2))
      
      tree.errors must beEmpty
      tree.buckets mustEqual Map(Set() -> expected)
    }
    
    "identify composite bucket for solve with constraint for 'a only solvable in constrains" in {
      val input = """
        | foo := //foo 
        | solve 'a = foo.a 
        |   count(foo where foo.a < 'a)
        """.stripMargin
      
      val Let(_, _, _, _,
        tree @ Solve(_,
          Vector(Eq(_, _, constrSol)), _)) = compileSingle(input)
          
      val expected = Group(None, constrSol, UnfixedSolution("'a", constrSol, Nil), Nil) 
      
      tree.errors must beEmpty
      tree.buckets mustEqual Map(Set() -> expected)
    }
    
    "identify separate buckets for independent tic variables on same set" in {
      val input = """
        | clicks := load(//clicks)
        | 
        | solve 'a, 'b
        |   bar := clicks where clicks.a = 'a
        |   baz := clicks where clicks.b = 'b
        |
        |   bar.a + baz.b
        | """.stripMargin
        
      val Let(_, _, _, _,
        tree @ Solve(_, _,
          Let(_, _, _, originA @ Where(_, targetA, Eq(_, solA, _)),
            Let(_, _, _, originB @ Where(_, targetB, Eq(_, solB, _)), add @ Add(_, descA @ Descent(_, dA, _), descB @ Descent(_, dB, _)))))) = compileSingle(input)
      
      val btrace1 = List()
      val btrace2 = List()
      val expected = IntersectBucketSpec(
        Group(Some(originA), targetA,
          UnfixedSolution("'a", solA, btrace1),
          btrace1),
        Group(Some(originB), targetB,
          UnfixedSolution("'b", solB, btrace2),
          btrace2))
      
      tree.errors must beEmpty
      tree.buckets mustEqual Map(Set() -> expected)
    }
    
    "identify separate buckets for independent tic variables on different sets" in {
      val input = """
        | clicks := load(//clicks)
        | imps := load(//impressions)
        | 
        | solve 'a, 'b
        |   bar := clicks where clicks.a = 'a
        |   baz := imps where imps.b = 'b
        |
        |   bar ~ baz
        |     bar.a + baz.b
        | """.stripMargin
        
      val Let(_, _, _, _,
        Let(_, _, _, _,
          tree @ Solve(_, _,
            Let(_, _, _, originA @ Where(_, targetA, Eq(_, solA, _)),
              Let(_, _, _, originB @ Where(_, targetB, Eq(_, solB, _)), rel @ Relate(_, _, _, add @ Add(_, decA @ Descent(_, dA: Dispatch, _), decB @ Descent(_, dB: Dispatch, _)))))))) = compileSingle(input)
      
      val btraceA = List()
      val btraceB = List()
              
      val expected = IntersectBucketSpec(
        Group(Some(originA), targetA,
          UnfixedSolution("'a", solA, btraceA), btraceA),
        Group(Some(originB), targetB,
          UnfixedSolution("'b", solB, btraceB), btraceB))
      
      tree.errors must beEmpty
      tree.buckets mustEqual Map(Set() -> expected)
    }
    
    "reject a group set that is a function of tic-variables being solved (cyclic constraints case 2)" in {
      val input = """
        | clicks := load(//clicks)
        | 
        | solve 'a, 'b
        |   bar := clicks where clicks.a = 'a
        |   baz := bar where bar.b = 'b
        |
        |   bar ~ baz
        |     bar.a + baz.b
        | """.stripMargin
        
      val Let(_, _, _, _,
        tree @ Solve(_, _,
          Let(_, _, _, originA @ Where(_, targetA, Eq(_, solA, _)),
            Let(_, _, _, originB @ Where(_, targetB, Eq(_, solB, _)), _)))) = compileSingle(input)
      
      tree.errors must not(beEmpty)     // TODO
    }
    
    "produce an error when a single tic-variable lacks a defining set" in {
      val input = """
        | foo := //foo
        |
        | solve 'a foo where foo.a < 'a
        | """.stripMargin
        
      compileSingle(input).errors must not(beEmpty)
    }    

    "accept a solve when a single tic-variable has a defining set only in the constraints" in {
      val input = """
        | foo := //foo
        |
        | solve 'a = foo.a 
        |   foo where foo.a < 'a
        | """.stripMargin
        
      compileSingle(input).errors must beEmpty
    }

    "reject a solve when a single tic-variable has a defining set only in the body" in {
      val input = """
        | foo := //foo
        |
        | solve 'a < foo.a
        |   foo where foo.a = 'a
        | """.stripMargin
        
      compileSingle(input).errors must not(beEmpty)
    }

    "accept a solve when a tic var is constrained in the constraints and the body" in {
      val input = """
        | foo := //foo
        | bar := //bar
        |
        | solve 'a = foo.a 
        |   count(bar where bar.a = 'a)
        | """.stripMargin
        
      compileSingle(input).errors must beEmpty
    }

    "accept a solve when a tic var is constrained inside a reduction" in {
      val input = """
        | foo := //foo
        |
        | solve 'a = foo.a 
        |   {count: count(foo where foo.a < 'a), value: 'a}
        | """.stripMargin
        
      compileSingle(input).errors must beEmpty
    }
    
    "produce an error when a single tic-variable lacks a defining set with extras" in {
      val input = """
        | foo := //foo
        |
        | solve 'a foo where foo.a < 'a & foo.b = 42
        | """.stripMargin
        
      compileSingle(input).errors must not(beEmpty)
    }
    
    "produce an error when one of several tic-variables lacks a defining set" in {
      val input = """
        | foo := //foo
        |
        | solve 'a, 'b
        |   foo' := foo where foo.a < 'a
        |   foo'' := foo where foo.b = 'b
        |   foo' + foo''
        | """.stripMargin
        
      compileSingle(input).errors must not(beEmpty)
    }
    
    "accept a solve when one of two reductions cannot be solved in the absence of an outer definition" in {
      val input = """
        | foo := //foo
        |
        | solve 'a
        |   count(foo where foo.a = 'a) + count(foo where foo.a = min('a + 1))
        | """.stripMargin
        
      compileSingle(input).errors must beEmpty
    }
    
    "accept a function when a reduction cannot be solved in the presence of an outer definition" in {
      val input = """
        | foo := //foo
        |
        | solve 'a
        |   (foo where foo.a = 'a) + count(foo where foo.a = min('a + 1))
        | """.stripMargin
        
      compileSingle(input).errors must beEmpty
    }
    
    "accept a function when an optional defining set cannot be solved for a single tic-variable" in {
      val input = """
        | foo := //foo
        |
        | solve 'a
        |   foo' := foo where foo.a < 'a
        |   foo'' := foo where foo.b = 'a
        |   foo' + foo''
        | """.stripMargin
        
      compileSingle(input).errors must beEmpty
    }

    "accept bucketing for indirect failed solution through reduction" in {
      val input = """
        | foo := //foo
        | bar := //bar
        | solve 'a
        |   foo' := foo where foo.a = 'a
        |   bar' := bar where bar.a > 'a
        |   count(foo') + count(bar')
        | """.stripMargin

      compileSingle(input).errors must beEmpty
    }
    
    "reject shared buckets for dependent tic variables on the same set" in {
      val input = """
        | organizations := load(//organizations)
        | 
        | solve 'revenue, 'campaign
        |   organizations' := organizations where organizations.revenue = 'revenue
        |   organizations'' := organizations' where organizations'.campaign = 'campaign
        |   
        |   organizations''
        | """.stripMargin
        
      val tree = compileSingle(input)
      tree.errors must not(beEmpty)
    }
    
    "reject bucketing of separate sets within a relation" in {
      val input = """
        | foo := //foo
        | bar := //bar
        | 
        | foo ~ bar
        |   solve 'a
        |     foo where bar.a = 'a""".stripMargin
        
      val tree = compileSingle(input)
      tree.errors must not(beEmpty)
    }
    
    "accept shared buckets for dependent tic variables on the same set when at least one can be solved" in {
      val input = """
        | campaigns := load(//campaigns)
        | organizations := load(//organizations)
        | 
        | solve 'revenue, 'campaign
        |   organizations' := organizations where organizations.revenue = 'revenue
        |   campaigns' := campaigns where campaigns.campaign = 'campaign
        |   organizations'' := organizations' where organizations'.campaign = 'campaign
        |   
        |   campaigns' ~ organizations''
        |     { revenue: 'revenue, num: count(campaigns') }
        | """.stripMargin
        
      val tree = compileSingle(input)
      tree.errors must beEmpty
    }
    
    "produce valid AST nodes when solving" in {
      val input = """
        | foo := //foo
        | solve 'a
        |   foo where foo.a = 'a + 42""".stripMargin
        
      val Let(_, _, _, _,
        tree @ Solve(_, _,
          origin @ Where(_,
            target,
            boolean @ Eq(_, fooa, Add(_, TicVar(_, "'a"), n @ NumLit(_, "42")))))) = compileSingle(input)
        
      tree.errors must beEmpty
      
      tree.buckets(Set()) must beLike {
        case Group(origin, target, UnfixedSolution("'a", sub @ Sub(_, fooa, n), _), _) => {
          sub.provenance mustEqual StaticProvenance("/foo")
          // anything else?
        }
      }
    }
    
    "reject a constraint which lacks an unambiguous common parent" in {
      val input = """
        | foo := //foo
        | bar := //bar
        | 
        | solve 'a
        |   foo ~ bar
        |     (foo + bar) where (foo - bar).a = 'a
        | """.stripMargin
        
      val tree @ Let(_, _, _, _,
        Let(_, _, _, _,
          solve @ Solve(_, _, 
            Relate(_, _, _,
              Where(_, left, right))))) = compileSingle(input)
      
      tree.errors must not(beEmpty)
    }
    
    "reject a constraint which attempts to parent through an assertion" in {
      val input = """
        | foo := //foo
        | foo' := assert true foo
        |
        | solve 'a
        |   foo where foo'.a = 'a
        | """.stripMargin
        
      compileSingle(input).errors must not(beEmpty)
    }

    "accept a constraint in a solve" in {
      val input = """
        | foo := //foo
        | bar := //bar
        |
        | solve 'a = bar.a
        |   count(foo where foo.a = 'a)
        | """.stripMargin

      compileSingle(input).errors must beEmpty
    }
    
    "accept a reduced sessionize" in {
      val input = """
        | rawInteractions := //interactions
        | 
        | solve 'userId
        |   interactions := rawInteractions where rawInteractions.userId = 'userId
        |   
        |   bounds := solve 'it
        |     interactions.time where interactions = 'it
        |     
        |   solve 'it1, 'it2
        |     bounds where bounds = 'it1 & bounds.isLower & bounds = 'it2
        | """.stripMargin
        
      compileSingle(input).errors must beEmpty
    }
    
    "accept a solve on the results of a relate operation" in {
      val input = """
        | clicks := //clicks
        | impressions := //impressions
        | 
        | data := clicks ~ impressions 
        |   clicks + impressions
        |
        | solve 'a
        |   count(data where data.a = 'a)
        | """.stripMargin
        
      compileSingle(input).errors must beEmpty
    }
    
    "accept a solve on the results of a union operation" in {
      val input = """
        | clicks := //clicks
        | impressions := //impressions
        | 
        | data := clicks union impressions
        |
        | solve 'a
        |   count(data where data.a = 'a)
        | """.stripMargin
        
      compileSingle(input).errors must beEmpty
    }
    
    "accept a solve involving variables as actuals" in {
      val input = """
        | foo := //foo
        | f(x) := x = foo.a
        | solve 'a
        |   foo where f('a)
        | """.stripMargin
        
      val Let(_, _, _, _,
        Let(_, _, _, Eq(_, _, solution),
          solve @ Solve(_, _,
            where @ Where(_, target, d: Dispatch)))) = compileSingle(input)
        
      val btrace = List()
      
      val expected = Group(Some(where), target, UnfixedSolution("'a", solution, d :: btrace), btrace)
        
      solve.errors must beEmpty
      solve.buckets mustEqual Map(Set() -> expected)
    }
    
    "reject a non-relating solve involving variables as actuals" in {
      val input = """
        | foo := //foo
        | f(x) := x
        | solve 'a
        |   foo where f('a)
        | """.stripMargin
        
      val tree = compileSingle(input)
      tree.errors must not(beEmpty)
    }
    
    "accept a solve involving a where as an actual" in {
      val input = """
        | foo := //foo
        | f(x) := x
        | solve 'a
        |   f(foo where foo.a = 'a)
        | """.stripMargin
        
      val Let(_, _, _, _,
        Let(_, _, _, _,
          solve @ Solve(_, _,
            d @ Dispatch(_, _, Vector(where @ Where(_, target, Eq(_, solution, _))))))) = compileSingle(input)
            
      val btrace = List(d)
      val expected = Group(Some(where), target, UnfixedSolution("'a", solution, btrace), btrace)
      
      solve.errors must beEmpty
      solve.buckets mustEqual Map(Set() -> expected)
    }
    
    "reject a solve involving a where as an actual" in {
      val input = """
        | foo := //foo
        | foo' := new //foo
        | f(x) := x
        | solve 'a
        |   f(foo where foo'.a = 'a)
        | """.stripMargin
        
      val tree = compileSingle(input)
      tree.errors must not(beEmpty)
    }
    
    "accept a solve involving relations as actuals" in {
      val input = """
        | foo := //foo
        | f(x) := x
        | solve 'a
        |   foo where f('a = foo.a)
        | """.stripMargin
        
      val Let(_, _, _, _,
        Let(_, _, _, _,
          solve @ Solve(_, _,
            where @ Where(_, target, d @ Dispatch(_, _, Vector(Eq(_, _, solution))))))) = compileSingle(input)
      
      val btrace = List()
      
      val expected = Group(Some(where), target, UnfixedSolution("'a", solution, d :: btrace), btrace)
        
      solve.errors must beEmpty
      solve.buckets mustEqual Map(Set() -> expected)
    }
    
    "reject a solve involving relations as actuals in a function with a problematic body" in {
      val input = """
        | foo := //foo
        | f(x) := count(x)
        | solve 'a
        |   foo where f('a = foo.a)
        | """.stripMargin
        
      val tree = compileSingle(input)
      tree.errors must not(beEmpty)
    }
    
    "accept a solve involving a generic where within a function" in {
      val input = """
        | foo := //foo
        | f(x, y) := x where y
        | solve 'a
        |   f(foo, foo.a = 'a)
        | """.stripMargin
        
      val Let(_, _, _, _,
        Let(_, _, _, where: Where,
          solve @ Solve(_, _,
            d @ Dispatch(_, _, Vector(
              target,
              Eq(_, solution, _)))))) = compileSingle(input)
              
      val btrace = List(d)
              
      val expected = Group(Some(where), target, UnfixedSolution("'a", solution, btrace), btrace)
      
      solve.errors must beEmpty
      solve.buckets mustEqual Map(Set() -> expected)
    }
    
    "accept another solve with a generic where inside a function" in {
      val input = """
        | medals := //summer_games/london_medals
        | athletes := //summer_games/athletes
        | 
        | data := athletes union (medals with { winner: medals."Medal winner" })
        | 
        | f(x, y) := x where y
        | 
        | solve 'winner 
        |   count(f(data.winner, data.winner = 'winner))
      """.stripMargin

      val Let(_, _, _, _,
        Let(_, _, _, _,
          Let(_, _, _, _,
            Let(_, _, _, where: Where,
              solve @ Solve(_, _,
                d1 @ Dispatch(_, _, Vector(
                  d2 @ Dispatch(_, _, Vector(
                    target,
                    Eq(_, solution, _)))))))))) = compileSingle(input)

      val btrace = List(d2)
                    
      val expected = Group(Some(where), target, UnfixedSolution("'winner", solution, btrace), btrace)

      solve.errors must beEmpty
      solve.buckets mustEqual Map(Set() -> expected)
    }

    "reject a solve involving a generic where within a function with invalid parameters" in {
      val input = """
        | foo := //foo
        | foo' := new //foo
        | f(x, y) := x where y
        | solve 'a
        |   f(foo, foo'.a = 'a)
        | """.stripMargin
        
      val tree = compileSingle(input)
      tree.errors must not(beEmpty)
    }
    
    "accept a solve with relation expressions involving formals" in {
      val input = """
        | foo := //foo
        | f(x) :=
        |   solve 'a
        |     foo where x = 'a
        | f(foo.a)
        | """.stripMargin
        
      val Let(_, _, _, _,
        Let(_, _, _,
          solve @ Solve(_, _,
            where @ Where(_, target, Eq(_, solution, _))),
          d @ Dispatch(_, _, _))) = compileSingle(input)
          
      val btrace = List(d)
          
      val expected = Group(Some(where), target, UnfixedSolution("'a", solution, btrace), btrace)
      
      solve.errors must beEmpty
      solve.buckets mustEqual Map(Set(d) -> expected)
    }
    
    "accept a solve with relation expressions involving formals of formals" in {
      val input = """
        | foo := //foo
        | g(x) :=
        |   f(x) :=
        |     solve 'a
        |       foo where x = 'a
        |   f(x.a)
        | g(foo)
        | """.stripMargin
        
      val Let(_, _, _, _,
        Let(_, _, _,
          Let(_, _, _,
            solve @ Solve(_, _,
              where @ Where(_, target, Eq(_, solution, _))),
            d @ Dispatch(_, _, _)),
          d2: Dispatch)) = compileSingle(input)
          
      val btrace = List(d, d2)
          
      val expected = Group(Some(where), target, UnfixedSolution("'a", solution, btrace), btrace)
      
      solve.errors must beEmpty
      solve.buckets mustEqual Map(Set(d, d2) -> expected)
    }
    
    "reject a solve with relation expressions involving invalid formals of formals" in {
      val input = """
        | foo := //foo
        | foo' := new //foo
        | g(x) :=
        |   f(x) :=
        |     solve 'a
        |       foo where x = 'a
        |   f(x.a)
        | g(foo')
        | """.stripMargin
        
      val tree = compileSingle(input)
      tree.errors must not(beEmpty)
    }
    
    "accept a solve that requires algebraic manipulation w.r.t. formals" in {
      val input = """
        | foo := //foo
        | f(x, y) := x = y
        | solve 'a
        |   foo where f(foo.a, 'a / foo.b)
        | """.stripMargin
        
      val tree = compileSingle(input)
      tree.errors must beEmpty
    }
    
    "accept a solve defined by a constraint clause and inequalities in the body" in {
      val input = """
        | import std::time::*
        | import std::stats::*
        | import std::math::*
        | 
        | agents := //snapEngage/customer/widget
        | 
        | upperBound := getMillis("2012-10-05T23:59:59")
        | lowerBound := getMillis("2012-10-05T00:00:00")
        | 
        | minuteOfDay(time) := minuteOfHour(time) + hourOfDay(time)*60
        | 
        | data := {agentId: agents.agentId, action: agents.action, minuteOfDay: minuteOfDay(agents.timeStamp) , millis: getMillis(agents.timeStamp)}
        | 
        | 
        | data' := data where data.millis > lowerBound & data.millis < upperBound
        | 
        | bin(time, sizeOfBin) := floor((time)/sizeOfBin)
        | 
        | bins := bin(data'.minuteOfDay, 5)
        | 
        | --result := data' with {bins: bins}
        | 
        | 
        | result2 := solve 'agent
        |     data' := data where data.millis <= upperBound & data.millis >= lowerBound & data.agentId = 'agent
        |     
        |     order := denseRank(data'.millis)
        |     data'' := data' with {rank: order}
        |     
        |     newData := new data''
        |     newData' := newData with {rank: newData.rank -1}
        |     
        |     result := newData' ~ data''
        |      {first: data'', second: newData'}
        |     where newData'.rank = data''.rank
        |     
        |     {start: result.first.millis, end: result.second.millis, agent: result.first.agentId, action: result.first.action, startBin: bin(minuteOfDay(millisToISO(result.first.millis, "+00:00")),5) , endBin:bin(minuteOfDay(millisToISO(result.second.millis, "+00:00")),5) }
        | 
        | 
        | solve 'bins = bins
        |   {bin: 'bins, count: count(result2.action where result2.action = "Online"  & 'bins >= result2.startBin & 'bins <=result2.endBin)}
        | """.stripMargin
      
      compileSingle(input).errors must beEmpty
    }

    "accept simplified solve" in {
      val input = """
        | foo := //foo
        |
        | minute(time) := std::time::minuteOfHour(time)
        | 
        | solve 'agent
        |   result := foo where minute(foo) = 'agent
        |   minute(result)
        | """.stripMargin
      
      compileSingle(input).errors must beEmpty
    }
     
    "correctly identify commonality for constraint clause deriving from object def on non-constant fields" in {
      val input = """
        | clicks := //clicks
        | data := { user: clicks.user, page: clicks.page }
        | 
        | solve 'bins = data
        |   'bins
        | """.stripMargin
        
      val tree @ Let(_, _, _, _,
        Let(_, _, _, _,
          solve @ Solve(_, Vector(Eq(_, _, target)), _))) = compileSingle(input)
          
      val expected = Group(None, target, UnfixedSolution("'bins", target, Nil), Nil)
      
      tree.errors must beEmpty
      solve.buckets mustEqual Map(Set() -> expected)
    }
    
    "reject a solve where the target includes a reduction on the commonality" in {
      val input = """
        | clicks := //clicks
        |
        | solve 'userId
        |   count(clicks) / clicks.time where clicks.userId = 'userId
        | """.stripMargin
          
      compileSingle(input).errors must not(beEmpty)
    }
    
    "accept interaction-totals.qrl" in {
      val input = """
        | interactions := //interactions
        | 
        | hourOfDay(time) := time / 3600000           -- timezones, anyone?
        | dayOfWeek(time) := time / 604800000         -- not even slightly correct
        | 
        | solve 'hour, 'day
        |   dayAndHour := dayOfWeek(interactions.time) = 'day & hourOfDay(interactions.time) = 'hour
        |   sum(interactions where dayAndHour)
        | """.stripMargin
        
      compileSingle(input).errors must beEmpty
    }
    
    "accept a solve grouping on the results of an object concat with a stdlib op1" in {
      val input = """
        | import std::time::*
        | 
        | agents := //clicks
        | data := { agentId: agents.userId, millis: getMillis(agents.timeString) }
        | 
        | upperBound := getMillis("2012-04-03T23:59:59")
        | 
        | solve 'agent
        |   data where data.millis < upperBound & data.agentId = 'agent
        | """.stripMargin
        
      compileSingle(input).errors must beEmpty
    }
    
    "accept a solve where the commonality is only equal ignoring location" in {
      val input = """
        | solve 'a
        |   //campaigns where (//campaigns).foo = 'a
        | """.stripMargin
        
      compileSingle(input).errors must beEmpty
    }
    
    "reject a solve where the extras involve reductions" in {
      val input = """
        | sales := //sales
        | solve 'state
        |   sales where sales.state = 'state &
        |     (sales.total = max(sales.total) | sales.total = min(sales.total))
        | """.stripMargin
        
      compileSingle(input).errors must not(beEmpty)
    }
    
    "accept a solve on the results of a denseRank" in {
      val input = """
        | agents := //snapEngage/customer/widget
        | data' := agents with { rank: denseRank(agents.millis) }
        |
        | solve 'rank
        |   data' where data'.rank = 'rank
        | """.stripMargin
        
      compileSingle(input).errors must beEmpty
    }
    
    "identify only the first of a double-constrained group set" in {
      val input = """
        | foo := //foo
        | 
        | solve 'a
        |   foo' := foo where foo = 'a
        |   count(foo' where foo' = 'a)
        | """.stripMargin
        
      val Let(_, _, _, _,
        solve @ Solve(_, _,
          Let(_, _, _,
            where @ Where(_, target, Eq(_, solution, _)),
            _))) = compileSingle(input) 
            
      val expected = Group(Some(where), target, UnfixedSolution("'a", solution, Nil), Nil)
      
      solve.errors must beEmpty
      solve.buckets mustEqual(Map(Set() -> expected))
    }
    
    "reject a solve with a doubly-compared tic variable" in {
      val input = """
        | foo := //foo
        | solve 'a = foo = 'a
        |   'a
        | """.stripMargin
        
      compileSingle(input).errors must contain(ExtraVarsInGroupConstraint("'a"))
    }
    
    "accept a solve on the results of an observation" in {
      val input = """
        | foo := //foo
        | r := observe(foo, std::random::foobar(42))
        | 
        | solve 'a
        |   r where r = 'a
        | """.stripMargin
        
      compileSingle(input).errors must beEmpty
    }
    
    "not explode more differently" in {
      val input = """
        | createModel(data) :=  
        |   cumProb := solve 'rank = data
        |     42
        | 
        |   solve 'rank = cumProb
        |     42
        | 
        | createModel(42)
        | """.stripMargin
      
      compileSingle(input) must not(throwA[Throwable])
    }
    
    "allow a solve with a critical condition defined by a Cond" in {
      val input = """
        | foo := //foo
        | solve 'a
        |   foo where (if foo.a then foo.b else foo.c) = 'a""".stripMargin
        
      compileSingle(input).errors must beEmpty
    }
    
    "reject a reduction in an extra" in {
      val input = """
        | foo := //foo
        | 
        | solve 'a
        |   foo where foo.a = 'a & exists(foo.b = "boo")
        | """.stripMargin
        
      compileSingle(input).errors must not(beEmpty)
    }
    
    "separate bucket specs for groups within functions within union" in {
      val input = """
        | data := new 12
        | 
        | f(path) := 
        |   solve 'price count(path where path = 'price)
        |     
        | f(data) union f(data with 24)""".stripMargin
        
      val Let(_, _, _, _,
        Let(_, _, _,
          solve @ Solve(_, _, Dispatch(_, _, Vector(
            target @ Where(_, path, _)))),
          Union(_,
            d1 @ Dispatch(_, _, Vector(data)),
            d2 @ Dispatch(_, _, Vector(data2))))) = compileSingle(input)
            
      solve.buckets must haveKey(Set(d1))
      solve.buckets must haveKey(Set(d2))
      
      val spec1 = solve.buckets(Set(d1))
      spec1 must beLike {
        case Group(Some(`target`), `data`, UnfixedSolution("'price", _, `d1` :: Nil), `d1` :: Nil) => ok
      }
      
      val spec2 = solve.buckets(Set(d2))
      spec2 must beLike {
        case Group(Some(`target`), `data2`, UnfixedSolution("'price", _, `d2` :: Nil), `d2` :: Nil) => ok
      }
    }
  }
}
