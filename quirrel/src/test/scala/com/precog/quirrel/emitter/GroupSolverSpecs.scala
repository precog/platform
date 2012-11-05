package com.precog.quirrel
package emitter

import com.precog.bytecode.Instructions
import com.precog.bytecode.RandomLibrary

import org.scalacheck.Prop
import org.specs2.ScalaCheck
import org.specs2.mutable._

import java.io.File
import scala.io.Source
import com.codecommit.gll.LineStream

import typer._

object GroupSolverSpecs extends Specification
    with ScalaCheck
    with StubPhases
    with Compiler
    with GroupSolver
    with ProvenanceChecker
    with RawErrors 
    with RandomLibrary {
      
  import ast._
  import buckets._
      
  "group solver" should {
    "identify and solve group set for trivial solve example" in {
      val input = "clicks := load(//clicks) solve 'day clicks where clicks.day = 'day"
      
      val Let(_, _, _, _,
        tree @ Solve(_, _, 
          origin @ Where(_, target, Eq(_, solution, _)))) = compile(input)
          
      val btrace = List()
      val expected = Group(Some(origin), target, UnfixedSolution("'day", solution), btrace)
      tree.errors must beEmpty
      tree.buckets mustEqual Map(Set() -> expected)
    }
    
    "identify composite bucket for trivial solve example with conjunction" in {
      val input = "clicks := load(//clicks) solve 'day clicks where clicks.day = 'day & clicks.din = 'day"
      
      val Let(_, _, _, _,
        tree @ Solve(_, _, 
          origin @ Where(_, target, And(_, Eq(_, leftSol, _), Eq(_, rightSol, _))))) = compile(input)
      
      val btrace = List()
      val expected = Group(Some(origin), target,
        IntersectBucketSpec(
          UnfixedSolution("'day", leftSol),
          UnfixedSolution("'day", rightSol)),
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
        tree @ Solve(_, _, _)) = compile(input)

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

      val let @ Let(_, _, _, _, _) = compile(input)

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
            tree2 @ Solve(_, _, _)))) = compile(input)

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
          tree @ Solve(_, _, _))) = compile(input)

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
          tree @ Solve(_, _, _))) = compile(input)

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
          tree @ Solve(_, _, _)) = compile(input)

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
        solve @ Solve(_, _, _)) = compile(input)

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
            solve2 @ Solve(_, _, _)))) = compile(input)

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
        tree @ Solve(_, _, _)) = compile(input)

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
            Vector(Eq(_, _, constrSol)), count @ Dispatch(_, _, Vector(origin @ Where(_, target, Eq(_, bodySol, _))))))) = compile(input)
          
      val btrace1 = List()
      val btrace2 = List()
      val expected = IntersectBucketSpec(
        Group(Some(origin), target,
          UnfixedSolution("'a", bodySol),
          btrace1),
        Group(None, constrSol,
          UnfixedSolution("'a", constrSol),
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
          Vector(Eq(_, _, constrSol)), _)) = compile(input)
          
      val expected = Group(None, constrSol, UnfixedSolution("'a", constrSol), List()) 
      
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
            Let(_, _, _, originB @ Where(_, targetB, Eq(_, solB, _)), add @ Add(_, descA @ Descent(_, dA, _), descB @ Descent(_, dB, _)))))) = compile(input)
      
      val btrace1 = List()
      val btrace2 = List()
      val expected = IntersectBucketSpec(
        Group(Some(originA), targetA,
          UnfixedSolution("'a", solA),
          btrace1),
        Group(Some(originB), targetB,
          UnfixedSolution("'b", solB),
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
              Let(_, _, _, originB @ Where(_, targetB, Eq(_, solB, _)), rel @ Relate(_, _, _, add @ Add(_, decA @ Descent(_, dA: Dispatch, _), decB @ Descent(_, dB: Dispatch, _)))))))) = compile(input)
      
      val btraceA = List()
      val btraceB = List()
              
      val expected = IntersectBucketSpec(
        Group(Some(originA), targetA,
          UnfixedSolution("'a", solA), btraceA),
        Group(Some(originB), targetB,
          UnfixedSolution("'b", solB), btraceB))
      
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
            Let(_, _, _, originB @ Where(_, targetB, Eq(_, solB, _)), _)))) = compile(input)
      
      tree.errors must not(beEmpty)     // TODO
    }
    
    "produce an error when a single tic-variable lacks a defining set" in {
      val input = """
        | foo := //foo
        |
        | solve 'a foo where foo.a < 'a
        | """.stripMargin
        
      compile(input).errors must not(beEmpty)
    }    

    "accept a solve when a single tic-variable has a defining set only in the constraints" in {
      val input = """
        | foo := //foo
        |
        | solve 'a = foo.a 
        |   foo where foo.a < 'a
        | """.stripMargin
        
      compile(input).errors must beEmpty
    }

    "reject a solve when a single tic-variable has a defining set only in the body" in {
      val input = """
        | foo := //foo
        |
        | solve 'a < foo.a
        |   foo where foo.a = 'a
        | """.stripMargin
        
      compile(input).errors must not(beEmpty)
    }

    "accept a solve when a tic var is constrained in the constraints and the body" in {
      val input = """
        | foo := //foo
        | bar := //bar
        |
        | solve 'a = foo.a 
        |   count(bar where bar.a = 'a)
        | """.stripMargin
        
      compile(input).errors must beEmpty
    }

    "accept a solve when a tic var is constrained inside a reduction" in {
      val input = """
        | foo := //foo
        |
        | solve 'a = foo.a 
        |   {count: count(foo where foo.a < 'a), value: 'a}
        | """.stripMargin
        
      compile(input).errors must beEmpty
    }
    
    "produce an error when a single tic-variable lacks a defining set with extras" in {
      val input = """
        | foo := //foo
        |
        | solve 'a foo where foo.a < 'a & foo.b = 42
        | """.stripMargin
        
      compile(input).errors must not(beEmpty)
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
        
      compile(input).errors must not(beEmpty)
    }
    
    "accept a solve when one of two reductions cannot be solved in the absence of an outer definition" in {
      val input = """
        | foo := //foo
        |
        | solve 'a
        |   count(foo where foo.a = 'a) + count(foo where foo.a = min('a + 1))
        | """.stripMargin
        
      compile(input).errors must beEmpty
    }
    
    "accept a function when a reduction cannot be solved in the presence of an outer definition" in {
      val input = """
        | foo := //foo
        |
        | solve 'a
        |   (foo where foo.a = 'a) + count(foo where foo.a = min('a + 1))
        | """.stripMargin
        
      compile(input).errors must beEmpty
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
        
      compile(input).errors must beEmpty
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

      compile(input).errors must beEmpty
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
        
      val tree = compile(input)
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
        
      val tree = compile(input)
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
        
      val tree = compile(input)
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
            boolean @ Eq(_, fooa, Add(_, TicVar(_, "'a"), n @ NumLit(_, "42")))))) = compile(input)
        
      tree.errors must beEmpty
      
      tree.buckets(Set()) must beLike {
        case Group(origin, target, UnfixedSolution("'a", sub @ Sub(_, fooa, n)), _) => {
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
              Where(_, left, right))))) = compile(input)
      
      tree.errors must not(beEmpty)
    }

    "accept a constraint in a solve" in {
      val input = """
        | foo := //foo
        | bar := //bar
        |
        | solve 'a = bar.a
        |   count(foo where foo.a = 'a)
        | """.stripMargin

      compile(input).errors must beEmpty
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
        
      compile(input).errors must beEmpty
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
        
      compile(input).errors must beEmpty
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
        
      compile(input).errors must beEmpty
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
            where @ Where(_, target, _)))) = compile(input)
        
      val btrace = List()
      
      val expected = Group(Some(where), target, UnfixedSolution("'a", solution), btrace)
        
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
        
      val tree = compile(input)
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
            d @ Dispatch(_, _, Vector(where @ Where(_, target, Eq(_, solution, _))))))) = compile(input)
            
      val btrace = List(d)
      val expected = Group(Some(where), target, UnfixedSolution("'a", solution), btrace)
      
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
        
      val tree = compile(input)
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
            where @ Where(_, target, Dispatch(_, _, Vector(Eq(_, _, solution))))))) = compile(input)
      
      val btrace = List()
      
      val expected = Group(Some(where), target, UnfixedSolution("'a", solution), btrace)
        
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
        
      val tree = compile(input)
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
              Eq(_, solution, _)))))) = compile(input)
              
      val btrace = List(d)
              
      val expected = Group(Some(where), target, UnfixedSolution("'a", solution), btrace)
      
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
                    Eq(_, solution, _)))))))))) = compile(input)

      val btrace = List(d2)
                    
      val expected = Group(Some(where), target, UnfixedSolution("'winner", solution), btrace)

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
        
      val tree = compile(input)
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
          d @ Dispatch(_, _, _))) = compile(input)
          
      val btrace = List(d)
          
      val expected = Group(Some(where), target, UnfixedSolution("'a", solution), btrace)
      
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
          d2: Dispatch)) = compile(input)
          
      val btrace = List(d, d2)
          
      val expected = Group(Some(where), target, UnfixedSolution("'a", solution), btrace)
      
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
        
      val tree = compile(input)
      tree.errors must not(beEmpty)
    }
    
    "accept a solve that requires algebraic manipulation w.r.t. formals" in {
      val input = """
        | foo := //foo
        | f(x, y) := x = y
        | solve 'a
        |   foo where f(foo.a, 'a / foo.b)
        | """.stripMargin
        
      val tree = compile(input)
      tree.errors must beEmpty
    }
  }
}
