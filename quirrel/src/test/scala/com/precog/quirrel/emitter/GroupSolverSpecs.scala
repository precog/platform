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
    "identify and solve group set for trivial cf example" in {
      val input = "clicks := load(//clicks) solve 'day clicks where clicks.day = 'day"
      
      val Let(_, _, _, _,
        tree @ Solve(_, _, 
          origin @ Where(_, target, Eq(_, solution, _)))) = compile(input)
          
      val expected = Group(origin, target, UnfixedSolution("'day", solution))
        
      tree.errors must beEmpty
      // tree.buckets must beSome(expected)
    }
    
    "identify composite bucket for trivial cf example with conjunction" in {
      val input = "clicks := load(//clicks) solve 'day clicks where clicks.day = 'day & clicks.din = 'day"
      
      val Let(_, _, _, _,
        tree @ Solve(_, _, 
          origin @ Where(_, target, And(_, Eq(_, leftSol, _), Eq(_, rightSol, _))))) = compile(input)
      
      val expected = Group(origin, target,
        IntersectBucketSpec(
          UnfixedSolution("'day", leftSol),
          UnfixedSolution("'day", rightSol)))
      
      tree.errors must beEmpty
      // tree.buckets must beSome(expected)
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
            Let(_, _, _, originB @ Where(_, targetB, Eq(_, solB, _)), _)))) = compile(input)
      
      val expected = IntersectBucketSpec(
        Group(originA, targetA,
          UnfixedSolution("'a", solA)),
        Group(originB, targetB,
          UnfixedSolution("'b", solB)))
      
      tree.errors must beEmpty
      // tree.buckets must beSome(expected)
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
              Let(_, _, _, originB @ Where(_, targetB, Eq(_, solB, _)), _))))) = compile(input)
      
      val expected = IntersectBucketSpec(
        Group(originA, targetA,
          UnfixedSolution("'a", solA)),
        Group(originB, targetB,
          UnfixedSolution("'b", solB)))
      
      tree.errors must beEmpty
      // tree.buckets must beSome(expected)
    }
    
    "produce an error when a single tic-variable lacks a defining set" in {
      val input = """
        | foo := //foo
        |
        | solve 'a foo where foo.a < 'a
        | """.stripMargin
        
      compile(input).errors must not(beEmpty)
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
      
      tree.buckets must beLike {
        case Some(Group(origin, target, UnfixedSolution("'a", sub @ Sub(_, fooa, n)))) => {
          sub.provenance mustEqual StaticProvenance("/foo")
          // anything else?
        }
      }
    }
  }
}
