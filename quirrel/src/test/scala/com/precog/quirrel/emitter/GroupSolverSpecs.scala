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
    with RawErrors 
    with RandomLibrary {
      
  import ast._
  import buckets._
      
  "group solver" should {
    "identify and solve group set for trivial cf example" in {
      val input = "clicks := load(//clicks) onDay('day) := clicks where clicks.day = 'day onDay"
      
      val tree @ Let(_, _, _, _,
        Let(_, _, _, 
          origin @ Where(_, target, Eq(_, solution, _)), _)) = compile(input)
          
      val expected = Group(origin, target, UnfixedSolution("'day", solution))
        
      tree.buckets must beSome(expected)
      tree.errors must beEmpty
    }
    
    "identify composite bucket for trivial cf example with conjunction" in {
      val input = "clicks := load(//clicks) onDay('day) := clicks where clicks.day = 'day & clicks.din = 'day onDay"
      
      val tree @ Let(_, _, _, _,
        Let(_, _, _, 
          origin @ Where(_, target, And(_, Eq(_, leftSol, _), Eq(_, rightSol, _))), _)) = compile(input)
      
      val expected = Group(origin, target,
        IntersectBucketSpec(
          UnfixedSolution("'day", leftSol),
          UnfixedSolution("'day", rightSol)))
      
      tree.buckets must beSome(expected)
      tree.errors must beEmpty
    }
    
    "identify separate buckets for independent tic variables on same set" in {
      val input = """
        | clicks := load(//clicks)
        | 
        | foo('a, 'b) :=
        |   bar := clicks where clicks.a = 'a
        |   baz := clicks where clicks.b = 'b
        |
        |   bar.a + baz.b
        |
        | foo""".stripMargin
        
      val tree @ Let(_, _, _, _,
        Let(_, _, _,
          Let(_, _, _, originA @ Where(_, targetA, Eq(_, solA, _)),
            Let(_, _, _, originB @ Where(_, targetB, Eq(_, solB, _)), _)),
          _)) = compile(input)
      
      val expected = IntersectBucketSpec(
        Group(originA, targetA,
          UnfixedSolution("'a", solA)),
        Group(originB, targetB,
          UnfixedSolution("'b", solB)))
      
      tree.buckets must beSome(expected)
      tree.errors must beEmpty
    }
    
    "identify separate buckets for independent tic variables on different sets" in {
      val input = """
        | clicks := load(//clicks)
        | imps := load(//impressions)
        | 
        | foo('a, 'b) :=
        |   bar := clicks where clicks.a = 'a
        |   baz := imps where imps.b = 'b
        |
        |   bar ~ baz
        |     bar.a + baz.b
        |
        | foo""".stripMargin
        
      val tree @ Let(_, _, _, _,
        Let(_, _, _, _,
          Let(_, _, _,
            Let(_, _, _, originA @ Where(_, targetA, Eq(_, solA, _)),
              Let(_, _, _, originB @ Where(_, targetB, Eq(_, solB, _)), _)),
          _))) = compile(input)
      
      val expected = IntersectBucketSpec(
        Group(originA, targetA,
          UnfixedSolution("'a", solA)),
        Group(originB, targetB,
          UnfixedSolution("'b", solB)))
      
      tree.buckets must beSome(expected)
      tree.errors must beEmpty
    }
    
    "reject shared buckets for dependent tic variables on the same set" in {
      {
        val input = """
          | organizations := load(//organizations)
          | 
          | hist('revenue, 'campaign) :=
          |   organizations' := organizations where organizations.revenue = 'revenue
          |   organizations'' := organizations' where organizations'.campaign = 'campaign
          |   
          |   organizations''
          |   
          | hist""".stripMargin
          
        val tree = compile(input)
        tree.errors must not(beEmpty)
      }
      
      {
        val input = """
          | campaigns := load(//campaigns)
          | organizations := load(//organizations)
          | 
          | hist('revenue, 'campaign) :=
          |   organizations' := organizations where organizations.revenue = 'revenue
          |   campaigns' := campaigns where campaigns.campaign = 'campaign
          |   organizations'' := organizations' where organizations'.campaign = 'campaign
          |   
          |   campaigns' ~ organizations''
          |     { revenue: 'revenue, num: count(campaigns') }
          |   
          | hist""".stripMargin
          
        val tree = compile(input)
        tree.errors must not(beEmpty)
      }
    }
  }
}
