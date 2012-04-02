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
      
  "group solver" should {
    "identify and solve group set for trivial cf example" in {
      val input = "clicks := load(//clicks) onDay('day) := clicks where clicks.day = 'day onDay"
      
      val tree @ Let(_, _, _, _,
        Let(_, _, _, 
          origin @ Where(_, target, Eq(_, solution, _)), d: Dispatch)) = compile(input)
        
      d.buckets must contain("'day" -> Group(origin, target, Definition(solution), Set()))
      tree.errors must beEmpty
    }
    
    "identify composite bucket for trivial cf example with conjunction" in {
      val input = "clicks := load(//clicks) onDay('day) := clicks where clicks.day = 'day & clicks.din = 'day onDay"
      
      val tree @ Let(_, _, _, _,
        Let(_, _, _, 
          origin @ Where(_, target, And(_, Eq(_, leftSol, _), Eq(_, rightSol, _))), d: Dispatch)) = compile(input)
      
      val bucket = Group(origin, target,
        Conjunction(Definition(leftSol), Definition(rightSol)), Set())
      
      d.buckets must contain("'day" -> bucket)
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
          d: Dispatch)) = compile(input)
      
      val bucketA = Group(originA, targetA, Definition(solA), Set())
      val bucketB = Group(originB, targetB, Definition(solB), Set())
      
      d.buckets mustEqual Map("'a" -> bucketA, "'b" -> bucketB)
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
          d: Dispatch))) = compile(input)
      
      val bucketA = Group(originA, targetA, Definition(solA), Set())
      val bucketB = Group(originB, targetB, Definition(solB), Set())
      
      d.buckets mustEqual Map("'a" -> bucketA, "'b" -> bucketB)
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
