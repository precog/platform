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
          origin @ Where(_, target, Eq(_, solution, _)), d: Dispatch)) = parse(input)
        
      d.buckets must contain("'day" -> Group(origin, target, Definition(solution), Set()))
      tree.errors must beEmpty
    }
    
    "identify composite bucket for trivial cf example with conjunction" in {
      val input = "clicks := load(//clicks) onDay('day) := clicks where clicks.day = 'day & clicks.din = 'day onDay"
      
      val tree @ Let(_, _, _, _,
        Let(_, _, _, 
          origin @ Where(_, target, And(_, Eq(_, leftSol, _), Eq(_, rightSol, _))), d: Dispatch)) = parse(input)
      
      val bucket = Group(origin, target,
        Conjunction(Definition(leftSol), Definition(rightSol)), Set())
      
      d.buckets must contain("'day" -> bucket)
      tree.errors must beEmpty
    }
  }
}
