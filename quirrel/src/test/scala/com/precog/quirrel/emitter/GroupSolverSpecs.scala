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
  }
}
