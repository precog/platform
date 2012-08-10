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
package quirrel
package typer

import bytecode.RandomLibrary
import com.codecommit.gll.LineStream
import org.specs2.mutable.Specification

import java.io.File
import scala.io.Source

object ProvenanceCheckingSpecs extends Specification
    with StubPhases
    with Compiler
    with ProvenanceChecker 
    with RandomLibrary {

  import ast._
  
  "provenance checking" should {
    "reject object definition on different loads" in {
      val tree = compile("{ a: //foo, b: //bar }")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject object definition on static and dynamic provenances" in {
      val tree = compile("{ a: //foo, b: new 1 }")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject object definition on differing dynamic provenances" in {
      val tree = compile("{ a: new 1, b: new 1 }")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject array definition on different loads" in {
      val tree = compile("[ //foo, //bar ]")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject array definition on static and dynamic provenances" in {
      val tree = compile("[ //foo, new 1 ]")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject array definition on differing dynamic provenances" in {
      val tree = compile("[ new 1, new 1 ]")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject deref on different loads" in {
      val tree = compile("//foo[//bar]")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject deref on static and dynamic provenances" in {
      val tree = compile("//foo[new 1]")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject deref on differing dynamic provenances" in {
      val tree = compile("(new 1)[new 1]")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject dispatch on different loads" in {
      val tree = compile("fun('a, 'b) := 'a + 'b fun(//foo, //bar)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject dispatch on static and dynamic provenances" in {
      val tree = compile("fun('a, 'b) := 'a + 'b fun(//foo, new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject dispatch on differing dynamic provenances" in {
      val tree = compile("fun('a, 'b) := 'a + 'b fun(new 1, new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject dispatch to new-modified identity function with dynamic provenance" in {
      val tree = compile("fun('a) := 'a + new 42 fun(new 24)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(SetFunctionAppliedToSet)
    }
    
    "reject dispatch to new-modified identity function with static provenance" in {
      val tree = compile("fun('a) := 'a + new 42 fun(//foo)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(SetFunctionAppliedToSet)
    }
    
    "reject dispatch to load-modified identity function with dynamic provenance" in {
      val tree = compile("fun('a) := 'a + //foo fun(new 24)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(SetFunctionAppliedToSet)
    }
    
    "reject dispatch to load-modified identity function with static provenance" in {
      val tree = compile("fun('a) := 'a + //foo fun(//foo)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(SetFunctionAppliedToSet)
    }
    
    "reject dispatch to load-modified identity function with union provenance" in {
      val input = """
        | foo := //foo
        | bar := //bar
        |
        | id('a, 'b) := 'a + 'b + foo
        |
        | foo ~ bar
        |   id(foo, bar)
        """.stripMargin
      val tree = compile(input)
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(SetFunctionAppliedToSet)
    }
    
    "reject dispatch to where-less value function with too few parameters" in {
      val tree = compile("fun('a) := 'a + 5 fun")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(UnspecifiedRequiredParams(Vector("'a")))
    }
    
    "reject dispatch to where-less static function with too few parameters" in {
      val tree = compile("fun('a) := 'a + //foo fun")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(UnspecifiedRequiredParams(Vector("'a")))
    }
    
    "reject dispatch to where-less dynamic function with too few parameters" in {
      val tree = compile("fun('a) := 'a + new 42 fun")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(UnspecifiedRequiredParams(Vector("'a")))
    }
    
    "reject dispatch with too many parameters" in {
      {
        val tree = compile("a := 42 a(1)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(IncorrectArity(0, 1))
      }
      
      {
        val tree = compile("a := //foo a(1)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(IncorrectArity(0, 1))
      }
      
      {
        val tree = compile("a := 42 a(1, 2)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(IncorrectArity(0, 2))
      }
      
      {
        val tree = compile("a('b) := 'b a(1, 2)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(IncorrectArity(1, 2))
      }
      
      {
        val tree = compile("a('b) := 'b + //foo a(1, 2)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(IncorrectArity(1, 2))
      }
      
      {
        val tree = compile("a('b, 'c, 'd) := 'b + 'c + 'd a(1, 2, 3, 4, 5)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(IncorrectArity(3, 5))
      }
      
      {
        val tree = compile("a('b, 'c, 'd) := 'b + 'c + 'd + //foo a(1, 2, 3, 4, 5)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(IncorrectArity(3, 5))
      }
    }
    
    "reject dispatch to a built-in function with the wrong number of parameters" in {
      "count" >> {
        {
          val tree = compile("count")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(IncorrectArity(1, 0))
        }
        
        {
          val tree = compile("count(1, 2)")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(IncorrectArity(1, 2))
        }
        
        {
          val tree = compile("count(1, 2, 3)")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(IncorrectArity(1, 3))
        }
      }      
      
      "geometricMean" >> {
        {
          val tree = compile("geometricMean")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(IncorrectArity(1, 0))
        }
        
        {
          val tree = compile("geometricMean(1, 2)")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(IncorrectArity(1, 2))
        }
        
        {
          val tree = compile("geometricMean(1, 2, 3)")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(IncorrectArity(1, 3))
        }
      }
      
      "load" >> {
        {
          val tree = compile("load")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(IncorrectArity(1, 0))
        }
        
        {
          val tree = compile("load(1, 2)")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(IncorrectArity(1, 2))
        }
        
        {
          val tree = compile("load(1, 2, 3)")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(IncorrectArity(1, 3))
        }
      }
      
      "max" >> {
        {
          val tree = compile("max")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(IncorrectArity(1, 0))
        }
        
        {
          val tree = compile("max(1, 2)")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(IncorrectArity(1, 2))
        }
        
        {
          val tree = compile("max(1, 2, 3)")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(IncorrectArity(1, 3))
        }
      }
      
      "mean" >> {
        {
          val tree = compile("mean")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(IncorrectArity(1, 0))
        }
        
        {
          val tree = compile("mean(1, 2)")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(IncorrectArity(1, 2))
        }
        
        {
          val tree = compile("mean(1, 2, 3)")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(IncorrectArity(1, 3))
        }
      }
      
      "median" >> {
        {
          val tree = compile("median")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(IncorrectArity(1, 0))
        }
        
        {
          val tree = compile("median(1, 2)")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(IncorrectArity(1, 2))
        }
        
        {
          val tree = compile("median(1, 2, 3)")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(IncorrectArity(1, 3))
        }
      }
      
      "min" >> {
        {
          val tree = compile("min")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(IncorrectArity(1, 0))
        }
        
        {
          val tree = compile("min(1, 2)")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(IncorrectArity(1, 2))
        }
        
        {
          val tree = compile("min(1, 2, 3)")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(IncorrectArity(1, 3))
        }
      }
      
      "mode" >> {
        {
          val tree = compile("mode")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(IncorrectArity(1, 0))
        }
        
        {
          val tree = compile("mode(1, 2)")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(IncorrectArity(1, 2))
        }
        
        {
          val tree = compile("mode(1, 2, 3)")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(IncorrectArity(1, 3))
        }
      }
      
      "stdDev" >> {
        {
          val tree = compile("stdDev")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(IncorrectArity(1, 0))
        }
        
        {
          val tree = compile("stdDev(1, 2)")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(IncorrectArity(1, 2))
        }
        
        {
          val tree = compile("stdDev(1, 2, 3)")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(IncorrectArity(1, 3))
        }
      }
      
      "sum" >> {
        {
          val tree = compile("sum")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(IncorrectArity(1, 0))
        }
        
        {
          val tree = compile("sum(1, 2)")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(IncorrectArity(1, 2))
        }
        
        {
          val tree = compile("sum(1, 2, 3)")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(IncorrectArity(1, 3))
        }
      }  

     "sumSq" >> {
        {
          val tree = compile("sumSq")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(IncorrectArity(1, 0))
        }
        
        {
          val tree = compile("sumSq(1, 2)")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(IncorrectArity(1, 2))
        }
        
        {
          val tree = compile("sumSq(1, 2, 3)")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(IncorrectArity(1, 3))
        }
      }    
      
      "variance" >> {
        {
          val tree = compile("variance")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(IncorrectArity(1, 0))
        }
        
        {
          val tree = compile("variance(1, 2)")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(IncorrectArity(1, 2))
        }
        
        {
          val tree = compile("variance(1, 2, 3)")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(IncorrectArity(1, 3))
        }
      }

      "distinct" >> {
        {
          val tree = compile("distinct")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(IncorrectArity(1, 0))
        }
        
        {
          val tree = compile("distinct(1, 2)")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(IncorrectArity(1, 2))
        }
        
        {
          val tree = compile("distinct(1, 2, 3)")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(IncorrectArity(1, 3))
        }
      }
    }
    
    "reject dispatch to a set function with set parameters" in {
      {
        val tree = compile("a('b) := 'b + //foo a(//foo)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(SetFunctionAppliedToSet)
      }
      
      {
        val tree = compile("a('b) := 'b + //foo a(//bar)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(SetFunctionAppliedToSet)
      }
      
      {
        val tree = compile("a('b, 'c) := 'b + 'c + //foo a(//foo, //foo)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(SetFunctionAppliedToSet)
      }
      
      {
        val tree = compile("a('b, 'c) := 'b + 'c + //foo a(//bar, //bar)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(SetFunctionAppliedToSet)
      }
      
      {
        val tree = compile("a('b, 'c) := 'b + 'c + //foo a(new 2, 42)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(SetFunctionAppliedToSet)
      }
      
      {
        val tree = compile("a('b) := 'b + new 1 a(//foo)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(SetFunctionAppliedToSet)
      }
      
      {
        val tree = compile("a('b) := 'b + new 1 a(//bar)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(SetFunctionAppliedToSet)
      }
      
      {
        val tree = compile("a('b, 'c) := 'b + 'c + new 1 a(//foo, //foo)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(SetFunctionAppliedToSet)
      }
      
      {
        val tree = compile("a('b, 'c) := 'b + 'c + new 1 a(//bar, //bar)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(SetFunctionAppliedToSet)
      }
      
      {
        val tree = compile("a('b, 'c) := 'b + 'c + new 1 a(new 2, 42)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(SetFunctionAppliedToSet)
      }
    }
    
    "reject where on different loads" in {
      val tree = compile("//foo where //bar")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }

    "reject operations on different loads through where" in {
      val rawInput = """
        | a := //users
        | b := //heightWeight
        | { name: a.name, height: b.height } where a.userId = b.userId """.stripMargin
        
      val tree = compile(rawInput)
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject where on static and dynamic provenances" in {
      val tree = compile("//foo where new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject where on differing dynamic provenances" in {
      val tree = compile("new 1 where new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject with on different loads" in {
      val tree = compile("//foo with //bar")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject with on static and dynamic provenances" in {
      val tree = compile("//foo with new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject with on differing dynamic provenances" in {
      val tree = compile("(new 1) with (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "accept union on different loads" in {
      val tree = compile("//foo union //bar")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty
    }
    
    "accept union on static and dynamic provenances" in {
      val tree = compile("//foo union new 1")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty
    }
    
    "accept union on differing dynamic provenances" in {
      val tree = compile("(new 1) union (new 1)")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty
    }
    
    "accept intersect on different loads" in {
      val tree = compile("//foo intersect //bar")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty
    }
    
    "accept intersect on static and dynamic provenances" in {
      val tree = compile("//foo intersect new 1")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty
    }
    
    "accept intersect on differing dynamic provenances" in {
      val tree = compile("(new 1) intersect (new 1)")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty
    }    

    "accept difference on different loads" in {
      val tree = compile("//foo difference //bar")
      tree.provenance must beLike { case StaticProvenance("/foo") => ok }
      tree.errors must beEmpty
    }
    
    "accept difference on static and dynamic provenances" in {
      val tree = compile("//foo difference new 1")
      tree.provenance must beLike { case StaticProvenance("/foo") => ok }
      tree.errors must beEmpty
    }

    "accept difference on static and dynamic provenances" in {
      val tree = compile("(new 1) difference //foo")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty
    }
    
    "accept difference on differing dynamic provenances" in {
      val tree = compile("(new 1) difference (new 1)")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty
    }    

    "reject addition on different loads" in {
      val tree = compile("//foo + //bar")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject addition on static and dynamic provenances" in {
      val tree = compile("//foo + new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject addition on differing dynamic provenances" in {
      val tree = compile("(new 1) + (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject subtraction on different loads" in {
      val tree = compile("//foo - //bar")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject subtraction on static and dynamic provenances" in {
      val tree = compile("//foo - new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject subtraction on differing dynamic provenances" in {
      val tree = compile("(new 1) - (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject multiplication on different loads" in {
      val tree = compile("//foo * //bar")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject multiplication on static and dynamic provenances" in {
      val tree = compile("//foo * new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject multiplication on differing dynamic provenances" in {
      val tree = compile("(new 1) * (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject division on different loads" in {
      val tree = compile("//foo / //bar")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject division on static and dynamic provenances" in {
      val tree = compile("//foo / new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject division on differing dynamic provenances" in {
      val tree = compile("(new 1) / (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject less-than on different loads" in {
      val tree = compile("//foo < //bar")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject less-than on static and dynamic provenances" in {
      val tree = compile("//foo < new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject less-than on differing dynamic provenances" in {
      val tree = compile("(new 1) < (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject less-than-equal on different loads" in {
      val tree = compile("//foo <= //bar")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject less-than-equal on static and dynamic provenances" in {
      val tree = compile("//foo <= new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject less-than-equal on differing dynamic provenances" in {
      val tree = compile("(new 1) <= (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject greater-than on different loads" in {
      val tree = compile("//foo > //bar")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject greater-than on static and dynamic provenances" in {
      val tree = compile("//foo > new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject greater-than on differing dynamic provenances" in {
      val tree = compile("(new 1) > (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject greater-than-equal on different loads" in {
      val tree = compile("//foo >= //bar")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject greater-than-equal on static and dynamic provenances" in {
      val tree = compile("//foo >= new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject greater-than-equal on differing dynamic provenances" in {
      val tree = compile("(new 1) >= (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject equality on different loads" in {
      val tree = compile("//foo = //bar")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject equality on static and dynamic provenances" in {
      val tree = compile("//foo = new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject equality on differing dynamic provenances" in {
      val tree = compile("(new 1) = (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject not-equality on different loads" in {
      val tree = compile("//foo != //bar")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject not-equality on static and dynamic provenances" in {
      val tree = compile("//foo != new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject not-equality on differing dynamic provenances" in {
      val tree = compile("(new 1) != (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject boolean and on different loads" in {
      val tree = compile("//foo & //bar")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject boolean and on static and dynamic provenances" in {
      val tree = compile("//foo & new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject boolean and on differing dynamic provenances" in {
      val tree = compile("(new 1) & (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject boolean or on different loads" in {
      val tree = compile("//foo | //bar")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject boolean or on static and dynamic provenances" in {
      val tree = compile("//foo | new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject boolean or on differing dynamic provenances" in {
      val tree = compile("(new 1) | (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
  }
}
