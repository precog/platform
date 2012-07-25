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

object ProvenanceSpecs extends Specification
    with StubPhases
    with Compiler
    with ProvenanceChecker 
    with RandomLibrary {

  import ast._
  
  "provenance computation" should {
    "compute result provenance correctly in BIF1" in {
      forall(lib1) { f =>
        val tree = parse("""
          clicks := //clicks
          foo('a) := %s('a) 
          foo(clicks)""".format(f.fqn))

        tree.provenance mustEqual StaticProvenance("/clicks")
        tree.errors must beEmpty
      }
    }       
    
    "compute result provenance correctly in BIF2" in {
      forall(lib2) { f =>
        val tree = parse("""
          clicks := //clicks
          foo('a, 'b) := %s('a, 'b) 
          foo(clicks.a, clicks.b)""".format(f.fqn))

        tree.provenance mustEqual StaticProvenance("/clicks")
        tree.errors must beEmpty
      }
    }     

    "compute result provenance correctly in a BIR" in {
      forall(libReduction) { f =>
        val tree = parse("""
          clicks := //clicks
          foo('a) := %s('a) 
          foo(clicks.a)""".format(f.fqn))

        tree.provenance mustEqual ValueProvenance

        tree.errors must beEmpty
      }
    }    

    "identify let according to its right expression" in {   // using raw, no-op let
      {
        val tree = parse("a := 1 1")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("a := 1 //foo")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("a := 1 (new 1)")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      {
        val tree = parse("a := 1 (distinct(1))")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "preserve provenance through let for unquantified function" in {
      val input = """
        | interactions := //interactions
        | bounds('it) :=
        |   interactions.time where interactions = 'it
        | init := bounds
        | init + bounds""".stripMargin
        
      val tree = compile(input)
      
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty
    }
    
    "identify new as dynamic" in {
      val tree = compile("new 1")
      tree.provenance must beLike {
        case DynamicProvenance(_) => ok
      }
      tree.errors must beEmpty
    }
    
    "identify new of unquantified function as distinct from the function" in {
      val input = """
        | histogram('a) :=
        |   'a + count(//foo where //foo = 'a)
        | 
        | histogram' := new histogram
        | 
        | histogram'""".stripMargin
      
      val tree @ Let(_, _, _, _, Let(_, _, _, New(_, target), result)) = compile(input)
      
      target.provenance must beLike { case DynamicProvenance(_) => ok }
      result.provenance must beLike { case DynamicProvenance(_) => ok }
      target.provenance mustNotEqual result.provenance
      
      tree.errors must beEmpty
    }
    
    "identify relate according to its last expression" in {
      {
        val tree = compile("//a ~ //b 3")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("//a ~ //b //foo")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("//a ~ //b (new 1)")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify tic-var as value" in {
      val tree @ Let(_, _, _, body, _) = parse("a('foo) := 'foo a(42)")    // uses raw tic-var
      body.provenance mustEqual ValueProvenance
      tree.errors must beEmpty
    }
    
    "identify string as value" in {
      val tree = compile("\"foo\"")
      tree.provenance mustEqual ValueProvenance
      tree.errors must beEmpty
    }
    
    "identify num as value" in {
      val tree = compile("42")
      tree.provenance mustEqual ValueProvenance
      tree.errors must beEmpty
    }
    
    "identify boolean as value" in {
      val tree = compile("true")
      tree.provenance mustEqual ValueProvenance
      tree.errors must beEmpty
    }    

    "identify null as value" in {
      val tree = compile("null")
      tree.provenance mustEqual ValueProvenance
      tree.errors must beEmpty
    }
    
    "identify empty object definitions as value" in {
      val tree = compile("{}")
      tree.provenance mustEqual ValueProvenance
      tree.errors must beEmpty
    }
    
    "identify object definition according to its properties" in {
      {
        val tree = compile("{ a: 1, b: 2, c: 3}")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("{ a: 1, b: 2, c: //foo }")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("{ a: 1, b: 2, c: new 2 }")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify empty array definitions as value" in {
      val tree = compile("[]")
      tree.provenance mustEqual ValueProvenance
      tree.errors must beEmpty
    }
    
    "identify array definition according to its values" in {
      {
        val tree = compile("[1, 2, 3]")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("[1, 2, //foo]")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("[1, 2, new 3]")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify descent according to its child expression" in {
      {
        val tree = compile("1.foo")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("//bar.foo")
        tree.provenance mustEqual StaticProvenance("/bar")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("(new 1).foo")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify dereference according to its children" in {
      {
        val tree = compile("1[2]")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("//foo[2]")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1[//foo]")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("(new 1)[2]")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1[new 2]")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    // TODO arity
    "identify built-in reduce dispatch as value" in {
      {
        val tree = compile("count(//foo)")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }     
      
      {
        val tree = compile("geometricMean(//foo)")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("max(//foo)")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("mean(//foo)")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("median(//foo)")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("min(//foo)")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("mode(//foo)")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("stdDev(//foo)")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("sum(//foo)")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("sumSq(//foo)")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }     
      
      {
        val tree = compile("variance(//foo)")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
    }

    "identify distinct dispatch" in {
      {
        val tree = compile("distinct(//foo)")
        tree.provenance must beLike { case DynamicProvenance(_) => ok }
        tree.errors must beEmpty
       
      }
    }

    "identify op1 dispatch according to its child" in {
      forall(lib1) { f =>
        val tree = compile("%s(//foo)".format(f.fqn))
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
    }

    "identify op1 dispatch given incorrect number of parameters" in {
      forall(lib1) { f =>
        val tree = compile("%s(//foo, //bar)".format(f.fqn))
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(IncorrectArity(1, 2))
      }
    }

    "identify op2 dispatch according to its children given unrelated sets" in {
      forall(lib2) { f => 
        val tree = compile("%s(//foo, //bar)".format(f.fqn))
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }

    "identify op2 dispatch according to its children given a load and a value" in {
      forall(lib2) { f =>
        val tree = compile("""%s(//foo, "bar")""".format(f.fqn))
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
    }

    "identify op2 dispatch according to its children given set related by ~" in {
      forall(lib2) { f =>
        val tree = compile("""//foo ~ //bar %s(//foo, //bar)""".format(f.fqn))
        tree.provenance must beLike { case DynamicProvenance(_) => ok }
        tree.errors must beEmpty
      }
    }
    
    "identify load dispatch with static params according to its path" in {
      {
        val tree = compile("//foo")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("//bar")
        tree.provenance mustEqual StaticProvenance("/bar")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("//bar/baz")
        tree.provenance mustEqual StaticProvenance("/bar/baz")
        tree.errors must beEmpty
      }
    }
    
    "identify load dispatch with non-static params as dynamic" in {
      {
        val tree = compile("load(42)")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("a := 42 load(a)")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("load(count(42))")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("load(new 42)")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify dispatch to identity function by parameter" in {
      {
        val tree = compile("id('a) := 'a id(42)")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("id('a) := 'a id(new 42)")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("id('a) := 'a id(//foo)")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }

    }
    
    "identify dispatch to value-modified identity function by parameter" in {
      {
        val tree = compile("id('a) := 'a + 5 id(42)")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("id('a) := 'a + 5 id(new 42)")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("id('a) := 'a + 5 id(//foo)")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
    }
    
    "identify dispatch to new-modified identity function as dynamic" in {
      val tree = compile("id('a) := 'a + new 42 id(24)")
      tree.provenance must beLike {
        case DynamicProvenance(_) => ok
      }
      tree.errors must beEmpty
    }
    
    "identify dispatch to load-modified identity function as static" in {
      val tree = compile("id('a) := 'a + //foo id(24)")
      tree.provenance mustEqual StaticProvenance("/foo")
      tree.errors must beEmpty
    }
    
    "identify dispatch to simple operation function by unification of parameters" in {
      {
        val tree = compile("fun('a, 'b) := 'a + 'b fun(1, 2)")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("fun('a, 'b) := 'a + 'b fun(//foo, 2)")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("fun('a, 'b) := 'a + 'b fun(1, //foo)")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("fun('a, 'b) := 'a + 'b fun(//foo, //foo)")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("fun('a, 'b) := 'a + 'b //foo ~ //bar fun(//foo, //bar)")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
     
    "identify a case when a tic variable is not solvable in all cases" in {
      {
        val tree = compile("""
        | a('b) :=
        |   k := //clicks.time where //clicks.time = 'b
        |   j := //views.time where //views.time > 'b
        |   k ~ j
        |   {kay: k, jay: j}
        | a""".stripMargin)

        tree.provenance must beLike { case DynamicProvenance(_) => ok }
        tree.errors must beEmpty
      }
    }

    "identify dispatch to an unquantified value function as dynamic" in {
      {
        val tree = compile("histogram('a) := 'a + count(//foo where //foo = 'a) histogram")
        tree.provenance must beLike { case DynamicProvenance(_) => ok }
        tree.errors must beEmpty
      }
      
      {
        val input = """
          | histogram('a) :=
          |   foo := //foo
          |   bar := //bar
          |   
          |   'a + count(foo ~ bar foo where foo = 'a & bar = 12)
          | 
          | histogram""".stripMargin
        
        val tree = compile(input)
        tree.provenance must beLike { case DynamicProvenance(_) => ok }
        tree.errors must beEmpty
      }
      
      {
        val input = """
          | histogram('a) :=
          |   foo := //foo
          |   bar := //bar
          |   
          |   foo' := foo where foo = 'a
          |   bar' := bar where bar = 'a
          | 
          |   'a + count(foo' ~ bar' foo + bar)
          | 
          | histogram""".stripMargin
        
        val tree = compile(input)
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify dispatch to an unquantified function with relate as dynamic" in {
      val input = """
        | fun('a) :=
        |   foo := //foo
        |   bar := //bar
        |
        |   foo' := foo where foo = 'a
        |   bar' := bar where bar = 'a
        |
        |   foo' ~ bar'
        |     foo.left + bar.right
        |
        | fun""".stripMargin
        
      val tree = compile(input)
      tree.provenance must beLike {
        case DynamicProvenance(_) => ok
      }
      tree.errors must beEmpty
    }

    "identify dispatch to unquantified function with a consistent dynamic provenance" in {
      val tree = compile("histogram('a) := 'a + count(//foo where //foo = 'a) histogram + histogram")   // if not consistent, binary op will fail
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty
    }
    
    "identify where according to its children" in {
      {
        val tree = compile("1 where 2")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("//foo where 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 where //foo")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("new 1 where 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 where new 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }    
    "identify with according to its children" in {
      {
        val tree = compile("1 with 2")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("//foo with 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 with //foo")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("new 1 with 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 with new 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    } 

    "determine accumulatedProvenance in all cases" >> {   
      "Let" >> {
        {
          val tree = compile("a := 1 a")
          tree.accumulatedProvenance must beLike { case Some(Vector()) => ok }
          tree.errors must beEmpty
        }          
        {
          val tree = compile("a := 1 foo(2)")
          tree.accumulatedProvenance must beLike { case None => ok }
          tree.errors mustEqual Set(UnusedLetBinding(Identifier(Vector(), "a")), UndefinedFunction(Identifier(Vector(), "foo")))
        }          
        {
          val tree = compile("a := 1 42")
          tree.accumulatedProvenance must beLike { case Some(Vector()) => ok }
          tree.errors mustEqual Set(UnusedLetBinding(Identifier(Vector(), "a")))
        }        
        {
          val tree = compile("a := [1,2,3] a")
          tree.accumulatedProvenance must beLike { case Some(Vector()) => ok }
          tree.errors must beEmpty
        } 
        {
          val tree = compile("a := {foo: null} a")
          tree.accumulatedProvenance must beLike { case Some(Vector()) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("a := //foo a")
          tree.accumulatedProvenance must beLike { case Some(Vector(StaticProvenance(_))) => ok }
          tree.errors must beEmpty
        }          
        {
          val tree = compile("foo('a) := 'a + 5 foo(13)")
          tree.accumulatedProvenance must beLike { case Some(Vector()) => ok }
          tree.errors must beEmpty
        }        
        {
          val tree = compile("foo('a) := 'a + 5 foo(//baz)")
          tree.accumulatedProvenance must beLike { case Some(Vector(StaticProvenance("/baz"))) => ok }
          tree.errors must beEmpty
        }          
        {
          val tree = compile("foo('a) := 'a + 5 bar::bak(//baz)")
          tree.accumulatedProvenance must beLike { case None => ok }
          tree.errors mustEqual Set(UndefinedFunction(Identifier(Vector("bar"), "bak")), UnusedLetBinding(Identifier(Vector(), "foo")))
        }         
        {
          val tree = compile("foo('a) := 'a + 5 true")
          tree.accumulatedProvenance must beLike { case Some(Vector()) => ok }
          tree.errors mustEqual Set(UnusedLetBinding(Identifier(Vector(), "foo")))
        }          
        {
          val tree = compile("foo('a, 'b) := 'a + 'b + 5 foo(1, 2)")
          tree.accumulatedProvenance must beLike { case Some(Vector()) => ok }
          tree.errors must beEmpty
        }         
        {
          val tree = compile("foo('a, 'b) := 'a + 'b + 5 foo(1, //baz)")
          tree.accumulatedProvenance must beLike { case Some(Vector(StaticProvenance("/baz"))) => ok }
          tree.errors must beEmpty
        }   
        {
          val tree = compile("foo('a, 'b) := 'a + 'b + 5 foo(//bar, //baz)")
          tree.accumulatedProvenance must beLike { case None => ok }
          tree.errors mustEqual Set(OperationOnUnrelatedSets)
        }         
        {
          val tree = compile("foo := [//bar] foo")
          tree.accumulatedProvenance must beLike { case Some(Vector(StaticProvenance("/bar"))) => ok }
          tree.errors must beEmpty
        }        
        {
          val tree = compile("""
            | clicks := new //clicks
            | clicks where true""".stripMargin)
          tree.accumulatedProvenance must beLike { case Some(Vector(DynamicProvenance(_))) => ok }
          tree.errors must beEmpty
        }         
        {
          val tree = compile("""
            | clicks := new //clicks
            | clicks where clicks""".stripMargin)
          tree.accumulatedProvenance must beLike { case Some(Vector(DynamicProvenance(_))) => ok }
          tree.errors must beEmpty
        } 
      }

      "New" >> {
        {
          val tree = compile("new 123")
          tree.accumulatedProvenance must beLike { case Some(Vector(DynamicProvenance(_))) => ok }
          tree.errors must beEmpty
        }        
        {
          val tree = compile("new [1,2,3]")
          tree.accumulatedProvenance must beLike { case Some(Vector(DynamicProvenance(_))) => ok }
          tree.errors must beEmpty
        } 
        {
          val tree = compile("new //foobar")
          tree.accumulatedProvenance must beLike { case Some(Vector(DynamicProvenance(_))) => ok }
          tree.errors must beEmpty
        } 
        {
          val tree = compile("new foo(42)")
          tree.accumulatedProvenance must beLike { case Some(Vector(DynamicProvenance(_))) => ok } 
          tree.errors mustEqual Set(UndefinedFunction(Identifier(Vector(), "foo")))
        }
      }

      "Relate" >> {
        {
          val tree = compile("//foo ~ //bar 5")
          tree.accumulatedProvenance must beLike { case Some(Vector()) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("//foo ~ //bar //bar")
          tree.accumulatedProvenance must beLike { case Some(Vector(StaticProvenance("/bar"))) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("//foo ~ //bar //foo + //bar")
          tree.accumulatedProvenance must beLike { case Some(Vector(StaticProvenance("/foo"), StaticProvenance("/bar"))) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("//foo ~ //bar //foo + //baz")
          tree.accumulatedProvenance must beLike { case  Some(Vector(StaticProvenance("/foo"), StaticProvenance("/baz"))) => ok } 
          tree.errors mustEqual Set(OperationOnUnrelatedSets)
        }
        {
          val tree = compile("//foo ~ //bar ~ //baz 5")
          tree.accumulatedProvenance must beLike { case Some(Vector()) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("//foo ~ bar(9) 5")
          tree.accumulatedProvenance must beLike { case None => ok }  
          tree.errors mustEqual Set(UndefinedFunction(Identifier(Vector(), "bar")))
        }
        {
          val tree = compile("//foo ~ 10 //foo")
          tree.accumulatedProvenance must beLike { case None => ok }
          tree.errors mustEqual Set(AlreadyRelatedSets)
        }
        {
          val tree = compile("//foo ~ //foo.bar 5")
          tree.accumulatedProvenance must beLike { case None => ok }
          tree.errors mustEqual Set(AlreadyRelatedSets)
        }
      }

      "TicVar and Literals" >> {
        {
          val tree = compile(""" "foo" """)
          tree.accumulatedProvenance must beLike { case Some(Vector()) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("2222")
          tree.accumulatedProvenance must beLike { case Some(Vector()) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("true")
          tree.accumulatedProvenance must beLike { case Some(Vector()) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("null")
          tree.accumulatedProvenance must beLike { case Some(Vector()) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("'a")
          tree.accumulatedProvenance must beLike { case Some(Vector()) => ok }
          tree.errors mustEqual Set(UndefinedTicVariable("'a"))
        }
        {
          val tree = compile("foo('a) := 'a foo(10)")
          tree.accumulatedProvenance must beLike { case Some(Vector()) => ok }
          tree.errors must beEmpty
        }
      }

      "ObjectDef" >> {
        {
          val tree = compile("{foo: 5}")
          tree.accumulatedProvenance must beLike { case Some(Vector()) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("{foo: //bar.baz}")
          tree.accumulatedProvenance must beLike { case Some(Vector(StaticProvenance("/bar"))) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("{foo1: //bar.baz, foo2: //bar.biz}")
          tree.accumulatedProvenance must beLike { case Some(Vector(StaticProvenance("/bar"))) => ok }
          tree.errors must beEmpty
        }        
        {
          val tree = compile("{foo1: //bar.baz, foo2: //ack}")
          tree.accumulatedProvenance must beLike { case Some(Vector(StaticProvenance("/bar"), StaticProvenance("/ack"))) => ok }
          tree.errors mustEqual Set(OperationOnUnrelatedSets)
        }
        {
          val tree = compile("{foo1: //bar.baz, foo2: 99}")
          tree.accumulatedProvenance must beLike { case Some(Vector(StaticProvenance("/bar"))) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("{}")
          tree.accumulatedProvenance must beLike { case Some(Vector()) => ok }
          tree.errors must beEmpty
        }
      }

      "ArrayDef" >> {
        {
          val tree = compile("[5]")
          tree.accumulatedProvenance must beLike { case Some(Vector()) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("[//bar.baz]")
          tree.accumulatedProvenance must beLike { case Some(Vector(StaticProvenance("/bar"))) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("[//bar.baz, //bar.biz]")
          tree.accumulatedProvenance must beLike { case Some(Vector(StaticProvenance("/bar"))) => ok }
          tree.errors must beEmpty
        }        
        {
          val tree = compile("[//bar.baz, //ack]")
          tree.accumulatedProvenance must beLike { case Some(Vector(StaticProvenance("/bar"), StaticProvenance("/ack"))) => ok }
          tree.errors mustEqual Set(OperationOnUnrelatedSets)
        }
        {
          val tree = compile("[//bar.baz, 99]")
          tree.accumulatedProvenance must beLike { case Some(Vector(StaticProvenance("/bar"))) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("[]")
          tree.accumulatedProvenance must beLike { case Some(Vector()) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("[foo(5), 1]")
          tree.accumulatedProvenance must beLike { case None => ok }
          tree.errors mustEqual Set(UndefinedFunction(Identifier(Vector(), "foo")))
        }
      }

      "Descent" >> {
        {
          val tree = compile("//foo.bar")
          tree.accumulatedProvenance must beLike { case Some(Vector(StaticProvenance("/foo"))) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("""{foo: "bar"}.foo""")
          tree.accumulatedProvenance must beLike { case Some(Vector()) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("{foo: //bar}.foo")
          tree.accumulatedProvenance must beLike { case Some(Vector(StaticProvenance("/bar"))) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("{foo: 1, bar: //baz}.foo")
          tree.accumulatedProvenance must beLike { case Some(Vector(StaticProvenance("/baz"))) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("{foo: //fob, bar: //baz}.bar")
          tree.accumulatedProvenance must beLike { case Some(Vector(StaticProvenance("/fob"), StaticProvenance("/baz"))) => ok }
          tree.errors mustEqual Set(OperationOnUnrelatedSets)
        }
        {
          val tree = compile("{foo: bar(5), baz: 1}.baz")
          tree.accumulatedProvenance must beLike { case None => ok }
          tree.errors mustEqual Set(UndefinedFunction(Identifier(Vector(), "bar")))
        }        
        {
          val tree = compile("true.foo")
          tree.accumulatedProvenance must beLike { case Some(Vector()) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("[1,2,3].foo")
          tree.accumulatedProvenance must beLike { case Some(Vector()) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("[].foo")
          tree.accumulatedProvenance must beLike { case Some(Vector()) => ok }
          tree.errors must beEmpty
        }
      }

      "Deref" >> {
        {
          val tree = compile("true[0]")
          tree.accumulatedProvenance must beLike { case Some(Vector()) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("//foo[2]")
          tree.accumulatedProvenance must beLike { case Some(Vector(StaticProvenance("/foo"))) => ok }
          tree.errors must beEmpty
        }       
        {
          val tree = compile("//foo.bar[2]")
          tree.accumulatedProvenance must beLike { case Some(Vector(StaticProvenance("/foo"))) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("foo := [2,3,4] foo[1]")
          tree.accumulatedProvenance must beLike { case Some(Vector()) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("{a: 1}[0]")
          tree.accumulatedProvenance must beLike { case Some(Vector()) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("{}[0]")
          tree.accumulatedProvenance must beLike { case Some(Vector()) => ok }
          tree.errors must beEmpty
        }        
        {
          val tree = compile("foo := [//bar] foo[0]")
          tree.accumulatedProvenance must beLike { case Some(Vector(StaticProvenance("/bar"))) => ok }
          tree.errors must beEmpty
        }
      }


      "Dispatch" >> {
        {
          val tree = compile("//foo")
          tree.accumulatedProvenance must beLike { case Some(Vector(StaticProvenance("/foo"))) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("""load("/foo")""")
          tree.accumulatedProvenance must beLike { case Some(Vector(StaticProvenance("/foo"))) => ok }  
          tree.errors must beEmpty
        }
        {
          val tree = compile("load(//foo)")
          tree.accumulatedProvenance must beLike { case Some(Vector(DynamicProvenance(_))) => ok }  
          tree.errors must beEmpty
        }
        {
          val tree = compile("load(//foo, bar)")
          tree.accumulatedProvenance must beLike { case None => ok }
          tree.errors mustEqual Set(IncorrectArity(1, 2), UndefinedFunction(Identifier(Vector(), "bar")))
        }
        {
          val tree = compile("distinct(//foo.a)")
          tree.accumulatedProvenance must beLike { case Some(Vector(DynamicProvenance(_))) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("distinct(//foo.a, //foo.b)")
          tree.accumulatedProvenance must beLike { case None => ok }
          tree.errors mustEqual Set(IncorrectArity(1, 2))
        }
        {
          val tree = compile("sum(//foo.a)")
          tree.accumulatedProvenance must beLike { case Some(Vector()) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("sum(//foo.a, //bar.b)")
          tree.accumulatedProvenance must beLike { case None => ok }
          tree.errors mustEqual Set(IncorrectArity(1, 2))
        }
        {
          forall(lib1) { f =>
            val tree = compile("%s(5)".format(f.fqn))
            tree.accumulatedProvenance must beLike { case Some(Vector()) => ok }
            tree.errors must beEmpty
          }
        }
        {
          forall(lib1) { f =>
            val tree = compile("%s(//faz)".format(f.fqn))
            tree.accumulatedProvenance must beLike { case Some(Vector(StaticProvenance("/faz"))) => ok }
            tree.errors must beEmpty
          }
        }
        {
          forall(lib1) { f =>
            val tree = compile("%s(//faz, //baz)".format(f.fqn))
            tree.accumulatedProvenance must beLike { case None => ok }
            tree.errors mustEqual Set(IncorrectArity(1, 2))
          }
        }
        {
          forall(lib2) { f =>
            val tree = compile("%s(5, 6)".format(f.fqn))
            tree.accumulatedProvenance must beLike { case Some(Vector()) => ok }
            tree.errors must beEmpty
          }
        }
        {
          forall(lib2) { f =>
            val tree = compile("%s(//faz.a, //faz.b)".format(f.fqn))
            tree.accumulatedProvenance must beLike { case Some(Vector(StaticProvenance("/faz"))) => ok }
            tree.errors must beEmpty
          }
        }        
        {
          forall(lib2) { f =>
            val tree = compile("%s(//faz.a, //baz.a)".format(f.fqn))
            tree.accumulatedProvenance must beLike { case Some(Vector(StaticProvenance("/faz"), StaticProvenance("/baz"))) => ok }
            tree.errors mustEqual Set(OperationOnUnrelatedSets)
          }
        }
        {
          forall(lib2) { f =>
            val tree = compile("%s(//faz.a, 55)".format(f.fqn))
            tree.accumulatedProvenance must beLike { case Some(Vector(StaticProvenance("/faz"))) => ok }
            tree.errors must beEmpty
          }
        }
        {
          forall(lib2) { f =>
            val tree = compile("%s(//faz)".format(f.fqn))
            tree.accumulatedProvenance must beLike { case None => ok }
            tree.errors mustEqual Set(IncorrectArity(2, 1))
         }
        }
        {
          val tree = compile("""bar("baz")""")
          tree.accumulatedProvenance must beLike { case None => ok }
          tree.errors mustEqual Set(UndefinedFunction(Identifier(Vector(), "bar")))
        }
        {
          val tree = compile("f('a, 'b) := 'a + 'b f(7)")
          tree.accumulatedProvenance must beLike { case None => ok }
          tree.errors mustEqual Set(UnspecifiedRequiredParams(Seq("'b")))  //note: error UnableToDetermineDefiningSets("'b") is generated in solveCriticalConditions, which is only stubbed here
        }
        {
          val tree = compile("f('a) := 'a + 2 f(4, 5)")
          tree.accumulatedProvenance must beLike { case None => ok }
          tree.errors mustEqual Set(IncorrectArity(1, 2))
        }
        {
          val tree = compile("f('a) := 'a + 2 f(//baz)")
          tree.accumulatedProvenance must beLike { case Some(Vector(StaticProvenance("/baz"))) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("f('a) := 'a + //baz f(2)")
          tree.accumulatedProvenance must beLike { case Some(Vector(StaticProvenance("/baz"))) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("f('a, 'b) := 'a + 'b f(new 2, //barbaz)")
          tree.accumulatedProvenance must beLike { case None => ok }
          tree.errors mustEqual Set(OperationOnUnrelatedSets)
        }          
        {
          val tree = compile("//baz ~ //bar f('a, 'b) := 'a + 'b f(//baz.x, //bar.x)")
          tree.accumulatedProvenance must beLike { case Some(Vector(StaticProvenance("/baz"), StaticProvenance("/bar"))) => ok }
          tree.errors must beEmpty
        }        
        {
          val tree = compile("f('a) := 'a + bar(43) f(9)")
          tree.accumulatedProvenance must beLike { case None => ok }
          tree.errors mustEqual Set(UndefinedFunction(Identifier(Vector(), "bar")))
        }        
        {
          val tree = compile("f('a) := 'a + 3 f(bar(baz))")
          tree.accumulatedProvenance must beLike { case None => ok }
          tree.errors mustEqual Set(UndefinedFunction(Identifier(Vector(), "baz")), UndefinedFunction(Identifier(Vector(), "bar")))
        }
        {
          val tree = compile("f := [new 3, new 4] f")
          tree.accumulatedProvenance must beLike { case None => ok }
          tree.errors mustEqual Set(OperationOnUnrelatedSets)
        }
        {
          val tree = compile("f('a, 'b) := //foo + 'a where //foo.b = 'b f(2)")
          tree.accumulatedProvenance must beLike { case Some(Vector(StaticProvenance("/foo"))) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("f('a) := 'a f(//bar)")
          tree.accumulatedProvenance must beLike { case Some(Vector(StaticProvenance("/bar"))) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("f('a) := //foobar where //foobar.a = 'a f(//bam)")
          tree.accumulatedProvenance must beLike { case None => ok }
          tree.errors mustEqual Set(SetFunctionAppliedToSet)
        }
        {
          val tree = compile("//foo ~ //bar f('a, 'b) := //foo + 'a where //foo.b = 'b f(//bar)")
          tree.accumulatedProvenance must beLike { case None => ok }
          tree.errors mustEqual Set(SetFunctionAppliedToSet)
        }        
        {
          val tree = compile("f('a, 'b) := //foo + 'a + 'b where //foo.b = 'b f(10)")
          tree.accumulatedProvenance must beLike { case Some(Vector(StaticProvenance("/foo"))) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("""
            foo := //foo
            bar := //bar
            f('b) := 
              foo' := foo where foo.b = 'b
              bar' := bar where bar.b = 'b
              foo' ~ bar'
              foo.x - bar.y
            f""")
          tree.accumulatedProvenance must beLike { case Some(Vector(StaticProvenance("/foo"), StaticProvenance("/bar"))) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("""
            clicks := //clicks
            views  := //views
            clickthroughRate('page) :=
              {page: 'page, ctr: count(clicks where clicks.pageId = 'page) / count(views where views.pageId = 'page)}
            clickthroughRate""")

          tree.accumulatedProvenance must beLike { case Some(Vector()) => ok }
          tree.errors must beEmpty
        }
      }

      "Where" >> {
        {
          val tree = compile("//foo where //foo.bar = 2")
          tree.accumulatedProvenance must beLike { case Some(Vector(StaticProvenance("/foo"))) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("10 where //foo.bar < 2")
          tree.accumulatedProvenance must beLike { case Some(Vector(StaticProvenance("/foo"))) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("//baz where //foo.bar <= 2")
          tree.accumulatedProvenance must beLike { case Some(Vector(StaticProvenance("/baz"), StaticProvenance("/foo"))) => ok }
          tree.errors mustEqual Set(OperationOnUnrelatedSets)
        }
        {
          val tree = compile("//foo.baz where //foo.bar >= 2")
          tree.accumulatedProvenance must beLike { case Some(Vector(StaticProvenance("/foo"))) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("//foo where null")
          tree.accumulatedProvenance must beLike { case Some(Vector(StaticProvenance("/foo"))) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("bar(2) where false")
          tree.accumulatedProvenance must beLike { case None => ok }
          tree.errors mustEqual Set(UndefinedFunction(Identifier(Vector(), "bar")))
        }
        {
          val tree = compile("//foo where true")
          tree.accumulatedProvenance must beLike { case Some(Vector(StaticProvenance("/foo"))) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("{} where 5 = 2")
          tree.accumulatedProvenance must beLike { case Some(Vector()) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("true where []")
          tree.accumulatedProvenance must beLike { case Some(Vector()) => ok }
          tree.errors must beEmpty
        }
      }

      "With" >> {
        {
          val tree = compile("(1 + 2) with {}")
          tree.accumulatedProvenance must beLike { case Some(Vector()) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("1 with baz")
          tree.accumulatedProvenance must beLike { case None => ok }
          tree.errors mustEqual Set(UndefinedFunction(Identifier(Vector(), "baz")))
        }
        {
          val tree = compile("1 with 2")
          tree.accumulatedProvenance must beLike { case Some(Vector()) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("//foo with {a: 2}")
          tree.accumulatedProvenance must beLike { case Some(Vector(StaticProvenance("/foo"))) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("//foo with {a: //foo.bar, b: 5}")
          tree.accumulatedProvenance must beLike { case Some(Vector(StaticProvenance("/foo"))) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("10 with {}")
          tree.accumulatedProvenance must beLike { case Some(Vector()) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("[] with {a: 4}")
          tree.accumulatedProvenance must beLike { case Some(Vector()) => ok }
          tree.errors must beEmpty
        }        
        {
          val tree = compile("{a: 4} with //foobar")
          tree.accumulatedProvenance must beLike { case Some(Vector(StaticProvenance("/foobar"))) => ok }
          tree.errors must beEmpty
        }
      }

      "Intersect" >> {
        {
          val tree = compile("3 intersect 4")
          tree.accumulatedProvenance must beLike { case Some(Vector()) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("//foo intersect //bar")
          tree.accumulatedProvenance must beLike { case Some(Vector(DynamicProvenance(_))) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("//foo intersect 5")
          tree.accumulatedProvenance must beLike { case None => ok }
          tree.errors mustEqual Set(IntersectProvenanceDifferentLength)
        }
        {
          val tree = compile("//foo intersect bar(2)")
          tree.accumulatedProvenance must beLike { case None => ok }
          tree.errors mustEqual Set(UndefinedFunction(Identifier(Vector(), "bar")))
        }
        {
          val tree = compile("(//foo.bar + 5) intersect 9")
          tree.accumulatedProvenance must beLike { case None => ok }
          tree.errors mustEqual Set(IntersectProvenanceDifferentLength)
        }
      }
      "Union" >> {
        {
          val tree = compile("3 union 4")
          tree.accumulatedProvenance must beLike { case Some(Vector()) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("//foo union //bar")
          tree.accumulatedProvenance must beLike { case Some(Vector(DynamicProvenance(_))) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("//foo union 5")
          tree.accumulatedProvenance must beLike { case None => ok }
          tree.errors mustEqual Set(UnionProvenanceDifferentLength)
        }
        {
          val tree = compile("//foo union bar(2)")
          tree.accumulatedProvenance must beLike { case None => ok }
          tree.errors mustEqual Set(UndefinedFunction(Identifier(Vector(), "bar")))
        }
        {
          val tree = compile("(//foo.bar + 5) union 9")
          tree.accumulatedProvenance must beLike { case None => ok }
          tree.errors mustEqual Set(UnionProvenanceDifferentLength)
        }
      }

      "Difference" >> {
        {
          val tree = compile("3 difference 4")
          tree.accumulatedProvenance must beLike { case None => ok }
          tree.errors mustEqual Set(DifferenceProvenanceValue)
        }
        {
          val tree = compile("//foo difference //bar")
          tree.accumulatedProvenance must beLike { case Some(Vector(StaticProvenance("/foo"))) => ok }
          tree.errors must beEmpty
        }        
        {
          val tree = compile("//foo difference //foo.a")
          tree.accumulatedProvenance must beLike { case Some(Vector(StaticProvenance("/foo"))) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("//foo difference 5")
          tree.accumulatedProvenance must beLike { case None => ok }
          tree.errors mustEqual Set(DifferenceProvenanceDifferentLength)
        }
        {
          val tree = compile("//foo difference bar(2)")
          tree.accumulatedProvenance must beLike { case None => ok }
          tree.errors mustEqual Set(UndefinedFunction(Identifier(Vector(), "bar")))
        }
        {
          val tree = compile("(//foo.bar + 5) difference 9")
          tree.accumulatedProvenance must beLike { case None => ok }
          tree.errors mustEqual Set(DifferenceProvenanceDifferentLength)
        }
      }

      "Add" >> {
        {
          val tree = compile("3 + 4")
          tree.accumulatedProvenance must beLike { case Some(Vector()) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("//clicks.foo + //clicks.bar")
          tree.accumulatedProvenance must beLike { case Some(Vector(StaticProvenance(_))) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("//clicks.foo + 4")
          tree.accumulatedProvenance must beLike { case Some(Vector(StaticProvenance(_))) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("//clicks.foo + //views.foo")
          tree.accumulatedProvenance must beLike { case Some(Vector(StaticProvenance(_), StaticProvenance(_))) => ok }
          tree.errors mustEqual Set(OperationOnUnrelatedSets)
        }
      }

      "Lt" >> {
        {
          val tree = compile("3 < 4")
          tree.accumulatedProvenance must beLike { case Some(Vector()) => ok }
          tree.errors must beEmpty
        }
      }

      "And" >> {
        {
          val tree = compile("3 & 4")
          tree.accumulatedProvenance must beLike { case Some(Vector()) => ok }
          tree.errors must beEmpty
        }
      }

      "Comp" >> {
        {
          val tree = compile("!true")
          tree.accumulatedProvenance must beLike { case Some(Vector()) => ok }
          tree.errors must beEmpty
        }
      }

      "Neg" >> {
        {
          val tree = compile("neg 3")
          tree.accumulatedProvenance must beLike { case Some(Vector()) => ok }
          tree.errors must beEmpty
        }
      }

      "Paren" >> {
        {
          val tree = compile("(3)")
          tree.accumulatedProvenance must beLike { case Some(Vector()) => ok }
          tree.errors must beEmpty
        }
      }

    }
        
    "identify union according to its children" >> {
      "Let" >> {
        {
          val tree = compile("foo := //clicks foo union 2")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors mustEqual Set(UnionProvenanceDifferentLength)
        }       
        {
          val tree = compile("foo := //clicks foo union //views")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }      
      }

      "New" >> {
        {
          val tree = compile("1 union new 2")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors mustEqual Set(UnionProvenanceDifferentLength)
        }
        {
          val tree = compile("(new 2) union //clicks")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }
      }

      "Relate" >> {
        {
          val tree = compile("//clicks ~ //views foo := //clicks + //views foo union 4")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors mustEqual Set(UnionProvenanceDifferentLength)
        }
        {
          val tree = compile("//clicks ~ //views //foo union 4")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors mustEqual Set(UnionProvenanceDifferentLength)
        }
      }

      "Literals" >> {
        {
          val tree = compile("""(1 union "foo") union (true union null) """)
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }
      }

      "ObjectDef" >> {
        {
          val tree = compile("{foo: //foobar.a, bar: //foobar.b} union //baz")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }      
        {
          val tree = compile("{foo: 5} union 6")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }        
        {
          val tree = compile("false union {foo: foo(3)}")
          tree.provenance must beLike { case NullProvenance => ok }
          tree.errors mustEqual Set(UndefinedFunction(Identifier(Vector(), "foo")))
        }        
      }

      "ArrayDef" >> {
        {
          val tree = compile("[4,5,6] union 7")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }        
        {
          val tree = compile("false union [foo(5), {bar: 10}]")
          tree.provenance must beLike { case NullProvenance => ok }
          tree.errors mustEqual Set(UndefinedFunction(Identifier(Vector(), "foo")))
        }        
      }

      "Descent" >> {
        {
          val tree = compile("//foo.a union 6")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors mustEqual Set(UnionProvenanceDifferentLength)
        }  
        {
          val tree = compile("6 union {foo: 5}.foo")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }
      }

      "Deref" >> {
        {
          val tree = compile("//clicks[1] union 6")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors mustEqual Set(UnionProvenanceDifferentLength)
        }
        {
          val tree = compile("foo := [3,4,5] foo[1] union 6")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }
      }

      "Dispatch" >> {
        {
          val tree = compile("//foo union //bar")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }      
        {
          val tree = compile("""foo::bar("baz") union 6""")
          tree.provenance must beLike { case NullProvenance => ok }
          tree.errors mustEqual Set(UndefinedFunction(Identifier(Vector("foo"), "bar")))
        }      
        {
          val tree = compile("//foo union 2")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors mustEqual Set(UnionProvenanceDifferentLength)
        }
        
        {
          val tree = compile("1 union //foo")        
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors mustEqual Set(UnionProvenanceDifferentLength)
        }
        {
          val tree = compile("distinct(//clicks.bar) union //bar")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("sum(//clicks.bar) union false")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("sum(//clicks.bar, 100) union false")
          tree.provenance must beLike { case NullProvenance => ok }
          tree.errors mustEqual Set(IncorrectArity(1, 2))
        }
        {
          forall(lib1) { f =>
            val tree = compile("%s(10) union {a: 33}".format(f.fqn))
            tree.provenance must beLike { case DynamicProvenance(_) => ok }
            tree.errors must beEmpty
          }
        }
        {
          forall(lib2) { f =>
            val tree = compile("%s(//bar.foo, //bar.ack) union //bar".format(f.fqn))
            tree.provenance must beLike { case DynamicProvenance(_) => ok }
            tree.errors must beEmpty
          }
        }
        {
          val tree = compile("f := true union false f")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("f('a) := (//foobar.a union //barfoo.a) where //foobar.a = 'a f(10)")
          tree.provenance must beLike { case NullProvenance => ok }
          tree.errors mustEqual Set(OperationOnUnrelatedSets)
        }
        {
          val tree = compile("f('a) := //foobar where //foobar.a = 'a f(10) union 12")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors mustEqual Set(UnionProvenanceDifferentLength)
        }
        {
          val tree = compile("f('a) := //foobar where //foobar.a = 'a f union //baz")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile(""" 
            clicks := //clicks
            views := //ciews
            clicks ~ views
            sum := clicks.time + views.time
            sum union //campaigns
            """)
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors mustEqual Set(UnionProvenanceDifferentLength)
        }
      }

      "Where" >> {
        {
          val tree = compile("(//foo where //foo.a = 10) union //baz")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("""
            //foo ~ //bar ~ //baz 
            ({a: //baz - //foo} where true) union //foo + //bar""")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("(//ack where //achoo.foo >= 3) union 12") 
          tree.provenance must beLike { case NullProvenance => ok }
          tree.errors mustEqual Set(OperationOnUnrelatedSets, UnionProvenanceDifferentLength)
        }
      }

      "With" >> {
        {
          val tree = compile("(//foo with {a: 1}) union //baz")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("(null with {}) union //baz")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors mustEqual Set(UnionProvenanceDifferentLength)
        }
      }

      "Union/Intersect" >> {
        {
          val tree = compile("(//foo union {a: 1}) union //baz")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors mustEqual Set(UnionProvenanceDifferentLength)
        }
        {
          val tree = compile("(null intersect {}) union 10")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("(//foo.a + //foo.b union //baz) union 12")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors mustEqual Set(UnionProvenanceDifferentLength)
        }
      }

      "Add/Sub/Mul/Div" >> {
        {
          val tree = compile("1 - 2 union 3 + 4")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("1 * //foo union //bazbarfoobam / 8")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }
      }

      "Lt/LtEq/Gt/GtEq/Eq/NotEq" >> {
        {
          val tree = compile("""(1 < 2) union ("there's a knot in this string") != "NOPE!" """)
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("(//foo.a <= 3) union (//iamasquirrel = 3)")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("(4 > 999999) union (//didsomeonesayoink.moooo >= 122)")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors mustEqual Set(UnionProvenanceDifferentLength)
        }
      }

      "And/Or" >> {
        {
          val tree = compile("""(1 & true) union (4 | null) """)
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }
      }

      "Comp" >> {
        {
          val tree = compile("4 union !true")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }      
      }

      "Neg" >> {
        {
          val tree = compile("neg 3 union 4")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }      
      }
      
      "Paren" >> {
        {
          val tree = compile("(//foo) union //bar")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("({}) union ([])")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }
      }
    }  

    "identify intersect according to its children" >> {
      "Let" >> {
        {
          val tree = compile("foo := //clicks foo intersect 2")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors mustEqual Set(IntersectProvenanceDifferentLength)
        }       
        {
          val tree = compile("foo := //clicks foo intersect //views")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }      
      }

      "New" >> {
        {
          val tree = compile("1 intersect new 2")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors mustEqual Set(IntersectProvenanceDifferentLength)
        }
        {
          val tree = compile("(new 2) intersect //clicks")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }
      }

      "Relate" >> {
        {
          val tree = compile("//clicks ~ //views foo := //clicks + //views foo intersect 4")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors mustEqual Set(IntersectProvenanceDifferentLength)
        }
        {
          val tree = compile("//clicks ~ //views //foo intersect 4")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors mustEqual Set(IntersectProvenanceDifferentLength)
        }
      }

      "Literals" >> {
        {
          val tree = compile("""(1 intersect "foo") intersect (true intersect null) """)
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }
      }

      "ObjectDef" >> {
        {
          val tree = compile("{foo: //foobar.a, bar: //foobar.b} intersect //baz")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }      
        {
          val tree = compile("{foo: 5} intersect 6")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }        
        {
          val tree = compile("false intersect {foo: foo(3)}")
          tree.provenance must beLike { case NullProvenance => ok }
          tree.errors mustEqual Set(UndefinedFunction(Identifier(Vector(), "foo")))
        }        
      }

      "ArrayDef" >> {
        {
          val tree = compile("[4,5,6] intersect 7")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }        
        {
          val tree = compile("false intersect [foo(5), {bar: 10}]")
          tree.provenance must beLike { case NullProvenance => ok }
          tree.errors mustEqual Set(UndefinedFunction(Identifier(Vector(), "foo")))
        }        
      }

      "Descent" >> {
        {
          val tree = compile("//foo.a intersect 6")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors mustEqual Set(IntersectProvenanceDifferentLength)
        }  
        {
          val tree = compile("6 intersect {foo: 5}.foo")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }
      }

      "Deref" >> {
        {
          val tree = compile("//clicks[1] intersect 6")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors mustEqual Set(IntersectProvenanceDifferentLength)
        }
        {
          val tree = compile("foo := [3,4,5] foo[1] intersect 6")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }
      }

      "Dispatch" >> {
        {
          val tree = compile("//foo intersect //bar")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }      
        {
          val tree = compile("""foo::bar("baz") intersect 6""")
          tree.provenance must beLike { case NullProvenance => ok }
          tree.errors mustEqual Set(UndefinedFunction(Identifier(Vector("foo"), "bar")))
        }      
        {
          val tree = compile("//foo intersect 2")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors mustEqual Set(IntersectProvenanceDifferentLength)
        }
        
        {
          val tree = compile("1 intersect //foo")        
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors mustEqual Set(IntersectProvenanceDifferentLength)
        }
        {
          val tree = compile("distinct(//clicks.bar) intersect //bar")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("sum(//clicks.bar) intersect false")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("sum(//clicks.bar, 100) intersect false")
          tree.provenance must beLike { case NullProvenance => ok }
          tree.errors mustEqual Set(IncorrectArity(1, 2))
        }
        {
          forall(lib1) { f =>
            val tree = compile("%s(10) intersect {a: 33}".format(f.fqn))
            tree.provenance must beLike { case DynamicProvenance(_) => ok }
            tree.errors must beEmpty
          }
        }
        {
          forall(lib2) { f =>
            val tree = compile("%s(//bar.foo, //bar.ack) intersect //bar".format(f.fqn))
            tree.provenance must beLike { case DynamicProvenance(_) => ok }
            tree.errors must beEmpty
          }
        }
        {
          val tree = compile("f := true intersect false f")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("f('a) := (//foobar.a intersect //barfoo.a) where //foobar.a = 'a f(10)")
          tree.provenance must beLike { case NullProvenance => ok }
          tree.errors mustEqual Set(OperationOnUnrelatedSets)
        }
        {
          val tree = compile("f('a) := //foobar where //foobar.a = 'a f(10) intersect 12")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors mustEqual Set(IntersectProvenanceDifferentLength)
        }
        {
          val tree = compile("f('a) := //foobar where //foobar.a = 'a f intersect //baz")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile(""" 
            clicks := //clicks
            views := //ciews
            clicks ~ views
            sum := clicks.time + views.time
            sum intersect //campaigns
            """)
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors mustEqual Set(IntersectProvenanceDifferentLength)
        }
      }

      "Where" >> {
        {
          val tree = compile("(//foo where //foo.a = 10) intersect //baz")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("""
            //foo ~ //bar ~ //baz 
            ({a: //baz - //foo} where true) intersect //foo + //bar""")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("(//ack where //achoo.foo >= 3) intersect 12") 
          tree.provenance must beLike { case NullProvenance => ok }
          tree.errors mustEqual Set(OperationOnUnrelatedSets, IntersectProvenanceDifferentLength)
        }
      }

      "With" >> {
        {
          val tree = compile("(//foo with {a: 1}) intersect //baz")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("(null with {}) intersect //baz")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors mustEqual Set(IntersectProvenanceDifferentLength)
        }
      }

      "intersect" >> {
        {
          val tree = compile("(//foo intersect {a: 1}) intersect //baz")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors mustEqual Set(IntersectProvenanceDifferentLength)
        }
        {
          val tree = compile("(null intersect {}) intersect 10")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("(//foo.a + //foo.b intersect //baz) intersect 12")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors mustEqual Set(IntersectProvenanceDifferentLength)
        }
      }

      "Add/Sub/Mul/Div" >> {
        {
          val tree = compile("1 - 2 intersect 3 + 4")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("1 * //foo intersect //bazbarfoobam / 8")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }
      }

      "Lt/LtEq/Gt/GtEq/Eq/NotEq" >> {
        {
          val tree = compile("""(1 < 2) intersect ("there's a knot in this string") != "NOPE!" """)
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("(//foo.a <= 3) intersect (//iamasquirrel = 3)")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("(4 > 999999) intersect (//didsomeonesayoink.moooo >= 122)")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors mustEqual Set(IntersectProvenanceDifferentLength)
        }
      }

      "And/Or" >> {
        {
          val tree = compile("""(1 & true) intersect (4 | null) """)
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }
      }

      "Comp" >> {
        {
          val tree = compile("4 intersect !true")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }      
      }

      "Neg" >> {
        {
          val tree = compile("neg 3 intersect 4")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }      
      }
      
      "Paren" >> {
        {
          val tree = compile("(//foo) intersect //bar")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("({}) intersect ([])")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }
      }
    }  

    "identify set difference according to its children" >> {
      "Let" >> {
        {
          val tree = compile("foo := //clicks foo difference 2")
          tree.provenance must beLike { case StaticProvenance("/clicks") => ok }
          tree.errors mustEqual Set(DifferenceProvenanceDifferentLength)
        }       
        {
          val tree = compile("foo := //clicks foo difference //views")
          tree.provenance must beLike { case StaticProvenance("/clicks") => ok }
          tree.errors must beEmpty
        }      
      }

      "New" >> {
        {
          val tree = compile("1 difference new 2")
          tree.provenance must beLike { case ValueProvenance => ok }
          tree.errors mustEqual Set(DifferenceProvenanceDifferentLength)
        }
        {
          val tree = compile("(new 2) difference //clicks")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }
      }

      "Relate" >> {
        {
          val tree = compile("//clicks ~ //views foo := //clicks + //views foo difference 4")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors mustEqual Set(DifferenceProvenanceDifferentLength)
        }
        {
          val tree = compile("//clicks ~ //views //foo difference 4")
          tree.provenance must beLike { case StaticProvenance("/foo") => ok }
          tree.errors mustEqual Set(DifferenceProvenanceDifferentLength)
        }
      }

      "Literals" >> {
        {
          val tree = compile("""(//foo difference //bar) difference (//bax difference //bao) """)
          tree.provenance must beLike { case StaticProvenance("/foo") => ok }
          tree.errors must beEmpty
        }
      }

      "ObjectDef" >> {
        {
          val tree = compile("{foo: //foobar.a, bar: //foobar.b} difference //baz")
          tree.provenance must beLike { case StaticProvenance("/foobar") => ok }
          tree.errors must beEmpty
        }      
        {
          val tree = compile("{foo: 5} difference 6")
          tree.provenance must beLike { case ValueProvenance => ok }
          tree.errors mustEqual Set(DifferenceProvenanceValue)
        }        
        {
          val tree = compile("//foo difference {x: bar(3)}")
          tree.provenance must beLike { case NullProvenance => ok }
          tree.errors mustEqual Set(UndefinedFunction(Identifier(Vector(), "bar")))
        }        
      }

      "ArrayDef" >> {
        {
          val tree = compile("[4,5,6] difference 7")
          tree.provenance must beLike { case ValueProvenance => ok }
          tree.errors mustEqual Set(DifferenceProvenanceValue)
        }        
        {
          val tree = compile("false difference [foo(5), {bar: 10}]")
          tree.provenance must beLike { case NullProvenance => ok }
          tree.errors mustEqual Set(UndefinedFunction(Identifier(Vector(), "foo")))
        }        
      }

      "Descent" >> {
        {
          val tree = compile("//foo.a difference 6")
          tree.provenance must beLike { case StaticProvenance("/foo") => ok }
          tree.errors mustEqual Set(DifferenceProvenanceDifferentLength)
        }  
        {
          val tree = compile("//foo.a difference //bar.b")
          tree.provenance must beLike { case StaticProvenance("/foo") => ok }
          tree.errors must beEmpty
        }
      }

      "Deref" >> {
        {
          val tree = compile("//clicks[1] difference 6")
          tree.provenance must beLike { case StaticProvenance("/clicks") => ok }
          tree.errors mustEqual Set(DifferenceProvenanceDifferentLength)
        }
        {
          val tree = compile("foo := [//bar] (foo[0] difference //bax)")
          tree.provenance must beLike { case StaticProvenance("/bar") => ok }
          tree.errors must beEmpty
        }
      }

      "Dispatch" >> {
        {
          val tree = compile("//foo difference //bar")
          tree.provenance must beLike { case StaticProvenance("/foo") => ok }
          tree.errors must beEmpty
        }      
        {
          val tree = compile("""foo::bar("baz") difference 6""")
          tree.provenance must beLike { case NullProvenance => ok }
          tree.errors mustEqual Set(UndefinedFunction(Identifier(Vector("foo"), "bar")))
        }      
        {
          val tree = compile("//foo difference 2")
          tree.provenance must beLike { case StaticProvenance("/foo") => ok }
          tree.errors mustEqual Set(DifferenceProvenanceDifferentLength)
        }
        
        {
          val tree = compile("1 difference //foo")        
          tree.provenance must beLike { case ValueProvenance => ok }
          tree.errors mustEqual Set(DifferenceProvenanceDifferentLength)
        }
        {
          val tree = compile("distinct(//clicks.bar) difference //bar")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("sum(//clicks.bar) difference false")
          tree.provenance must beLike { case ValueProvenance => ok }
          tree.errors mustEqual Set(DifferenceProvenanceValue)
        }
        {
          val tree = compile("sum(//clicks.bar, 100) difference false")
          tree.provenance must beLike { case NullProvenance => ok }
          tree.errors mustEqual Set(IncorrectArity(1, 2))
        }
        {
          forall(lib1) { f =>
            val tree = compile("%s(10) difference {a: //foo}".format(f.fqn))
            tree.provenance must beLike { case ValueProvenance => ok }
            tree.errors mustEqual Set(DifferenceProvenanceDifferentLength)
          }
        }
        {
          forall(lib2) { f =>
            val tree = compile("%s(//bar.foo, //bar.ack) difference //baz".format(f.fqn))
            tree.provenance must beLike { case StaticProvenance("/bar") => ok }
            tree.errors must beEmpty
          }
        }
        {
          val tree = compile("f := //foo difference //bar f")
          tree.provenance must beLike { case StaticProvenance("/foo") => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("f('a) := (//foobar.a difference //barfoo.a) where //foobar.a = 'a f(10)")
          tree.provenance must beLike { case StaticProvenance("/foobar") => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("f('a) := //foobar where //foobar.a = 'a f(10) difference 12")
          tree.provenance must beLike { case StaticProvenance("/foobar") => ok }
          tree.errors mustEqual Set(DifferenceProvenanceDifferentLength)
        }
        {
          val tree = compile("f('a) := //foobar where //foobar.a = 'a f difference //baz")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile(""" 
            clicks := //clicks
            views := //ciews
            clicks ~ views
            sum := clicks.time + views.time
            sum difference //campaigns
            """)
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors mustEqual Set(DifferenceProvenanceDifferentLength)
        }
      }

      "Where" >> {
        {
          val tree = compile("(//foo where //foo.a = 10) difference //baz")
          tree.provenance must beLike { case StaticProvenance("/foo") => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("""
            //foo ~ //bar ~ //baz 
            ({a: //baz - //foo} where true) difference //foo + //bar""")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("(//ack where //achoo.foo >= 3) difference 12") 
          tree.provenance must beLike { case NullProvenance => ok }
          tree.errors mustEqual Set(OperationOnUnrelatedSets, DifferenceProvenanceDifferentLength)
        }
      }

      "With" >> {
        {
          val tree = compile("(//foo with {a: 1}) difference //baz")
          tree.provenance must beLike { case StaticProvenance("/foo") => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("(null with {}) difference //baz")
          tree.provenance must beLike { case ValueProvenance => ok }
          tree.errors mustEqual Set(DifferenceProvenanceDifferentLength)
        }
      }

      "intersect" >> {
        {
          val tree = compile("(//foo intersect //bar) difference {a: 5}")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors mustEqual Set(DifferenceProvenanceDifferentLength)
        }
        {
          val tree = compile("(//a intersect //b) difference //c")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("(//foo.a + //foo.b intersect //baz) difference //baz.c")
          tree.provenance must beLike { case DynamicProvenance(_) => ok }
          tree.errors must beEmpty
        }
      }

      "Add/Sub/Mul/Div" >> {
        {
          val tree = compile("1 - 2 difference 3 + 4")
          tree.provenance must beLike { case ValueProvenance => ok }
          tree.errors mustEqual Set(DifferenceProvenanceValue)
        }
        {
          val tree = compile("1 * //foo difference //bazbarfoobam / 8")
          tree.provenance must beLike { case StaticProvenance("/foo") => ok }
          tree.errors must beEmpty
        }
      }

      "Lt/LtEq/Gt/GtEq/Eq/NotEq" >> {
        {
          val tree = compile("""(1 < 2) difference ("there's a knot in this string") != "NOPE!" """)
          tree.provenance must beLike { case ValueProvenance => ok }
          tree.errors mustEqual Set(DifferenceProvenanceValue)
        }
        {
          val tree = compile("(//foo.a <= 3) difference (//iamasquirrel = 3)")
          tree.provenance must beLike { case StaticProvenance("/foo") => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("(4 > 999999) difference (//didsomeonesayoink.moooo >= 122)")
          tree.provenance must beLike { case ValueProvenance => ok }
          tree.errors mustEqual Set(DifferenceProvenanceDifferentLength)
        }
      }

      "And/Or" >> {
        {
          val tree = compile("""(//foo & //bar) difference (//x | //y) """)
          tree.provenance must beLike { case NullProvenance => ok }
          tree.errors mustEqual Set(OperationOnUnrelatedSets)
        }
      }

      "Comp" >> {
        {
          val tree = compile("//foo.a difference !//fob.b")
          tree.provenance must beLike { case StaticProvenance("/foo") => ok }
          tree.errors must beEmpty
        }      
      }

      "Neg" >> {
        {
          val tree = compile("neg //foo difference //bar")
          tree.provenance must beLike { case StaticProvenance("/foo") => ok }
          tree.errors must beEmpty
        }      
      }
      
      "Paren" >> {
        {
          val tree = compile("(//foo) difference //bar")
          tree.provenance must beLike { case StaticProvenance("/foo") => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("({}) difference ([])")
          tree.provenance must beLike { case ValueProvenance => ok }
          tree.errors mustEqual Set(DifferenceProvenanceValue)
        }
      }
    }  

    "accept user-defined union, intersect, and difference" in {
      {
        val tree = compile("foo := //baz union //bar foo")
        tree.provenance must beLike { case DynamicProvenance(_) => ok }
        tree.errors must beEmpty
      }
      {
        val tree = compile("foo := //baz intersect //bar foo")
        tree.provenance must beLike { case DynamicProvenance(_) => ok }
        tree.errors must beEmpty
      }
      {
        val tree = compile("foo := //baz difference //bar foo")
        tree.provenance must beLike { case StaticProvenance("/baz") => ok }
        tree.errors must beEmpty
      }
    }

    "accept user-defined function within a user-defined function" in {
      {
        val tree = compile("""
          foo('a) := 
            bar('b) :=
              //clicks where //clicks.a = 'b
            bar('a)
          foo(2)""")
        tree.provenance must beLike { case StaticProvenance("/clicks") => ok }
        tree.errors must beEmpty
      }      
      {
        val tree = compile("""
          foo('a) := 
            bar('b) :=
              //clicks where //clicks.a = 'b + 'a
            bar
          foo(2)""")
        tree.provenance must beLike { case DynamicProvenance(_) => ok }
        tree.errors must beEmpty
      }
    }

    "check provenance of partially-quantified function" in {
      val tree = compile("""
        foo('a) :=
          //clicks where //clicks.a = 'a
        foo""")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty
    }

    "identify intersect according to its children" in {
      {
        val tree = compile("1 intersect 2")
        tree.provenance must beLike { case DynamicProvenance(_) => ok }
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("//foo intersect 2")
        tree.provenance must beLike { case DynamicProvenance(_) => ok }
        tree.errors must contain(IntersectProvenanceDifferentLength)
      }
      
      {
        val tree = compile("1 intersect //foo")        
        tree.provenance must beLike { case DynamicProvenance(_) => ok }
        tree.errors must contain(IntersectProvenanceDifferentLength)

      }
      
      {
        val tree = compile("new (1 intersect 2)")
        tree.provenance must beLike { case DynamicProvenance(_) => ok }
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 intersect new 2")
        tree.provenance must beLike { case DynamicProvenance(_) => ok }
        tree.errors must contain(IntersectProvenanceDifferentLength)
      }
      
      {
        val tree = compile("//foo intersect //bar")
        tree.provenance must beLike { case DynamicProvenance(_) => ok }
        tree.errors must beEmpty
      }
    }  

    
    "identify addition according to its children" in {
      {
        val tree = compile("1 + 2")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("//foo + 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 + //foo")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("new 1 + 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 + new 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify subtraction according to its children" in {
      {
        val tree = compile("1 - 2")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("//foo - 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 - //foo")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("new 1 - 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 - new 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify multiplication according to its children" in {
      {
        val tree = compile("1 * 2")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("//foo * 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 * //foo")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("new 1 * 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 * new 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify division according to its children" in {
      {
        val tree = compile("1 / 2")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("//foo / 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 / //foo")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("new 1 / 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 / new 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify less-than according to its children" in {
      {
        val tree = compile("1 < 2")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("//foo < 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 < //foo")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("new 1 < 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 < new 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify less-than-equal according to its children" in {
      {
        val tree = compile("1 <= 2")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("//foo <= 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 <= //foo")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("new 1 <= 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 <= new 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify greater-than according to its children" in {
      {
        val tree = compile("1 > 2")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("//foo > 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 > //foo")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("new 1 > 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 > new 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify greater-than-equal according to its children" in {
      {
        val tree = compile("1 >= 2")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("//foo >= 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 >= //foo")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("new 1 >= 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 >= new 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify equal according to its children" in {
      {
        val tree = compile("1 = 2")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("//foo = 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 = //foo")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("new 1 = 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 = new 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify not-equal according to its children" in {
      {
        val tree = compile("1 != 2")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("//foo != 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 != //foo")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("new 1 != 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 != new 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify boolean and according to its children" in {
      {
        val tree = compile("1 & 2")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("//foo & 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 & //foo")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("new 1 & 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 & new 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify boolean or according to its children" in {
      {
        val tree = compile("1 | 2")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("//foo | 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 | //foo")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("new 1 | 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 | new 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify complement according to its child" in {
      {
        val tree = compile("!1")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("!//foo")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("!(new 1)")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify negation according to its child" in {
      {
        val tree = compile("neg 1")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("neg //foo")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("neg (new 1)")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify parenthetical according to its child" in {
      {
        val tree = compile("(1)")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("(//foo)")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("(new 1)")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
  }
  
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
  
  "explicit relation" should {
    "fail on natively-related sets" in {
      {
        val tree = compile("//a ~ //a 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(AlreadyRelatedSets)
      }
      
      {
        val tree = compile("1 ~ 2 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(AlreadyRelatedSets)
      }
      
      {
        val tree = compile("a := new 1 a ~ a 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(AlreadyRelatedSets)
      }
    }
    
    "fail on explicitly related sets" in {
      val tree = compile("a := //a b := //b a ~ b a ~ b 42")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(AlreadyRelatedSets)
    }
    
    "accept object definition on different loads when related" in {
      val tree = compile("//foo ~ //bar { a: //foo, b: //bar }")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept object definition on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 //foo ~ s { a: //foo, b: s }")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept object definition on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 ~ s2 { a: s1, b: s2 }")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept array definition on different loads when related" in {
      val tree = compile("//foo ~ //bar [ //foo, //bar ]")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept array definition on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 //foo ~ s [ //foo, s ]")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept array definition on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 ~ s2 [ s1, s2 ]")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept deref on different loads when related" in {
      val tree = compile("//foo ~ //bar //foo[//bar]")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept deref on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 //foo ~ s //foo[s]")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept deref on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 ~ s2 s1[s2]")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept dispatch on different loads when related" in {
      val tree = compile("//foo ~ //bar fun('a, 'b) := 'a + 'b fun(//foo, //bar)")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty
    }
    
    "accept dispatch on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 //foo ~ s fun('a, 'b) := 'a + 'b fun(//foo, s)")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty
    }
    
    "accept dispatch on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 ~ s2 fun('a, 'b) := 'a + 'b fun(s1, s2)")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty
    }
    
    "accept where on different loads when related" in {
      val tree = compile("//foo ~ //bar //foo where //bar")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept where on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 //foo ~ s //foo where s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept where on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 ~ s2 s1 where s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }    
    "accept with on different loads when related" in {
      val tree = compile("//foo ~ //bar //foo with //bar")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept with on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 //foo ~ s //foo with s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept with on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 ~ s2 s1 with s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }    
    "accept union on different loads when related" in {
      val tree = compile("//foo ~ //bar //foo union //bar")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept union on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 //foo ~ s //foo union s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept union on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 ~ s2 s1 union s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }    
    "accept intersect on different loads when related" in {
      val tree = compile("//foo ~ //bar //foo intersect //bar")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept intersect on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 //foo ~ s //foo intersect s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept intersect on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 ~ s2 s1 intersect s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }    
    
    "accept difference on different loads when related" in {
      val tree = compile("//foo ~ //bar //foo difference //bar")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept difference on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 //foo ~ s //foo difference s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept difference on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 ~ s2 s1 difference s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept addition on different loads when related" in {
      val tree = compile("//foo ~ //bar //foo + //bar")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept addition on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 //foo ~ s //foo + s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept addition on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 ~ s2 s1 + s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept subtraction on different loads when related" in {
      val tree = compile("//foo ~ //bar //foo - //bar")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept subtraction on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 //foo ~ s //foo - s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept subtraction on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 ~ s2 s1 - s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept multiplication on different loads when related" in {
      val tree = compile("//foo ~ //bar //foo * //bar")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept multiplication on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 //foo ~ s //foo * s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept multiplication on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 ~ s2 s1 * s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept division on different loads when related" in {
      val tree = compile("//foo ~ //bar //foo / //bar")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept division on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 //foo ~ s //foo / s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept division on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 ~ s2 s1 / s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept less-than on different loads when related" in {
      val tree = compile("//foo ~ //bar //foo < //bar")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept less-than on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 //foo ~ s //foo < s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept less-than on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 ~ s2 s1 < s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept less-than-equal on different loads when related" in {
      val tree = compile("//foo ~ //bar //foo <= //bar")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept less-than-equal on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 //foo ~ s //foo <= s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept less-than-equal on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 ~ s2 s1 <= s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept greater-than on different loads when related" in {
      val tree = compile("//foo ~ //bar //foo > //bar")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept greater-than on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 //foo ~ s //foo > s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept greater-than on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 ~ s2 s1 > s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept greater-than-equal on different loads when related" in {
      val tree = compile("//foo ~ //bar //foo >= //bar")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept greater-than-equal on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 //foo ~ s //foo >= s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept greater-than-equal on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 ~ s2 s1 >= s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept equality on different loads when related" in {
      val tree = compile("//foo ~ //bar //foo = //bar")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept equality on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 //foo ~ s //foo = s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept equality on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 ~ s2 s1 = s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept not-equality on different loads when related" in {
      val tree = compile("//foo ~ //bar //foo != //bar")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept not-equality on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 //foo ~ s //foo != s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept not-equality on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 ~ s2 s1 != s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept boolean and on different loads when related" in {
      val tree = compile("//foo ~ //bar //foo & //bar")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept boolean and on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 //foo ~ s //foo & s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept boolean and on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 ~ s2 s1 & s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept boolean or on different loads when related" in {
      val tree = compile("//foo ~ //bar //foo | //bar")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept boolean or on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 //foo ~ s //foo | s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept boolean or on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 ~ s2 s1 | s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "reject addition with unrelated relation" in {
      val tree = compile("//a ~ //b //c + //d")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "accept operations according to the commutative relation" in {
      {
        val input = """
          | foo := //foo
          | bar := //bar
          | 
          | foo ~ bar
          |   foo + bar""".stripMargin
          
        val tree = compile(input)
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val input = """
          | foo := //foo
          | bar := //bar
          | 
          | foo ~ bar
          |   bar + foo""".stripMargin
          
        val tree = compile(input)
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "accept operations according to the transitive relation" in {
      val input = """
        | foo := //foo
        | bar := //bar
        | baz := //baz
        | 
        | foo ~ bar
        |   bar ~ baz
        |     foo + baz""".stripMargin
        
      val tree = compile(input)
      tree.provenance must beLike {
        case DynamicProvenance(_) => ok
      }
      tree.errors must beEmpty
    }
    
    "accept operations according to the commutative-transitive relation" in {
      val input = """
        | foo := //foo
        | bar := //bar
        | baz := //baz
        | 
        | foo ~ bar
        |   bar ~ baz
        |     baz + foo""".stripMargin
        
      val tree = compile(input)
      tree.provenance must beLike {
        case DynamicProvenance(_) => ok
      }
      tree.errors must beEmpty
    }
    
    "accept multiple nested expressions in relation" in {
      {
        val input = """
        | foo := //foo
        | bar := //bar
        | 
        | foo ~ bar
        |   foo + bar + foo""".stripMargin
        
        val tree = compile(input)
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val input = """
        | foo := //foo
        | bar := //bar
        | 
        | foo ~ bar
        |   bar + foo + foo""".stripMargin
        
        val tree = compile(input)
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "attribute union provenance to constituents in trinary operation" in {
      val input = """
        | foo := //foo
        | bar := //bar
        | baz := //baz
        |
        | foo ~ bar
        |   bar ~ baz
        |     (foo.a - bar.a) * (bar.b / baz.b)
        """.stripMargin
        
      val tree @ Let(_, _, _, _, Let(_, _, _, _, Let(_, _, _, _, Relate(_, _, _, Relate(_, _, _, body @ Mul(_, left @ Sub(_, minLeft, minRight), right @ Div(_, divLeft, divRight))))))) =
        compile(input)
      
      tree.provenance must beLike {
        case DynamicProvenance(_) => ok
      }
      tree.errors must beEmpty
      
      body.provenance must beLike {
        case p: UnionProvenance => {
          p.possibilities must contain(StaticProvenance("/foo"))
          p.possibilities must contain(StaticProvenance("/bar"))
          p.possibilities must contain(StaticProvenance("/baz"))
        }
      }
      
      body.provenance.possibilities must contain(StaticProvenance("/foo"))
      body.provenance.possibilities must contain(StaticProvenance("/bar"))
      body.provenance.possibilities must contain(StaticProvenance("/baz"))
      
      left.provenance.possibilities must contain(StaticProvenance("/foo"))
      left.provenance.possibilities must contain(StaticProvenance("/bar"))
      left.provenance.possibilities must not(contain(StaticProvenance("/baz")))
      
      right.provenance.possibilities must not(contain(StaticProvenance("/foo")))
      right.provenance.possibilities must contain(StaticProvenance("/bar"))
      right.provenance.possibilities must contain(StaticProvenance("/baz"))
      
      minLeft.provenance mustEqual StaticProvenance("/foo")
      minRight.provenance mustEqual StaticProvenance("/bar")
      
      divLeft.provenance mustEqual StaticProvenance("/bar")
      divRight.provenance mustEqual StaticProvenance("/baz")
    }
  }
  
  "null provenance" should {
    "synthesize when name binding is null in dispatch" in {
      compile("fubar").provenance mustEqual NullProvenance
    }
    
    "propagate through let" in {
      {
        val tree = compile("a := //foo + //b a")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compile("a := //foo a + //bar")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "not propagate through new" in {
      val tree = compile("new (//a + //b)")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "propagate through relate" in {
      {
        val tree = compile("(//a + //b) ~ //c 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compile("//c ~ (//a + //b) 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through object definition" in {
      val tree = compile("{ a: //a + //b, b: 42 }")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "propagate through array definition" in {
      val tree = compile("[//a + //b, 42]")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "propagate through descent" in {
      val tree = compile("(//a + //b).foo")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "propagate through dereference" in {
      {
        val tree = compile("(//a + //b)[42]")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compile("42[//a + //b]")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through dispatch" in {
      val tree = compile("a('b) := 'b a(//a + //b)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "propagate through where" in {
      {
        val tree = compile("(//a + //b) where 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compile("42 + (//a where //b)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through with" in {
      {
        val tree = compile("(//a + //b) with 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compile("42 + (//a with //b)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through union" in {
      {
        val tree = compile("(//a + //b) union 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets, UnionProvenanceDifferentLength) 
      }
      
      {
        val tree = compile("42 union (//a + //b)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets, UnionProvenanceDifferentLength)
      }
    }
    
    "propagate through intersect" in {
      {
        val tree = compile("(//a + //b) intersect 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets, IntersectProvenanceDifferentLength)
      }
      
      {
        val tree = compile("42 intersect (//a + //b)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets, IntersectProvenanceDifferentLength)
      }
    }    

    "propagate through difference" in {
      {
        val tree = compile("(//a + //b) difference 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets, DifferenceProvenanceDifferentLength)
      }
      
      {
        val tree = compile("42 difference (//a + //b)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets, DifferenceProvenanceDifferentLength)
      }
    }
    
    "propagate through addition" in {
      {
        val tree = compile("(//a + //b) + 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compile("42 + (//a + //b)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through subtraction" in {
      {
        val tree = compile("(//a + //b) - 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compile("42 - (//a + //b)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through multiplication" in {
      {
        val tree = compile("(//a + //b) * 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compile("42 * (//a + //b)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through division" in {
      {
        val tree = compile("(//a + //b) / 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compile("42 / (//a + //b)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through less-than" in {
      {
        val tree = compile("(//a + //b) < 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compile("42 < (//a + //b)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through less-than-equal" in {
      {
        val tree = compile("(//a + //b) <= 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compile("42 <= (//a + //b)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through greater-than" in {
      {
        val tree = compile("(//a + //b) > 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compile("42 > (//a + //b)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through greater-than-equal" in {
      {
        val tree = compile("(//a + //b) >= 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compile("42 >= (//a + //b)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through equality" in {
      {
        val tree = compile("(//a + //b) = 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compile("42 = (//a + //b)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through not-equality" in {
      {
        val tree = compile("(//a + //b) != 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compile("42 != (//a + //b)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through boolean and" in {
      {
        val tree = compile("(//a + //b) & 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compile("42 & (//a + //b)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through boolean or" in {
      {
        val tree = compile("(//a + //b) | 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compile("42 | (//a + //b)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through complementation" in {
      val tree = compile("!(//a + //b)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "propagate through negation" in {
      val tree = compile("neg (//a + //b)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "propagate through parenthetical" in {
      val tree = compile("(//a + //b)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
  }
  
  "constraining expression determination" should {
    "leave values unconstrained" in {
      compile("42").constrainingExpr must beNone
    }
    
    "leave loads unconstrained when outside a relation" in {
      compile("//foo").constrainingExpr must beNone
    }
    
    "constrain loads within a relation" in {
      {
        val Relate(_, from, _, in) = compile("//foo ~ //bar //foo")
        in.constrainingExpr must beSome(from)
      }
      
      {
        val Relate(_, _, to, in) = compile("//foo ~ //bar //bar")
        in.constrainingExpr must beSome(to)
      }
    }
    
    "leave unconnected loads unconstrained within a relation" in {
      val Relate(_, from, _, in) = compile("//foo ~ //bar //baz")
      in.constrainingExpr must beNone
    }
    
    "propagate constraints through a nested relation" in {
      {
        val Relate(_, from1, to1, Relate(_, from2, to2, in)) = compile("""
          | //foo ~ //bar
          |   //foo ~ //baz
          |     //foo""".stripMargin)
        
        in.constrainingExpr must beSome(from2)
      }
      
      {
        val Relate(_, from1, to1, Relate(_, from2, to2, in)) = compile("""
          | //foo ~ //bar
          |   //foo ~ //baz
          |     //bar""".stripMargin)
        
        in.constrainingExpr must beSome(to1)
      }
      
      {
        val Relate(_, from1, to1, Relate(_, from2, to2, in)) = compile("""
          | //foo ~ //bar
          |   //foo ~ //baz
          |     //baz""".stripMargin)
        
        in.constrainingExpr must beSome(to2)
      }
    }
  }
   
  val exampleDir = new File("quirrel/examples")
  
  if (exampleDir.exists) {
    "specification examples" >> {
      for (file <- exampleDir.listFiles if file.getName endsWith ".qrl") {
        file.getName >> {
          val result = compile(LineStream(Source.fromFile(file)))
          result.provenance mustNotEqual NullProvenance
          result.errors must beEmpty
        }
      }
    }
  } else {
    "specification examples" >> skipped
  }
}


