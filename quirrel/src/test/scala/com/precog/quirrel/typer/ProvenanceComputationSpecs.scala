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

object ProvenanceComputationSpecs extends Specification
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

    "determine provenance coming out of a forall" in {
      {
        val tree = compile("""
          | foo := //foo
          | forall 'a {bar: sum(foo where foo.a = 'a)}
          """.stripMargin)
        tree.provenance must beLike { case DynamicProvenance(_) => ok }
        tree.errors must beEmpty
      }
      {
        val tree = compile("""
          | foo := //foo
          | obj := forall 'a {bar: sum(foo where foo.a = 'a)}
          | obj
          """.stripMargin)
        tree.provenance must beLike { case DynamicProvenance(_) => ok }
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

      "Forall" >> {
        {
          val tree = compile("""
            | foo := //foo
            | foobar := forall 'a {a: 'a, bar: count(foo where foo.a = 'a)}
            | foobaz := forall 'b {b: 'b, baz: count(foo where foo.b = 'b)}
            | foobar union foobaz
            """.stripMargin)
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
        {
          val tree = compile("""
            | foo := //foo
            | foobar := forall 'a {a: 'a, bar: count(foo where foo.a = 'a)}
            | foobar union 5
            """.stripMargin)

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


