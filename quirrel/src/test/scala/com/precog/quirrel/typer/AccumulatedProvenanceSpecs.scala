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

object AccumulatedProvenanceSpecs extends Specification
    with StubPhases
    with Compiler
    with ProvenanceChecker 
    with RandomLibrary {

  import ast._
  
  "accumulatedProvenance computation" should {
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

      "Forall" >> {
        {
        val tree = compile("""
          | foo := //foo
          | forall 'a {bar: count(foo where foo.a = 'a)}
          """.stripMargin)
        tree.accumulatedProvenance must beLike { case Some(Vector(DynamicProvenance(_))) => ok }
        tree.errors must beEmpty
        }
        {
        val tree = compile("""
          | foo := //foo
          | forall 'a {bar: foo where foo.a = 'a}
          """.stripMargin)
        tree.accumulatedProvenance must beLike { case Some(Vector(DynamicProvenance(_))) => ok }
        tree.errors must beEmpty
        }
        {
        val tree = compile("""
          | foo := //foo
          | forall 'a foo where foo.a = 'a
          """.stripMargin)
        tree.accumulatedProvenance must beLike { case Some(Vector(DynamicProvenance(_))) => ok }
        tree.errors must beEmpty
        }        
        {
        val tree = compile("""
          | foo := //foo
          | foobar := forall 'a foo where foo.a = 'a
          | foobar
          """.stripMargin)
        tree.accumulatedProvenance must beLike { case Some(Vector(DynamicProvenance(_))) => ok }
        tree.errors must beEmpty
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
      
      "MetaDescent" >> {
        {
          val tree = compile("//foo@bar")
          tree.accumulatedProvenance must beLike { case Some(Vector(StaticProvenance("/foo"))) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("""{foo: "bar"}@foo""")
          tree.accumulatedProvenance must beLike { case Some(Vector()) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("{foo: //bar}@foo")
          tree.accumulatedProvenance must beLike { case Some(Vector(StaticProvenance("/bar"))) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("{foo: 1, bar: //baz}@foo")
          tree.accumulatedProvenance must beLike { case Some(Vector(StaticProvenance("/baz"))) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("{foo: //fob, bar: //baz}@bar")
          tree.accumulatedProvenance must beLike { case Some(Vector(StaticProvenance("/fob"), StaticProvenance("/baz"))) => ok }
          tree.errors mustEqual Set(OperationOnUnrelatedSets)
        }
        {
          val tree = compile("{foo: bar(5), baz: 1}@baz")
          tree.accumulatedProvenance must beLike { case None => ok }
          tree.errors mustEqual Set(UndefinedFunction(Identifier(Vector(), "bar")))
        }        
        {
          val tree = compile("true@foo")
          tree.accumulatedProvenance must beLike { case Some(Vector()) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("[1,2,3]@foo")
          tree.accumulatedProvenance must beLike { case Some(Vector()) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("[]@foo")
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
          val tree = compile("foo := //foo f('a, 'b) := foo + 'a where foo.b = 'b f(2)")
          tree.accumulatedProvenance must beLike { case Some(Vector(DynamicProvenance(_))) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("foo := //foo forall 'b foo + 2 where foo.b = 'b")
          tree.accumulatedProvenance must beLike { case Some(Vector(DynamicProvenance(_))) => ok }
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
          tree.accumulatedProvenance must beLike { case Some(Vector(DynamicProvenance(_))) => ok }
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
          tree.accumulatedProvenance must beLike { case Some(Vector(DynamicProvenance(_))) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compile("""
            clicks := //clicks
            views  := //views
            clickthroughRate('page) :=
              {page: 'page, ctr: count(clicks where clicks.pageId = 'page) / count(views where views.pageId = 'page)}
            clickthroughRate""")

          tree.accumulatedProvenance must beLike { case Some(Vector(DynamicProvenance(_))) => ok }
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
  }
}
