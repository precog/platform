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

import com.precog.bytecode.IdentityPolicy

import com.codecommit.gll.LineStream
import org.specs2.mutable.Specification

import java.io.File
import scala.io.Source

object ProvenanceComputationSpecs extends Specification
    with StubPhases
    with CompilerUtils
    with Compiler
    with ProvenanceChecker 
    with StaticLibrarySpec {

  import ast._
  import library._
  
  "provenance computation" should {
    "compute result provenance correctly in BIF1" in {
      forall(lib1) { f =>
        val tree = compileSingle("""
          clicks := //clicks
          foo(a) := %s(a) 
          foo(clicks)""".format(f.fqn))

        tree.provenance mustEqual StaticProvenance("/clicks")
        tree.errors filterNot isWarning must beEmpty
      }
    }
    
    "compute result provenance correctly in BIF2" in {
      forall(lib2) { f =>
        val tree = compileSingle("""
          clicks := //clicks
          foo(a, b) := %s(a, b) 
          foo(clicks.a, clicks.b)""".format(f.fqn))

        tree.provenance mustEqual StaticProvenance("/clicks")
        tree.errors filterNot isWarning must beEmpty
      }
    }     

    "compute result provenance correctly in a BIR" in {
      forall(libReduction) { f =>
        val tree = compileSingle("""
          clicks := //clicks
          foo(a) := %s(a) 
          foo(clicks.a)""".format(f.fqn))

        tree.provenance mustEqual ValueProvenance

        tree.errors filterNot isWarning must beEmpty
      }
    } 
    "compute result provenance correctly in a morph1" in {
      forall(libMorphism1) { f =>
        val tree = compileSingle("""
          clicks := //clicks
          foo(a) := %s(a) 
          foo(clicks.a)""".format(f.fqn))

        if (f.namespace == Vector("std", "random")) {
          tree.provenance mustEqual InfiniteProvenance
          tree.errors mustEqual Set(CannotUseDistributionWithoutSampling)
        } else {
          f.idPolicy match {
            case IdentityPolicy.Product(left, right) => {
              tree.provenance must beLike {
                case ProductProvenance(DynamicProvenance(_), StaticProvenance("/clicks")) => ok
              }
              tree.errors filterNot isWarning must beEmpty
            }

            case _: IdentityPolicy.Retain => {
              tree.provenance mustEqual StaticProvenance("/clicks")
              tree.errors filterNot isWarning must beEmpty
            }
            
            case IdentityPolicy.Synthesize => {
              tree.provenance must beLike {
                case DynamicProvenance(_) => ok
              }
              tree.errors filterNot isWarning must beEmpty
            }
            
            case IdentityPolicy.Strip => {
              tree.provenance mustEqual ValueProvenance
              tree.errors filterNot isWarning must beEmpty
            }
          }
        }
      }
    } 
    "compute result provenance correctly in a morph2" in {
      forall(libMorphism2) { f =>
        val tree = compileSingle("""
          clicks := //clicks
          foo(a, b) := %s(a, b) 
          foo(clicks.a, clicks.b)""".format(f.fqn))
          
        f.idPolicy match {
          case IdentityPolicy.Product(left, right) => {
            tree.provenance must beLike {
              case ProductProvenance(DynamicProvenance(_), StaticProvenance("/clicks")) => ok
            }
            tree.errors filterNot isWarning must beEmpty
          }

          case IdentityPolicy.Retain.Right => {
            tree.provenance mustEqual StaticProvenance("/clicks")
            tree.errors filterNot isWarning must beEmpty
          }
          
          case IdentityPolicy.Retain.Left => {
            tree.provenance mustEqual StaticProvenance("/clicks")
            tree.errors filterNot isWarning must beEmpty
          }
          
          case IdentityPolicy.Retain.Merge => {
            tree.provenance mustEqual StaticProvenance("/clicks")
            tree.errors filterNot isWarning must beEmpty
          }
          
          case IdentityPolicy.Synthesize => {
            tree.provenance must beLike {
              case DynamicProvenance(_) => ok
            }
          }
          
          case IdentityPolicy.Strip => {
            tree.provenance mustEqual ValueProvenance
          }
        }

        tree.errors filterNot isWarning must beEmpty
      }
    }

    "identify let according to its right expression" in {   // using raw, no-op let
      {
        val tree = parseSingle("a := 1 1")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = parseSingle("a := 1 //foo")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = parseSingle("a := 1 (new 1)")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      {
        val tree = parseSingle("a := 1 (distinct(1))")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "preserve provenance through let for unquantified function" in {
      val input = """
        | interactions := //interactions
        | bounds := solve 'it
        |   interactions.time where interactions = 'it
        | init := bounds
        | init + bounds""".stripMargin
        
      val tree = compileSingle(input)
      
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty
    }
    
    "identify import according to its child expression" in {
      val tree = compileSingle("import std //foo")
      tree.provenance mustEqual StaticProvenance("/foo")
      tree.errors must beEmpty
    }
    
    "identify assert according to its right expression" in {
      val tree = compileSingle("assert //bar //foo")
      tree.provenance mustEqual StaticProvenance("/foo")
      tree.errors must beEmpty
    }
    
    "identify new as dynamic" in {
      val tree = compileSingle("new 1")
      tree.provenance must beLike {
        case DynamicProvenance(_) => ok
      }
      tree.errors must beEmpty
    }
    
    "identify new of unquantified function as distinct from the function" in {
      val input = """
        | histogram := solve 'a
        |   'a + count(//foo where //foo = 'a)
        | 
        | histogram' := new histogram
        | 
        | histogram'""".stripMargin
      
      val tree @ Let(_, _, _, _, Let(_, _, _, New(_, target), result)) = compileSingle(input)
      
      target.provenance must beLike { case DynamicProvenance(_) => ok }
      result.provenance must beLike { case DynamicProvenance(_) => ok }
      target.provenance mustNotEqual result.provenance
      
      tree.errors must beEmpty
    }
    
    "identify relate according to its last expression" in {
      {
        val tree = compileSingle("//a ~ //b 3")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("//a ~ //b //foo")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("//a ~ //b (new 1)")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify undefined as undefined" in {
      val tree = compileSingle("undefined")
      tree.provenance mustEqual UndefinedProvenance
      tree.errors must beEmpty
    }

    "identify tic-var as value" in {
      val tree @ Let(_, _, _, body, _) = compileSingle("a(foo) := foo a(42)")
      body.provenance mustEqual ParamProvenance(Identifier(Vector(), "foo"), tree)
      tree.errors must beEmpty
    }
    
    "identify string as value" in {
      val tree = compileSingle("\"foo\"")
      tree.provenance mustEqual ValueProvenance
      tree.errors must beEmpty
    }
    
    "identify num as value" in {
      val tree = compileSingle("42")
      tree.provenance mustEqual ValueProvenance
      tree.errors must beEmpty
    }
    
    "identify boolean as value" in {
      val tree = compileSingle("true")
      tree.provenance mustEqual ValueProvenance
      tree.errors must beEmpty
    }    

    "identify null as value" in {
      val tree = compileSingle("null")
      tree.provenance mustEqual ValueProvenance
      tree.errors must beEmpty
    }
    
    "identify empty object definitions as value" in {
      val tree = compileSingle("{}")
      tree.provenance mustEqual ValueProvenance
      tree.errors must beEmpty
    }
    
    "identify object definition according to its properties" in {
      {
        val tree = compileSingle("{ a: 1, b: 2, c: 3}")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("{ a: 1, b: 2, c: //foo }")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("{ a: 1, b: 2, c: new 2 }")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      {
        val tree = compileSingle("{a: undefined, b: 2, c: 3}")
        tree.provenance mustEqual UndefinedProvenance
        tree.errors must beEmpty
      }
    }
    
    "identify empty array definitions as value" in {
      val tree = compileSingle("[]")
      tree.provenance mustEqual ValueProvenance
      tree.errors must beEmpty
    }
    
    "identify array definition according to its values" in {
      {
        val tree = compileSingle("[1, 2, 3]")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("[1, 2, //foo]")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("[1, 2, new 3]")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      {
        val tree = compileSingle("[4,5,undefined]")
        tree.provenance mustEqual UndefinedProvenance
        tree.errors must beEmpty
      }
    }
    
    "identify descent according to its child expression" in {
      {
        val tree = compileSingle("1.foo")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("(//bar).foo")
        tree.provenance mustEqual StaticProvenance("/bar")
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("(new 1).foo")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify metadescent according to its child expression" in {
      {
        val tree = compileSingle("1@foo")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("(//bar)@foo")
        tree.provenance mustEqual StaticProvenance("/bar")
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("(new 1)@foo")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify dereference according to its children" in {
      {
        val tree = compileSingle("1[2]")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("//foo[2]")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("1[//foo]")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("(new 1)[2]")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("1[new 2]")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    // TODO arity
    "identify built-in reduce dispatch as value" in {
      {
        val tree = compileSingle("count(//foo)")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }     
      
      {
        val tree = compileSingle("geometricMean(//foo)")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("max(//foo)")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("mean(//foo)")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("median(//foo)")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("min(//foo)")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("mode(//foo)")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("stdDev(//foo)")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("sum(//foo)")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("sumSq(//foo)")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }     
      
      {
        val tree = compileSingle("variance(//foo)")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
    }

    "determine provenance coming out of a solve" in {
      {
        val tree = compileSingle("""
          | foo := //foo
          | solve 'a {bar: sum(foo where foo.a = 'a)}
          """.stripMargin)
        tree.provenance must beLike { case DynamicProvenance(_) => ok }
        tree.errors must beEmpty
      }
      {
        val tree = compileSingle("""
          | foo := //foo
          | obj := solve 'a {bar: sum(foo where foo.a = 'a)}
          | obj
          """.stripMargin)
        tree.provenance must beLike { case DynamicProvenance(_) => ok }
        tree.errors must beEmpty
      }
      {
        val tree = compileSingle("""
          | foo := //foo
          | bar := //bar
          | solve 'a = bar.a
          |   count(foo where foo.a = 'a)
          """.stripMargin)
        tree.provenance must beLike { case DynamicProvenance(_) => ok }
        tree.errors must beEmpty
      }
      {
        val tree = compileSingle("""
          | foo := //foo
          | solve 'a = foo.a
          |   {count: count(foo where foo.a < 'a), value: 'a}
          """.stripMargin)
        tree.provenance must beLike { case DynamicProvenance(_) => ok }
        tree.errors must beEmpty
      }
    }

    "determine provenance from constraints of a solve" in {
      {
        val tree = compileSingle("""
          | solve 'a = //foo + //bar
          |   {a: 'a}
          """.stripMargin)
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      {
        val tree = compileSingle("""
          | solve (//foo + //bar) 4
          """.stripMargin)
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(OperationOnUnrelatedSets, SolveLackingFreeVariables)
      }
    }

    "determine provenance from new" in {
      val tree = compileSingle("""
        | new (//foo + //bar) 
        """.stripMargin)
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }

    "identify distinct dispatch" in {
      {
        val tree = compileSingle("distinct(//foo)")
        tree.provenance must beLike { case DynamicProvenance(_) => ok }
        tree.errors must beEmpty
       
      }
    }

    "identify reduction dispatch according to its child" in {
      forall(libReduction) { f =>
        val tree = compileSingle("%s(//foo)".format(f.fqn))
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
    }
    "identify op1 dispatch according to its child" in {
      forall(lib1) { f =>
        val tree = compileSingle("%s(//foo)".format(f.fqn))
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
    }

    "identify op1 dispatch given incorrect number of parameters" in {
      forall(lib1) { f =>
        val tree = compileSingle("%s(//foo, //bar)".format(f.fqn))
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(IncorrectArity(1, 2))
      }
    }

    "identify op2 dispatch according to its children given unrelated sets" in {
      forall(lib2) { f => 
        val tree = compileSingle("%s(//foo, //bar)".format(f.fqn))
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }

    "identify op2 dispatch according to its children given a load and a value" in {
      forall(lib2) { f =>
        val tree = compileSingle("""%s(//foo, "bar")""".format(f.fqn))
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
    }

    "identify op2 dispatch according to its children given set related by ~" in {
      forall(lib2) { f =>
        val tree = compileSingle("""//foo ~ //bar %s(//foo, //bar)""".format(f.fqn))
        tree.provenance mustEqual ProductProvenance(StaticProvenance("/foo"), StaticProvenance("/bar"))
        tree.errors must beEmpty
      }
    }
    
    "identify morph1 dispatch given incorrect number of parameters" in {
      forall(libMorphism1) { f =>
        val tree = compileSingle("%s(//foo, //bar)".format(f.fqn))
        tree.provenance mustEqual NullProvenance
        tree.errors filterNot isWarning mustEqual Set(IncorrectArity(1, 2))
      }
    }
    "identify morph1 dispatch according to its child" in {
      forall(libMorphism1) { f =>
        val tree = compileSingle("%s(//foo)".format(f.fqn))
        
        if (f.namespace == Vector("std", "random")) {
          tree.provenance mustEqual InfiniteProvenance
          tree.errors mustEqual Set(CannotUseDistributionWithoutSampling)
        } else {
          f.idPolicy match {
            case IdentityPolicy.Product(left, right) => {
              tree.provenance must beLike {
                case ProductProvenance(DynamicProvenance(_), StaticProvenance("/foo")) => ok
              }
              tree.errors filterNot isWarning must beEmpty
            }

            case _: IdentityPolicy.Retain => {
              tree.provenance mustEqual StaticProvenance("/foo")
              tree.errors filterNot isWarning must beEmpty
            }
            
            case IdentityPolicy.Synthesize => {
              tree.provenance must beLike {
                case DynamicProvenance(_) => ok
              }
              tree.errors filterNot isWarning must beEmpty
            }
            
            case IdentityPolicy.Strip => {
              tree.provenance mustEqual ValueProvenance
              tree.errors filterNot isWarning must beEmpty
            }
          }
        }
      }
    }

    "identify morph2 dispatch according to its children given unrelated sets" in {
      forall(libMorphism2) { f => 
        val tree = compileSingle("%s(//foo, //bar)".format(f.fqn))
        tree.provenance mustEqual NullProvenance
        tree.errors filterNot isWarning mustEqual Set(OperationOnUnrelatedSets)
      }
    }

    "identify morph2 dispatch according to its children given a load and a value" in {
      forall(libMorphism2) { f =>
        val tree = compileSingle("""%s(//foo, "bar")""".format(f.fqn))
        
        f.idPolicy match {
          case IdentityPolicy.Product(left, right) => {
            tree.provenance must beLike {
              case ProductProvenance(DynamicProvenance(_), StaticProvenance("/foo")) => ok
            }
            tree.errors filterNot isWarning must beEmpty
          }

          case IdentityPolicy.Retain.Right => {
            tree.provenance mustEqual ValueProvenance
            tree.errors filterNot isWarning must beEmpty
          }
          
          case IdentityPolicy.Retain.Left => {
            tree.provenance mustEqual StaticProvenance("/foo")
            tree.errors filterNot isWarning must beEmpty
          }
          
          case IdentityPolicy.Retain.Merge => {
            tree.provenance mustEqual StaticProvenance("/foo")
            tree.errors filterNot isWarning must beEmpty
          }

          case IdentityPolicy.Synthesize => {
            tree.provenance must beLike {
              case DynamicProvenance(_) => ok
            }
            tree.errors filterNot isWarning must beEmpty
          }
          
          case IdentityPolicy.Strip => {
            tree.provenance mustEqual ValueProvenance
            tree.errors filterNot isWarning must beEmpty
          }
        }
        
        tree.errors filterNot isWarning must beEmpty
      }
    }

    "identify morph2 dispatch according to its children given set related by ~" in {
      forall(libMorphism2) { f =>
        val tree = compileSingle("""//foo ~ //bar %s(//foo, //bar)""".format(f.fqn))
        
        f.idPolicy match {
          case IdentityPolicy.Product(left, right) => {
            val prov = ProductProvenance(StaticProvenance("/foo"), StaticProvenance("/bar"))
            tree.provenance must beLike {
              case ProductProvenance(DynamicProvenance(_), prov) => ok
            }
            tree.errors filterNot isWarning must beEmpty
          }

          case IdentityPolicy.Retain.Right => {
            tree.provenance mustEqual StaticProvenance("/bar")
            tree.errors filterNot isWarning must beEmpty
          }
          
          case IdentityPolicy.Retain.Left => {
            tree.provenance mustEqual StaticProvenance("/foo")
            tree.errors filterNot isWarning must beEmpty
          }
          
          case IdentityPolicy.Retain.Merge => {
            val prov = ProductProvenance(StaticProvenance("/foo"), StaticProvenance("/bar"))
            tree.provenance mustEqual prov
            tree.errors filterNot isWarning must beEmpty
          }
          
          case _: IdentityPolicy.Retain => { 
            tree.provenance mustEqual ProductProvenance(StaticProvenance("/foo"), StaticProvenance("/bar"))
            tree.errors filterNot isWarning must beEmpty
          }
          
          case IdentityPolicy.Synthesize => {
            tree.provenance must beLike {
              case DynamicProvenance(_) => ok
            }
            tree.errors filterNot isWarning must beEmpty
          }
          
          case IdentityPolicy.Strip => {
            tree.provenance mustEqual ValueProvenance
            tree.errors filterNot isWarning must beEmpty
          }
        }
        
        tree.errors filterNot isWarning must beEmpty
      }
    }
    "identify morph2 dispatch according to its children given sets not related" in {
      forall(libMorphism2) { f =>
        val tree = compileSingle("""foo(a, b) := %s(a, b) foo(//bar, //baz)""".format(f.fqn))

        tree.provenance mustEqual NullProvenance
        tree.errors filterNot isWarning mustEqual Set(OperationOnUnrelatedSets)
      }
    }

    
    "identify load dispatch with static params according to its path" in {
      {
        val tree = compileSingle("//foo")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("//bar")
        tree.provenance mustEqual StaticProvenance("/bar")
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("//bar/baz")
        tree.provenance mustEqual StaticProvenance("/bar/baz")
        tree.errors must beEmpty
      }
    }
    
    "identify load dispatch with non-static params as dynamic" in {
      {
        val tree = compileSingle("load(42)")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("a := 42 load(a)")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("load(count(42))")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("load(new 42)")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }      
      {
        val tree = compileSingle("""load("/clicks")""")
        tree.provenance mustEqual StaticProvenance("/clicks")
        tree.errors must beEmpty
      }
    }
    
    "identify dispatch to identity function by parameter" in {
      {
        val tree = compileSingle("id(a) := a id(42)")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("id(a) := a id(new 42)")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("id(a) := a id(//foo)")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }

    }
    
    "identify dispatch to value-modified identity function by parameter" in {
      {
        val tree = compileSingle("id(a) := a + 5 id(42)")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("id(a) := a + 5 id(new 42)")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("id(a) := a + 5 id(//foo)")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
    }
    
    "identify dispatch to new-modified identity function as dynamic" in {
      val tree = compileSingle("id(a) := a + new 42 id(24)")
      tree.provenance must beLike {
        case DynamicProvenance(_) => ok
      }
      tree.errors must beEmpty
    }
    
    "identify dispatch to load-modified identity function as static" in {
      val tree = compileSingle("id(a) := a + //foo id(24)")
      tree.provenance mustEqual StaticProvenance("/foo")
      tree.errors must beEmpty
    }
    
    "identify dispatch to simple operation function by unification of parameters" in {
      {
        val tree = compileSingle("fun(a, b) := a + b fun(1, 2)")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("fun(a, b) := a + b fun(//foo, 2)")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("fun(a, b) := a + b fun(1, //foo)")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("fun(a, b) := a + b fun(//foo, //foo)")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("fun(a, b) := a + b //foo ~ //bar fun(//foo, //bar)")
        tree.provenance mustEqual ProductProvenance(StaticProvenance("/foo"), StaticProvenance("/bar"))
        tree.errors must beEmpty
      }
    }
     
    "identify a case when a tic variable is not solvable in all cases" in {
      {
        val tree = compileSingle("""
        | clicks := //clicks
        | views := //views
        | a := solve 'b
        |   k := clicks.time where clicks.time = 'b
        |   j := views.time where views.time > 'b
        |   k ~ j
        |   {kay: k, jay: j}
        | a""".stripMargin)

        tree.provenance must beLike { case DynamicProvenance(_) => ok }
        tree.errors must beEmpty
      }
    }

    "identify dispatch to an unquantified value function as dynamic" in {
      {
        val tree = compileSingle("""
        | foo := //foo 
        | histogram := solve 'a 
        |   'a + count(foo where foo = 'a) 
        | histogram""".stripMargin)
        tree.provenance must beLike { case DynamicProvenance(_) => ok }
        tree.errors must beEmpty
      }
      
      {
        val input = """
          | histogram := solve 'a
          |   foo := //foo
          |   bar := //bar
          |   
          |   'a + count(foo ~ bar foo where foo = 'a & bar = 12)
          | 
          | histogram""".stripMargin
        
        val tree = compileSingle(input)
        tree.provenance must beLike { case DynamicProvenance(_) => ok }
        tree.errors must beEmpty
      }
      
      {
        val input = """
          | histogram := solve 'a
          |   foo := //foo
          |   bar := //bar
          |   
          |   foo' := foo where foo = 'a
          |   bar' := bar where bar = 'a
          | 
          |   'a + count(foo' ~ bar' foo + bar)
          | 
          | histogram""".stripMargin
        
        val tree = compileSingle(input)
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify dispatch to an unquantified function with relate as dynamic" in {
      val input = """
        | fun := solve 'a
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
        
      val tree = compileSingle(input)
      tree.provenance must beLike {
        case DynamicProvenance(_) => ok
      }
      tree.errors must beEmpty
    }

    "identify dispatch to unquantified function with a consistent dynamic provenance" in {
      val tree = compileSingle("""
        | foo := //foo
        | histogram := solve 'a
        |   'a + count(foo where foo = 'a) 
        | histogram + histogram""".stripMargin)   // if not consistent, binary op will fail
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty
    }
    
    "identify where according to its children" in {
      {
        val tree = compileSingle("1 where 2")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("//foo where 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("1 where //foo")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("new 1 where 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("1 where new 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }    
    "identify with according to its children" in {
      {
        val tree = compileSingle("1 with 2")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("//foo with 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("1 with //foo")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("new 1 with 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("1 with new 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    } 

    "identify union according to its children" >> {
      "Simple Union" >> {
        val tree = compileSingle("//clicks union 2")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(ProductProvenanceDifferentLength)
      }

      "Let" >> {
        {
          val tree = compileSingle("foo := //clicks foo union 2")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(ProductProvenanceDifferentLength)
        }       
        {
          val tree = compileSingle("foo := //clicks foo union //views")
          tree.provenance must beLike { case CoproductProvenance(StaticProvenance("/clicks"), StaticProvenance("/views")) => ok }
          tree.errors must beEmpty
        }      
      }

      "New" >> {
        {
          val tree = compileSingle("1 union new 2")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(ProductProvenanceDifferentLength)
        }
        {
          val tree = compileSingle("(new 2) union //clicks")
          tree.provenance must beLike { case CoproductProvenance(DynamicProvenance(_), StaticProvenance("/clicks")) => ok }
          tree.errors must beEmpty
        }
      }

      "Solve" >> {
        {
          val tree = compileSingle("""
            | foo := //foo
            | foobar := solve 'a {a: 'a, bar: count(foo where foo.a = 'a)}
            | foobaz := solve 'b {b: 'b, baz: count(foo where foo.b = 'b)}
            | foobar union foobaz
            """.stripMargin)
          tree.provenance must beLike { case CoproductProvenance(DynamicProvenance(_), DynamicProvenance(_)) => ok }
          tree.errors must beEmpty
        }
      }

      "Relate" >> {
        {
          val tree = compileSingle("//clicks ~ //views foo := //clicks + //views foo union 4")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(ProductProvenanceDifferentLength)
        }
        {
          val tree = compileSingle("//clicks ~ //views //foo union 4")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(ProductProvenanceDifferentLength)
        }
      }

      "Literals" >> {
        {
          val tree = compileSingle("""(1 union "foo") union (true union null) """)
          tree.provenance mustEqual ValueProvenance
          tree.errors must beEmpty
        }
      }

      "ObjectDef" >> {
        {
          val tree = compileSingle("{foo: (//foobar).a, bar: (//foobar).b} union //baz")
          tree.provenance must beLike { case CoproductProvenance(StaticProvenance("/foobar"), StaticProvenance("/baz")) => ok }
          tree.errors must beEmpty
        }      
        {
          val tree = compileSingle("{foo: 5} union 6")
          tree.provenance mustEqual ValueProvenance
          tree.errors must beEmpty
        }        
        {
          val tree = compileSingle("false union {foo: foo(3)}")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(UndefinedFunction(Identifier(Vector(), "foo")))
        }        
      }

      "ArrayDef" >> {
        {
          val tree = compileSingle("[4,5,6] union 7")
          tree.provenance mustEqual ValueProvenance
          tree.errors must beEmpty
        }        
        {
          val tree = compileSingle("false union [foo(5), {bar: 10}]")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(UndefinedFunction(Identifier(Vector(), "foo")))
        }        
      }

      "Descent" >> {
        {
          val tree = compileSingle("(//foo).a union 6")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(ProductProvenanceDifferentLength)
        }  
        {
          val tree = compileSingle("6 union {foo: 5}.foo")
          tree.provenance mustEqual ValueProvenance
          tree.errors must beEmpty
        }
      }

      "Deref" >> {
        {
          val tree = compileSingle("//clicks[1] union 6")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(ProductProvenanceDifferentLength)
        }
        {
          val tree = compileSingle("foo := [3,4,5] foo[1] union 6")
          tree.provenance mustEqual ValueProvenance
          tree.errors must beEmpty
        }
      }

      "Dispatch" >> {
        {
          val tree = compileSingle("//foo union //bar")
          tree.provenance must beLike { case CoproductProvenance(StaticProvenance("/foo"), StaticProvenance("/bar")) => ok }
          tree.errors must beEmpty
        }      
        {
          val tree = compileSingle("""foo::bar("baz") union 6""")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(UndefinedFunction(Identifier(Vector("foo"), "bar")))
        }      
        {
          val tree = compileSingle("//foo union 2")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(ProductProvenanceDifferentLength)
        }
        
        {
          val tree = compileSingle("1 union //foo")        
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(ProductProvenanceDifferentLength)
        }
        {
          val tree = compileSingle("distinct((//clicks).bar) union //bar")
          tree.provenance must beLike { case CoproductProvenance(DynamicProvenance(_), StaticProvenance("/bar")) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compileSingle("sum((//clicks).bar) union false")
          tree.provenance mustEqual ValueProvenance
          tree.errors must beEmpty
        }
        {
          val tree = compileSingle("sum((//clicks).bar, 100) union false")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(IncorrectArity(1, 2))
        }
        {
          forall(lib1) { f =>
            val tree = compileSingle("%s(10) union {a: 33}".format(f.fqn))
            tree.provenance mustEqual ValueProvenance
            tree.errors filterNot isWarning must beEmpty
          }
        }
        {
          forall(lib2) { f =>
            val tree = compileSingle("%s((//bar).foo, (//bar).ack) union //bar".format(f.fqn))
            tree.provenance mustEqual StaticProvenance("/bar")
            tree.errors filterNot isWarning must beEmpty
          }
        }        
        {
          forall(libReduction) { f =>
            val tree = compileSingle("%s((//bar).foo) union [1, 9]".format(f.fqn))
            tree.provenance mustEqual ValueProvenance 
            tree.errors filterNot isWarning must beEmpty
          }
        }          
        {
          forall(libReduction) { f =>
            val tree = compileSingle("%s((//bar).foo) union //foo".format(f.fqn))
            tree.provenance mustEqual NullProvenance
            tree.errors filterNot isWarning mustEqual Set(ProductProvenanceDifferentLength)
          }
        }        
        {
          forall(libMorphism1) { f =>
            val tree = compileSingle("%s((//bar).foo) union //baz".format(f.fqn))
            
            if (f.namespace == Vector("std", "random")) {
              tree.provenance mustEqual NullProvenance 
              tree.errors filterNot isWarning mustEqual Set(CannotUseDistributionWithoutSampling)
            } else {
              f.idPolicy match {
                case IdentityPolicy.Product(left, right) => {
                  tree.provenance mustEqual NullProvenance
                  tree.errors mustEqual Set(ProductProvenanceDifferentLength)
                }

                case _: IdentityPolicy.Retain => {
                  tree.provenance must beLike { case CoproductProvenance(StaticProvenance("/bar"), StaticProvenance("/baz")) => ok }
                  tree.errors filterNot isWarning must beEmpty
                }
                
                case IdentityPolicy.Synthesize => {
                  tree.provenance must beLike {
                    case CoproductProvenance(DynamicProvenance(_), StaticProvenance("/baz")) => ok
                  }
                  tree.errors filterNot isWarning must beEmpty
                }
                
                case IdentityPolicy.Strip => {
                  tree.provenance mustEqual NullProvenance
                  tree.errors filterNot isWarning mustEqual Set(ProductProvenanceDifferentLength)
                }
              }
            }
          }
        }        
        {
          forall(libMorphism2) { f =>
            val tree = compileSingle("%s((//bar).foo, (//bar).ack) union //baz".format(f.fqn))
            
            f.idPolicy match {
              case IdentityPolicy.Product(left, right) => {
                tree.provenance mustEqual NullProvenance
                tree.errors filterNot isWarning mustEqual Set(ProductProvenanceDifferentLength)
              }

              case IdentityPolicy.Retain.Right => {
                val prov = CoproductProvenance(StaticProvenance("/bar"), StaticProvenance("/baz"))
                tree.provenance mustEqual prov
                tree.errors filterNot isWarning must beEmpty
              }
              
              case IdentityPolicy.Retain.Left => {
                val prov = CoproductProvenance(StaticProvenance("/bar"), StaticProvenance("/baz"))
                tree.provenance mustEqual prov
                tree.errors filterNot isWarning must beEmpty
              }
              
              case IdentityPolicy.Retain.Merge => {
                tree.provenance must beLike {
                  case CoproductProvenance(StaticProvenance("/bar"), StaticProvenance("/baz")) => ok
                }
                tree.errors filterNot isWarning must beEmpty
              }
              
              case IdentityPolicy.Synthesize => {
                tree.provenance must beLike {
                  case CoproductProvenance(DynamicProvenance(_), StaticProvenance("/baz")) => ok
                }
                tree.errors filterNot isWarning must beEmpty
              }
              
              case IdentityPolicy.Strip => {
                tree.provenance mustEqual NullProvenance
                tree.errors filterNot isWarning mustEqual Set(ProductProvenanceDifferentLength)
              }
            }
          }
        }
        {
          forall(libMorphism2) { f =>
            val tree = compileSingle("%s(//bar, //foo) union //ack".format(f.fqn))
            tree.provenance mustEqual NullProvenance
            tree.errors filterNot isWarning mustEqual Set(OperationOnUnrelatedSets)
          }
        }
        {
          val tree = compileSingle("f(a) := a intersect undefined f(//foo)")
          tree.provenance mustEqual UndefinedProvenance
          tree.errors must beEmpty
        }
        {
          val tree = compileSingle("f(a) := a union undefined f(//foo)")
          tree.provenance mustEqual StaticProvenance("/foo")
          tree.errors must beEmpty
        }
        {
          val tree = compileSingle("f(a) := g(b) := b union undefined g(a) f(//foo)")
          tree.provenance mustEqual StaticProvenance("/foo")
          tree.errors must beEmpty
        }
        {
          val tree = compileSingle("f := true union false f")
          tree.provenance mustEqual ValueProvenance
          tree.errors must beEmpty
        }
        {
          val tree = compileSingle("f(a) := ((//foobar).a union (//barfoo).a) where (//foobar).a = a f(10)")
          tree.provenance mustEqual StaticProvenance("/foobar")
          tree.errors must beEmpty
        }
        {
          val tree = compileSingle("f(a) := //foobar where (//foobar).a = a f(10) union 12")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(ProductProvenanceDifferentLength)
        }
        {
          val tree = compileSingle("f := solve 'a //foobar where (//foobar).a = 'a f union //baz")
          tree.provenance must beLike { case CoproductProvenance(DynamicProvenance(_), StaticProvenance("/baz")) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compileSingle(""" 
            clicks := //clicks
            views := //views
            clicks ~ views
            sum := clicks.time + views.time
            sum union //campaigns
            """)
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(ProductProvenanceDifferentLength)
        }
        {
          val tree = compileSingle("(//foo union //bar) + //bar")
          tree.provenance mustEqual StaticProvenance("/bar")
          tree.errors must beEmpty
        }
      }

      "If/Else" >> {
        {
          val tree = compileSingle("if (//bar union //baz) then //bar else //baz")
          tree.provenance must beLike { case CoproductProvenance(StaticProvenance("/bar"), StaticProvenance("/baz")) => ok }
          tree.errors must beEmpty
        }
        // Regression test for #PLATFORM-652
        {
          val tree = compileSingle("if //foo then 1 else 0")
          tree.provenance mustEqual StaticProvenance("/foo")
          tree.errors mustEqual Set()
        }
        {
          val tree = compileSingle("if //foo then //bar else //baz")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(OperationOnUnrelatedSets)
        }
        {
          val tree = compileSingle("if //bar then //bar else //baz")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(OperationOnUnrelatedSets)
        }
        {
          val tree = compileSingle("if //baz then //bar else //baz")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(OperationOnUnrelatedSets)
        }
      }

      "Where" >> {
        {
          val tree = compileSingle("(//foo where (//foo).a = 10) union //baz")
          tree.provenance must beLike { case CoproductProvenance(StaticProvenance("/foo"), StaticProvenance("/baz")) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compileSingle("""
            //foo ~ //bar ~ //baz 
            ({a: //baz - //foo} where true) union //foo + //bar""")
          tree.provenance must beLike { case CoproductProvenance(ProductProvenance(StaticProvenance("/baz"), StaticProvenance("/foo")), ProductProvenance(StaticProvenance("/foo"), StaticProvenance("/bar"))) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compileSingle("(//ack where (//achoo).foo >= 3) union 12") 
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(OperationOnUnrelatedSets)
        }
      }

      "With" >> {
        {
          val tree = compileSingle("(//foo with {a: 1}) union //baz")
          tree.provenance must beLike { case CoproductProvenance(StaticProvenance("/foo"), StaticProvenance("/baz")) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compileSingle("(null with {}) union //baz")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(ProductProvenanceDifferentLength)
        }
      }

      "Union/Intersect" >> {
        {
          val tree = compileSingle("(//foo union {a: 1}) union //baz")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(ProductProvenanceDifferentLength)
        }
        {
          val tree = compileSingle("undefined union //baz")
          tree.provenance mustEqual StaticProvenance("/baz")
          tree.errors must beEmpty
        }
        {
          val tree = compileSingle("undefined intersect {}")
          tree.provenance mustEqual UndefinedProvenance
          tree.errors must beEmpty
        }
        {
          val tree = compileSingle("(null intersect {}) union 10")
          tree.provenance mustEqual ValueProvenance
          tree.errors must beEmpty
        }
        {
          val tree = compileSingle("((//foo).a + (//foo).b union //baz) union 12")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(ProductProvenanceDifferentLength)
        }
        {
          val tree = compileSingle("""
            | foo := //foo
            | foobar := solve 'a {a: 'a, bar: count(foo where foo.a = 'a)}
            | foobar union 5
            """.stripMargin)

          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(ProductProvenanceDifferentLength)
        }
        {
          val tree = compileSingle("""
            | billing := //billing
            | billing' := new billing
            | billing'' := new billing
            |
            | billing union billing' union billing''
            | """.stripMargin)

          tree.provenance must beLike { case CoproductProvenance(CoproductProvenance(StaticProvenance("/billing"), _), _) => ok }
          tree.errors must beEmpty
        }
        // Regression test for Pivotal #37558157
        {
          val tree = compileSingle("""
            | athletes := //summer_games/athletes
            | 
            | firstHalf := athletes.Name where athletes.Population < 1000
            | secondHalf := athletes.Name where athletes.Population > 1000
            | 
            | {name: (firstHalf union secondHalf)} with {country: athletes.Countryname}
            | """.stripMargin)

          tree.provenance mustEqual StaticProvenance("/summer_games/athletes")
          tree.errors must beEmpty
        }
      }

      "Add/Sub/Mul/Div" >> {
        {
          val tree = compileSingle("1 - 2 union 3 + 4")
          tree.provenance mustEqual ValueProvenance
          tree.errors must beEmpty
        }
        {
          val tree = compileSingle("1 * //foo union //bazbarfoobam / 8")
          tree.provenance must beLike { case CoproductProvenance(StaticProvenance("/foo"), StaticProvenance("/bazbarfoobam")) => ok }
          tree.errors must beEmpty
        }
      }

      "Lt/LtEq/Gt/GtEq/Eq/NotEq" >> {
        {
          val tree = compileSingle("""(1 < 2) union ("there's a knot in this string") != "NOPE!" """)
          tree.provenance mustEqual ValueProvenance
          tree.errors must beEmpty
        }
        {
          val tree = compileSingle("((//foo).a <= 3) union (//iamasquirrel = 3)")
          tree.provenance must beLike { case CoproductProvenance(StaticProvenance("/foo"), StaticProvenance("/iamasquirrel")) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compileSingle("(4 > 999999) union ((//didsomeonesayoink).moooo >= 122)")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(ProductProvenanceDifferentLength)
        }
      }

      "And/Or" >> {
        {
          val tree = compileSingle("""(1 & true) union (4 | null) """)
          tree.provenance mustEqual ValueProvenance
          tree.errors must beEmpty
        }
      }

      "Comp" >> {
        {
          val tree = compileSingle("4 union !true")
          tree.provenance mustEqual ValueProvenance
          tree.errors must beEmpty
        }      
      }

      "Neg" >> {
        {
          val tree = compileSingle("neg 3 union 4")
          tree.provenance mustEqual ValueProvenance
          tree.errors must beEmpty
        }      
      }
      
      "Paren" >> {
        {
          val tree = compileSingle("(//foo) union //bar")
          tree.provenance must beLike { case CoproductProvenance(StaticProvenance("/foo"), StaticProvenance("/bar")) => ok }
          tree.errors must beEmpty
        }
        {
          val tree = compileSingle("({}) union ([])")
          tree.provenance mustEqual ValueProvenance
          tree.errors must beEmpty
        }
      }
    }  

    "accept user-defined union, intersect, and difference" in {
      {
        val tree = compileSingle("foo := //baz union //bar foo")
        tree.provenance must beLike { case CoproductProvenance(StaticProvenance("/baz"), StaticProvenance("/bar")) => ok }
        tree.errors must beEmpty
      }
      {
        val tree = compileSingle("foo(x) := //baz union x union //qux foo(//bar)")
        tree.provenance must beLike { case CoproductProvenance(CoproductProvenance(StaticProvenance("/baz"), StaticProvenance("/bar")), StaticProvenance("/qux")) => ok }
        tree.errors must beEmpty
      }
      {
        val tree = compileSingle("foo := //baz intersect //bar foo")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(IntersectProvenanceDifferentLength)
      }
      {
        val tree = compileSingle("foo := (//foo union //bar) intersect (//bar union //baz) foo")
        tree.provenance mustEqual StaticProvenance("/bar")
        tree.errors must beEmpty
      }
      {
        val tree = compileSingle("foo := //baz difference //baz foo")
        tree.provenance must beLike { case StaticProvenance("/baz") => ok }
        tree.errors must beEmpty
      }
    }

    "accept user-defined function within a user-defined function" in {
      {
        val tree = compileSingle("""
          foo(a) := 
            bar(b) :=
              //clicks where (//clicks).baz = b
            bar(a)
          foo(2)""")
        tree.provenance must beLike { case StaticProvenance("/clicks") => ok }
        tree.errors must beEmpty
      }      
      {
        val tree = compileSingle("""
          foo(a) := 
            bar := solve 'b
              //clicks where (//clicks).baz = 'b + a
            bar
          foo(2)""")
        tree.provenance must beLike { case DynamicProvenance(_) => ok }
        tree.errors must beEmpty
      }
    }

    "check provenance of partially-quantified function" in {
      val tree = compileSingle("""
        foo := solve 'a
          //clicks where (//clicks).a = 'a
        foo""")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty
    }

    "identify intersect according to its children" in {
      {
        val tree = compileSingle("1 intersect 2")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("//foo intersect 2")
        tree.provenance mustEqual NullProvenance
        tree.errors must contain(IntersectProvenanceDifferentLength)
      }
      
      {
        val tree = compileSingle("1 intersect //foo")        
        tree.provenance mustEqual NullProvenance
        tree.errors must contain(IntersectProvenanceDifferentLength)

      }
      
      {
        val tree = compileSingle("new (1 intersect 2)")
        tree.provenance must beLike { case DynamicProvenance(_) => ok }
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("1 intersect new 2")
        tree.provenance mustEqual NullProvenance
        tree.errors must contain(IntersectProvenanceDifferentLength)
      }
      
      {
        val tree = compileSingle("//foo intersect //bar")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(IntersectProvenanceDifferentLength)
      }
    }  

    "identify undefined in operations" in {
      {
        val tree = compileSingle("1 * undefined")
        tree.provenance mustEqual UndefinedProvenance
        tree.errors must beEmpty
      }
    }

    "identify addition according to its children" in {
      {
        val tree = compileSingle("1 + 2")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("//foo + 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("1 + //foo")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("new 1 + 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("1 + new 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify subtraction according to its children" in {
      {
        val tree = compileSingle("1 - 2")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("//foo - 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("1 - //foo")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("new 1 - 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("1 - new 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify multiplication according to its children" in {
      {
        val tree = compileSingle("1 * 2")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("//foo * 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("1 * //foo")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("new 1 * 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("1 * new 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify division according to its children" in {
      {
        val tree = compileSingle("1 / 2")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("//foo / 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("1 / //foo")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("new 1 / 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("1 / new 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify mod according to its children" in {
      {
        val tree = compileSingle("1 % 2")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("//foo % 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("1 % //foo")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("new 1 % 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("1 % new 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify less-than according to its children" in {
      {
        val tree = compileSingle("1 < 2")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("//foo < 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("1 < //foo")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("new 1 < 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("1 < new 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify less-than-equal according to its children" in {
      {
        val tree = compileSingle("1 <= 2")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("//foo <= 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("1 <= //foo")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("new 1 <= 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("1 <= new 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify greater-than according to its children" in {
      {
        val tree = compileSingle("1 > 2")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("//foo > 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("1 > //foo")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("new 1 > 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("1 > new 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify greater-than-equal according to its children" in {
      {
        val tree = compileSingle("1 >= 2")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("//foo >= 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("1 >= //foo")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("new 1 >= 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("1 >= new 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify equal according to its children" in {
      {
        val tree = compileSingle("1 = 2")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("//foo = 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("1 = //foo")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("new 1 = 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("1 = new 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify not-equal according to its children" in {
      {
        val tree = compileSingle("1 != 2")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("//foo != 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("1 != //foo")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("new 1 != 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("1 != new 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify boolean and according to its children" in {
      {
        val tree = compileSingle("1 & 2")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("//foo & 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("1 & //foo")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("new 1 & 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("1 & new 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify boolean or according to its children" in {
      {
        val tree = compileSingle("1 | 2")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("//foo | 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("1 | //foo")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("new 1 | 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("1 | new 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify complement according to its child" in {
      {
        val tree = compileSingle("!1")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("!//foo")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("!(new 1)")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify negation according to its child" in {
      {
        val tree = compileSingle("neg 1")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("neg //foo")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("neg (new 1)")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify parenthetical according to its child" in {
      {
        val tree = compileSingle("(1)")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("(//foo)")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compileSingle("(new 1)")
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
          val result = compileSingle(LineStream(Source.fromFile(file)))
          result.provenance mustNotEqual NullProvenance
          result.errors must beEmpty
        }
      }
    }
  } else {
    "specification examples" >> skipped
  }
  
  
  private def parseSingle(str: LineStream): Expr = {
    val set = parse(str)
    set must haveSize(1)
    set.head
  }
  
  private def parseSingle(str: String): Expr = parseSingle(LineStream(str))
}


