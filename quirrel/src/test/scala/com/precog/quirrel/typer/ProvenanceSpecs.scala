package com.precog
package quirrel
package typer

import bytecode.RandomLibrary
import edu.uwm.cs.gll.LineStream
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
    "identify let according to its right expression" in {   // using raw, no-op let
      {
        val tree = parse("a := 1 1")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("a := 1 load(//foo)")
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
    }
    
    "preserve provenance through let for unquantified function" in {
      val input = """
        | interactions := load(//interactions)
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
        |   'a + count(load(//foo) where load(//foo) = 'a)
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
        val tree = compile("load(//a) ~ load(//b) 3")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("load(//a) ~ load(//b) load(//foo)")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("load(//a) ~ load(//b) (new 1)")
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
        val tree = compile("{ a: 1, b: 2, c: load(//foo) }")
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
        val tree = compile("[1, 2, load(//foo)]")
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
        val tree = compile("load(//bar).foo")
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
        val tree = compile("load(//foo)[2]")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1[load(//foo)]")
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
        val tree = compile("count(load(//foo))")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("max(load(//foo))")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("mean(load(//foo))")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("median(load(//foo))")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("min(load(//foo))")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("mode(load(//foo))")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("stdDev(load(//foo))")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("sum(load(//foo))")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
    }

    "identify built-in non-reduce dispatch of arity 1 according to its child" in {
      val f = lib1.head
      val tree = compile("%s(load(//foo))".format(f.fqn))
      tree.provenance mustEqual StaticProvenance("/foo")
      tree.errors must beEmpty
    }

    "identify built-in reduce dispatch of arity 1 given incorrect number of parameters" in {
      val f = lib1.head
      val tree = compile("%s(load(//foo), load(//bar))".format(f.fqn))
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(IncorrectArity(1, 2))
    }

    "identify built-in non-reduce dispatch of arity 2 according to its children given unrelated sets" in {
      val f = lib2.head
      val tree = compile("%s(load(//foo), load(//bar))".format(f.fqn))
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }

    "identify built-in non-reduce dispatch of arity 2 according to its children given related sets" in {
      val f = lib2.head
      val tree = compile("""%s(load(//foo), "bar")""".format(f.fqn))
      tree.provenance mustEqual StaticProvenance("/foo")
      tree.errors must beEmpty
    }
    
    "identify load dispatch with static params according to its path" in {
      {
        val tree = compile("load(//foo)")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("load(//bar)")
        tree.provenance mustEqual StaticProvenance("/bar")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("load(//bar/baz)")
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
        val tree = compile("id('a) := 'a id(load(//foo))")
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
        val tree = compile("id('a) := 'a + 5 id(load(//foo))")
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
      val tree = compile("id('a) := 'a + load(//foo) id(24)")
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
        val tree = compile("fun('a, 'b) := 'a + 'b fun(load(//foo), 2)")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("fun('a, 'b) := 'a + 'b fun(1, load(//foo))")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("fun('a, 'b) := 'a + 'b fun(load(//foo), load(//foo))")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("fun('a, 'b) := 'a + 'b load(//foo) ~ load(//bar) fun(load(//foo), load(//bar))")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify dispatch to an unquantified value function as dynamic" in {
      {
        val tree = compile("histogram('a) := 'a + count(load(//foo) where load(//foo) = 'a) histogram")
        tree.provenance must beLike { case DynamicProvenance(_) => ok }
        tree.errors must beEmpty
      }
      
      {
        val input = """
          | histogram('a) :=
          |   foo := load(//foo)
          |   bar := load(//bar)
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
          |   foo := load(//foo)
          |   bar := load(//bar)
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
        |   foo := load(//foo)
        |   bar := load(//bar)
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
      val tree = compile("histogram('a) := 'a + count(load(//foo) where load(//foo) = 'a) histogram + histogram")   // if not consistent, binary op will fail
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty
    }
    
    "identify operation according to its children" in {
      {
        val tree = compile("1 where 2")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("load(//foo) where 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 where load(//foo)")
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
    
    "identify addition according to its children" in {
      {
        val tree = compile("1 + 2")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("load(//foo) + 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 + load(//foo)")
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
        val tree = compile("load(//foo) - 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 - load(//foo)")
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
        val tree = compile("load(//foo) * 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 * load(//foo)")
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
        val tree = compile("load(//foo) / 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 / load(//foo)")
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
        val tree = compile("load(//foo) < 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 < load(//foo)")
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
        val tree = compile("load(//foo) <= 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 <= load(//foo)")
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
        val tree = compile("load(//foo) > 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 > load(//foo)")
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
        val tree = compile("load(//foo) >= 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 >= load(//foo)")
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
        val tree = compile("load(//foo) = 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 = load(//foo)")
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
        val tree = compile("load(//foo) != 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 != load(//foo)")
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
        val tree = compile("load(//foo) & 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 & load(//foo)")
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
        val tree = compile("load(//foo) | 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 | load(//foo)")
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
        val tree = compile("!load(//foo)")
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
        val tree = compile("neg load(//foo)")
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
        val tree = compile("(load(//foo))")
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
      val tree = compile("{ a: load(//foo), b: load(//bar) }")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject object definition on static and dynamic provenances" in {
      val tree = compile("{ a: load(//foo), b: new 1 }")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject object definition on differing dynamic provenances" in {
      val tree = compile("{ a: new 1, b: new 1 }")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject array definition on different loads" in {
      val tree = compile("[ load(//foo), load(//bar) ]")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject array definition on static and dynamic provenances" in {
      val tree = compile("[ load(//foo), new 1 ]")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject array definition on differing dynamic provenances" in {
      val tree = compile("[ new 1, new 1 ]")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject deref on different loads" in {
      val tree = compile("load(//foo)[load(//bar)]")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject deref on static and dynamic provenances" in {
      val tree = compile("load(//foo)[new 1]")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject deref on differing dynamic provenances" in {
      val tree = compile("(new 1)[new 1]")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject dispatch on different loads" in {
      val tree = compile("fun('a, 'b) := 'a + 'b fun(load(//foo), load(//bar))")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject dispatch on static and dynamic provenances" in {
      val tree = compile("fun('a, 'b) := 'a + 'b fun(load(//foo), new 1)")
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
      val tree = compile("fun('a) := 'a + new 42 fun(load(//foo))")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(SetFunctionAppliedToSet)
    }
    
    "reject dispatch to load-modified identity function with dynamic provenance" in {
      val tree = compile("fun('a) := 'a + load(//foo) fun(new 24)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(SetFunctionAppliedToSet)
    }
    
    "reject dispatch to load-modified identity function with static provenance" in {
      val tree = compile("fun('a) := 'a + load(//foo) fun(load(//foo))")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(SetFunctionAppliedToSet)
    }
    
    "reject dispatch to load-modified identity function with union provenance" in {
      val input = """
        | foo := load(//foo)
        | bar := load(//bar)
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
      val tree = compile("fun('a) := 'a + load(//foo) fun")
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
        val tree = compile("a := load(//foo) a(1)")
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
        val tree = compile("a('b) := 'b + load(//foo) a(1, 2)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(IncorrectArity(1, 2))
      }
      
      {
        val tree = compile("a('b, 'c, 'd) := 'b + 'c + 'd a(1, 2, 3, 4, 5)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(IncorrectArity(3, 5))
      }
      
      {
        val tree = compile("a('b, 'c, 'd) := 'b + 'c + 'd + load(//foo) a(1, 2, 3, 4, 5)")
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
    }
    
    "reject dispatch to a set function with set parameters" in {
      {
        val tree = compile("a('b) := 'b + load(//foo) a(load(//foo))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(SetFunctionAppliedToSet)
      }
      
      {
        val tree = compile("a('b) := 'b + load(//foo) a(load(//bar))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(SetFunctionAppliedToSet)
      }
      
      {
        val tree = compile("a('b, 'c) := 'b + 'c + load(//foo) a(load(//foo), load(//foo))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(SetFunctionAppliedToSet)
      }
      
      {
        val tree = compile("a('b, 'c) := 'b + 'c + load(//foo) a(load(//bar), load(//bar))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(SetFunctionAppliedToSet)
      }
      
      {
        val tree = compile("a('b, 'c) := 'b + 'c + load(//foo) a(new 2, 42)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(SetFunctionAppliedToSet)
      }
      
      {
        val tree = compile("a('b) := 'b + new 1 a(load(//foo))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(SetFunctionAppliedToSet)
      }
      
      {
        val tree = compile("a('b) := 'b + new 1 a(load(//bar))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(SetFunctionAppliedToSet)
      }
      
      {
        val tree = compile("a('b, 'c) := 'b + 'c + new 1 a(load(//foo), load(//foo))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(SetFunctionAppliedToSet)
      }
      
      {
        val tree = compile("a('b, 'c) := 'b + 'c + new 1 a(load(//bar), load(//bar))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(SetFunctionAppliedToSet)
      }
      
      {
        val tree = compile("a('b, 'c) := 'b + 'c + new 1 a(new 2, 42)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(SetFunctionAppliedToSet)
      }
    }
    
    "reject operation on different loads" in {
      val tree = compile("load(//foo) where load(//bar)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject operation on static and dynamic provenances" in {
      val tree = compile("load(//foo) where new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject operation on differing dynamic provenances" in {
      val tree = compile("new 1 where new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject addition on different loads" in {
      val tree = compile("load(//foo) + load(//bar)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject addition on static and dynamic provenances" in {
      val tree = compile("load(//foo) + new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject addition on differing dynamic provenances" in {
      val tree = compile("(new 1) + (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject subtraction on different loads" in {
      val tree = compile("load(//foo) - load(//bar)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject subtraction on static and dynamic provenances" in {
      val tree = compile("load(//foo) - new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject subtraction on differing dynamic provenances" in {
      val tree = compile("(new 1) - (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject multiplication on different loads" in {
      val tree = compile("load(//foo) * load(//bar)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject multiplication on static and dynamic provenances" in {
      val tree = compile("load(//foo) * new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject multiplication on differing dynamic provenances" in {
      val tree = compile("(new 1) * (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject division on different loads" in {
      val tree = compile("load(//foo) / load(//bar)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject division on static and dynamic provenances" in {
      val tree = compile("load(//foo) / new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject division on differing dynamic provenances" in {
      val tree = compile("(new 1) / (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject less-than on different loads" in {
      val tree = compile("load(//foo) < load(//bar)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject less-than on static and dynamic provenances" in {
      val tree = compile("load(//foo) < new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject less-than on differing dynamic provenances" in {
      val tree = compile("(new 1) < (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject less-than-equal on different loads" in {
      val tree = compile("load(//foo) <= load(//bar)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject less-than-equal on static and dynamic provenances" in {
      val tree = compile("load(//foo) <= new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject less-than-equal on differing dynamic provenances" in {
      val tree = compile("(new 1) <= (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject greater-than on different loads" in {
      val tree = compile("load(//foo) > load(//bar)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject greater-than on static and dynamic provenances" in {
      val tree = compile("load(//foo) > new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject greater-than on differing dynamic provenances" in {
      val tree = compile("(new 1) > (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject greater-than-equal on different loads" in {
      val tree = compile("load(//foo) >= load(//bar)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject greater-than-equal on static and dynamic provenances" in {
      val tree = compile("load(//foo) >= new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject greater-than-equal on differing dynamic provenances" in {
      val tree = compile("(new 1) >= (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject equality on different loads" in {
      val tree = compile("load(//foo) = load(//bar)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject equality on static and dynamic provenances" in {
      val tree = compile("load(//foo) = new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject equality on differing dynamic provenances" in {
      val tree = compile("(new 1) = (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject not-equality on different loads" in {
      val tree = compile("load(//foo) != load(//bar)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject not-equality on static and dynamic provenances" in {
      val tree = compile("load(//foo) != new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject not-equality on differing dynamic provenances" in {
      val tree = compile("(new 1) != (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject boolean and on different loads" in {
      val tree = compile("load(//foo) & load(//bar)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject boolean and on static and dynamic provenances" in {
      val tree = compile("load(//foo) & new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject boolean and on differing dynamic provenances" in {
      val tree = compile("(new 1) & (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject boolean or on different loads" in {
      val tree = compile("load(//foo) | load(//bar)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject boolean or on static and dynamic provenances" in {
      val tree = compile("load(//foo) | new 1")
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
        val tree = compile("load(//a) ~ load(//a) 42")
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
      val tree = compile("a := load(//a) b := load(//b) a ~ b a ~ b 42")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(AlreadyRelatedSets)
    }
    
    "accept object definition on different loads when related" in {
      val tree = compile("load(//foo) ~ load(//bar) { a: load(//foo), b: load(//bar) }")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept object definition on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 load(//foo) ~ s { a: load(//foo), b: s }")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept object definition on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 ~ s2 { a: s1, b: s2 }")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept array definition on different loads when related" in {
      val tree = compile("load(//foo) ~ load(//bar) [ load(//foo), load(//bar) ]")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept array definition on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 load(//foo) ~ s [ load(//foo), s ]")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept array definition on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 ~ s2 [ s1, s2 ]")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept deref on different loads when related" in {
      val tree = compile("load(//foo) ~ load(//bar) load(//foo)[load(//bar)]")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept deref on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 load(//foo) ~ s load(//foo)[s]")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept deref on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 ~ s2 s1[s2]")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept dispatch on different loads when related" in {
      val tree = compile("load(//foo) ~ load(//bar) fun('a, 'b) := 'a + 'b fun(load(//foo), load(//bar))")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty
    }
    
    "accept dispatch on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 load(//foo) ~ s fun('a, 'b) := 'a + 'b fun(load(//foo), s)")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty
    }
    
    "accept dispatch on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 ~ s2 fun('a, 'b) := 'a + 'b fun(s1, s2)")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty
    }
    
    "accept operation on different loads when related" in {
      val tree = compile("load(//foo) ~ load(//bar) load(//foo) where load(//bar)")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept operation on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 load(//foo) ~ s load(//foo) where s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept operation on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 ~ s2 s1 where s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept addition on different loads when related" in {
      val tree = compile("load(//foo) ~ load(//bar) load(//foo) + load(//bar)")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept addition on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 load(//foo) ~ s load(//foo) + s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept addition on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 ~ s2 s1 + s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept subtraction on different loads when related" in {
      val tree = compile("load(//foo) ~ load(//bar) load(//foo) - load(//bar)")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept subtraction on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 load(//foo) ~ s load(//foo) - s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept subtraction on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 ~ s2 s1 - s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept multiplication on different loads when related" in {
      val tree = compile("load(//foo) ~ load(//bar) load(//foo) * load(//bar)")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept multiplication on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 load(//foo) ~ s load(//foo) * s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept multiplication on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 ~ s2 s1 * s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept division on different loads when related" in {
      val tree = compile("load(//foo) ~ load(//bar) load(//foo) / load(//bar)")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept division on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 load(//foo) ~ s load(//foo) / s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept division on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 ~ s2 s1 / s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept less-than on different loads when related" in {
      val tree = compile("load(//foo) ~ load(//bar) load(//foo) < load(//bar)")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept less-than on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 load(//foo) ~ s load(//foo) < s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept less-than on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 ~ s2 s1 < s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept less-than-equal on different loads when related" in {
      val tree = compile("load(//foo) ~ load(//bar) load(//foo) <= load(//bar)")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept less-than-equal on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 load(//foo) ~ s load(//foo) <= s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept less-than-equal on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 ~ s2 s1 <= s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept greater-than on different loads when related" in {
      val tree = compile("load(//foo) ~ load(//bar) load(//foo) > load(//bar)")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept greater-than on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 load(//foo) ~ s load(//foo) > s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept greater-than on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 ~ s2 s1 > s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept greater-than-equal on different loads when related" in {
      val tree = compile("load(//foo) ~ load(//bar) load(//foo) >= load(//bar)")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept greater-than-equal on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 load(//foo) ~ s load(//foo) >= s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept greater-than-equal on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 ~ s2 s1 >= s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept equality on different loads when related" in {
      val tree = compile("load(//foo) ~ load(//bar) load(//foo) = load(//bar)")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept equality on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 load(//foo) ~ s load(//foo) = s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept equality on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 ~ s2 s1 = s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept not-equality on different loads when related" in {
      val tree = compile("load(//foo) ~ load(//bar) load(//foo) != load(//bar)")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept not-equality on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 load(//foo) ~ s load(//foo) != s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept not-equality on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 ~ s2 s1 != s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept boolean and on different loads when related" in {
      val tree = compile("load(//foo) ~ load(//bar) load(//foo) & load(//bar)")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept boolean and on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 load(//foo) ~ s load(//foo) & s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept boolean and on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 ~ s2 s1 & s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept boolean or on different loads when related" in {
      val tree = compile("load(//foo) ~ load(//bar) load(//foo) | load(//bar)")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept boolean or on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 load(//foo) ~ s load(//foo) | s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept boolean or on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 ~ s2 s1 | s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "reject addition with unrelated relation" in {
      val tree = compile("load(//a) ~ load(//b) load(//c) + load(//d)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "accept operations according to the commutative relation" in {
      {
        val input = """
          | foo := load(//foo)
          | bar := load(//bar)
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
          | foo := load(//foo)
          | bar := load(//bar)
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
        | foo := load(//foo)
        | bar := load(//bar)
        | baz := load(//baz)
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
        | foo := load(//foo)
        | bar := load(//bar)
        | baz := load(//baz)
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
        | foo := load(//foo)
        | bar := load(//bar)
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
        | foo := load(//foo)
        | bar := load(//bar)
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
        | foo := load(//foo)
        | bar := load(//bar)
        | baz := load(//baz)
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
        val tree = compile("a := load(//foo) + load(//b) a")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compile("a := load(//foo) a + load(//bar)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "not propagate through new" in {
      val tree = compile("new (load(//a) + load(//b))")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "propagate through relate" in {
      {
        val tree = compile("(load(//a) + load(//b)) ~ load(//c) 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compile("load(//c) ~ (load(//a) + load(//b)) 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through object definition" in {
      val tree = compile("{ a: load(//a) + load(//b), b: 42 }")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "propagate through array definition" in {
      val tree = compile("[load(//a) + load(//b), 42]")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "propagate through descent" in {
      val tree = compile("(load(//a) + load(//b)).foo")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "propagate through dereference" in {
      {
        val tree = compile("(load(//a) + load(//b))[42]")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compile("42[load(//a) + load(//b)]")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through dispatch" in {
      val tree = compile("a('b) := 'b a(load(//a) + load(//b))")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "propagate through operation" in {
      {
        val tree = compile("(load(//a) + load(//b)) where 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compile("42 + (load(//a) where load(//b))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through addition" in {
      {
        val tree = compile("(load(//a) + load(//b)) + 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compile("42 + (load(//a) + load(//b))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through subtraction" in {
      {
        val tree = compile("(load(//a) + load(//b)) - 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compile("42 - (load(//a) + load(//b))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through multiplication" in {
      {
        val tree = compile("(load(//a) + load(//b)) * 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compile("42 * (load(//a) + load(//b))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through division" in {
      {
        val tree = compile("(load(//a) + load(//b)) / 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compile("42 / (load(//a) + load(//b))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through less-than" in {
      {
        val tree = compile("(load(//a) + load(//b)) < 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compile("42 < (load(//a) + load(//b))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through less-than-equal" in {
      {
        val tree = compile("(load(//a) + load(//b)) <= 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compile("42 <= (load(//a) + load(//b))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through greater-than" in {
      {
        val tree = compile("(load(//a) + load(//b)) > 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compile("42 > (load(//a) + load(//b))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through greater-than-equal" in {
      {
        val tree = compile("(load(//a) + load(//b)) >= 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compile("42 >= (load(//a) + load(//b))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through equality" in {
      {
        val tree = compile("(load(//a) + load(//b)) = 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compile("42 = (load(//a) + load(//b))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through not-equality" in {
      {
        val tree = compile("(load(//a) + load(//b)) != 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compile("42 != (load(//a) + load(//b))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through boolean and" in {
      {
        val tree = compile("(load(//a) + load(//b)) & 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compile("42 & (load(//a) + load(//b))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through boolean or" in {
      {
        val tree = compile("(load(//a) + load(//b)) | 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compile("42 | (load(//a) + load(//b))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through complementation" in {
      val tree = compile("!(load(//a) + load(//b))")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "propagate through negation" in {
      val tree = compile("neg (load(//a) + load(//b))")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "propagate through parenthetical" in {
      val tree = compile("(load(//a) + load(//b))")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
  }
  
  "constraining expression determination" should {
    "leave values unconstrained" in {
      compile("42").constrainingExpr must beNone
    }
    
    "leave loads unconstrained when outside a relation" in {
      compile("load(//foo)").constrainingExpr must beNone
    }
    
    "constrain loads within a relation" in {
      {
        val Relate(_, from, _, in) = compile("load(//foo) ~ load(//bar) load(//foo)")
        in.constrainingExpr must beSome(from)
      }
      
      {
        val Relate(_, _, to, in) = compile("load(//foo) ~ load(//bar) load(//bar)")
        in.constrainingExpr must beSome(to)
      }
    }
    
    "leave unconnected loads unconstrained within a relation" in {
      val Relate(_, from, _, in) = compile("load(//foo) ~ load(//bar) load(//baz)")
      in.constrainingExpr must beNone
    }
    
    "propagate constraints through a nested relation" in {
      {
        val Relate(_, from1, to1, Relate(_, from2, to2, in)) = compile("""
          | load(//foo) ~ load(//bar)
          |   load(//foo) ~ load(//baz)
          |     load(//foo)""".stripMargin)
        
        in.constrainingExpr must beSome(from2)
      }
      
      {
        val Relate(_, from1, to1, Relate(_, from2, to2, in)) = compile("""
          | load(//foo) ~ load(//bar)
          |   load(//foo) ~ load(//baz)
          |     load(//bar)""".stripMargin)
        
        in.constrainingExpr must beSome(to1)
      }
      
      {
        val Relate(_, from1, to1, Relate(_, from2, to2, in)) = compile("""
          | load(//foo) ~ load(//bar)
          |   load(//foo) ~ load(//baz)
          |     load(//baz)""".stripMargin)
        
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


