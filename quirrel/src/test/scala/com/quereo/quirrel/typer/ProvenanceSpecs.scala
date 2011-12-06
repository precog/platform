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
package com.quereo.quirrel
package typer

import edu.uwm.cs.gll.LineStream
import org.specs2.mutable.Specification

object ProvenanceSpecs extends Specification
    with StubPhases
    with Compiler
    with ProvenanceChecker
    with CriticalConditionFinder {
  
  "provenance computation" should {
    "identify let according to its right expression" in {   // using raw, no-op let
      {
        val tree = parse("a := 1 1")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("a := 1 dataset(//foo)")
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
    
    "identify new as dynamic" in {
      val tree = compile("new 1")
      tree.provenance must beLike {
        case DynamicProvenance(_) => ok
      }
      tree.errors must beEmpty
    }
    
    "identify relate according to its last expression" in {
      {
        val tree = compile("dataset(//a) :: dataset(//b) 3")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("dataset(//a) :: dataset(//b) dataset(//foo)")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("dataset(//a) :: dataset(//b) (new 1)")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify tic-var as value" in {
      val tree = parse("'foo")    // uses raw tic-var
      tree.provenance mustEqual ValueProvenance
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
        val tree = compile("{ a: 1, b: 2, c: dataset(//foo) }")
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
        val tree = compile("[1, 2, dataset(//foo)]")
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
        val tree = compile("dataset(//bar).foo")
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
        val tree = compile("dataset(//foo)[2]")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1[dataset(//foo)]")
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
        val tree = compile("count(dataset(//foo))")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("max(dataset(//foo))")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("mean(dataset(//foo))")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("median(dataset(//foo))")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("min(dataset(//foo))")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("mode(dataset(//foo))")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("stdDev(dataset(//foo))")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("sum(dataset(//foo))")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
    }
    
    "identify dataset dispatch with static params according to its path" in {
      {
        val tree = compile("dataset(//foo)")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("dataset(//bar)")
        tree.provenance mustEqual StaticProvenance("/bar")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("dataset(//bar/baz)")
        tree.provenance mustEqual StaticProvenance("/bar/baz")
        tree.errors must beEmpty
      }
    }
    
    "identify dataset dispatch with non-static params as dynamic" in {
      {
        val tree = compile("dataset(42)")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("a := 42 dataset(a)")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("dataset(count(42))")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("dataset(new 42)")
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
        val tree = compile("id('a) := 'a id(dataset(//foo))")
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
        val tree = compile("id('a) := 'a + 5 id(dataset(//foo))")
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
    
    "identify dispatch to dataset-modified identity function as static" in {
      val tree = compile("id('a) := 'a + dataset(//foo) id(24)")
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
        val tree = compile("fun('a, 'b) := 'a + 'b fun(dataset(//foo), 2)")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("fun('a, 'b) := 'a + 'b fun(1, dataset(//foo))")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("fun('a, 'b) := 'a + 'b fun(dataset(//foo), dataset(//foo))")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("fun('a, 'b) := 'a + 'b dataset(//foo) :: dataset(//bar) fun(dataset(//foo), dataset(//bar))")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify dispatch to an unquantified value function by the values it takes on" in {
      {
        val tree = compile("histogram('a) := 'a + count(dataset(//foo) where dataset(//foo) = 'a) histogram")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val input = """
          | histogram('a) :=
          |   foo := dataset(//foo)
          |   bar := dataset(//bar)
          |   
          |   'a + count(foo :: bar foo where foo = 'a & bar = 12)
          | 
          | histogram""".stripMargin
        
        val tree = compile(input)
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val input = """
          | histogram('a) :=
          |   foo := dataset(//foo)
          |   bar := dataset(//bar)
          |   
          |   foo' := foo where foo = 'a
          |   bar' := bar where bar = 'a
          | 
          |   'a + count(foo' :: bar' foo + bar)
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
        |   foo := dataset(//foo)
        |   bar := dataset(//bar)
        |
        |   foo' := foo where foo = 'a
        |   bar' := bar where bar = 'a
        |
        |   foo' :: bar'
        |     foo.left + bar.right
        |
        | fun""".stripMargin
        
      val tree = compile(input)
      tree.provenance must beLike {
        case DynamicProvenance(_) => ok
      }
      tree.errors must beEmpty
    }
    
    "identify operation according to its children" in {
      {
        val tree = compile("1 where 2")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("dataset(//foo) where 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 where dataset(//foo)")
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
        val tree = compile("dataset(//foo) + 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 + dataset(//foo)")
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
        val tree = compile("dataset(//foo) - 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 - dataset(//foo)")
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
        val tree = compile("dataset(//foo) * 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 * dataset(//foo)")
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
        val tree = compile("dataset(//foo) / 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 / dataset(//foo)")
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
        val tree = compile("dataset(//foo) < 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 < dataset(//foo)")
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
        val tree = compile("dataset(//foo) <= 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 <= dataset(//foo)")
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
        val tree = compile("dataset(//foo) > 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 > dataset(//foo)")
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
        val tree = compile("dataset(//foo) >= 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 >= dataset(//foo)")
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
        val tree = compile("dataset(//foo) = 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 = dataset(//foo)")
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
        val tree = compile("dataset(//foo) != 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 != dataset(//foo)")
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
        val tree = compile("dataset(//foo) & 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 & dataset(//foo)")
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
        val tree = compile("dataset(//foo) | 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("1 | dataset(//foo)")
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
        val tree = compile("!dataset(//foo)")
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
        val tree = compile("~1")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("~dataset(//foo)")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = compile("~(new 1)")
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
        val tree = compile("(dataset(//foo))")
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
    "reject object definition on different datasets" in {
      val tree = compile("{ a: dataset(//foo), b: dataset(//bar) }")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject object definition on static and dynamic provenances" in {
      val tree = compile("{ a: dataset(//foo), b: new 1 }")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject object definition on differing dynamic provenances" in {
      val tree = compile("{ a: new 1, b: new 1 }")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject array definition on different datasets" in {
      val tree = compile("[ dataset(//foo), dataset(//bar) ]")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject array definition on static and dynamic provenances" in {
      val tree = compile("[ dataset(//foo), new 1 ]")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject array definition on differing dynamic provenances" in {
      val tree = compile("[ new 1, new 1 ]")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject deref on different datasets" in {
      val tree = compile("dataset(//foo)[dataset(//bar)]")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject deref on static and dynamic provenances" in {
      val tree = compile("dataset(//foo)[new 1]")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject deref on differing dynamic provenances" in {
      val tree = compile("(new 1)[new 1]")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject dispatch on different datasets" in {
      val tree = compile("fun('a, 'b) := 'a + 'b fun(dataset(//foo), dataset(//bar))")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject dispatch on static and dynamic provenances" in {
      val tree = compile("fun('a, 'b) := 'a + 'b fun(dataset(//foo), new 1)")
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
      val tree = compile("fun('a) := 'a + new 42 fun(dataset(//foo))")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(SetFunctionAppliedToSet)
    }
    
    "reject dispatch to dataset-modified identity function with dynamic provenance" in {
      val tree = compile("fun('a) := 'a + dataset(//foo) fun(new 24)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(SetFunctionAppliedToSet)
    }
    
    "reject dispatch to dataset-modified identity function with static provenance" in {
      val tree = compile("fun('a) := 'a + dataset(//foo) fun(dataset(//foo))")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(SetFunctionAppliedToSet)
    }
    
    "reject dispatch to where-less value function with too few parameters" in {
      val tree = compile("fun('a) := 'a + 5 fun")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(UnspecifiedRequiredParams(Vector("'a")))
    }
    
    "reject dispatch to where-less static function with too few parameters" in {
      val tree = compile("fun('a) := 'a + dataset(//foo) fun")
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
        val tree = compile("a := dataset(//foo) a(1)")
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
        val tree = compile("a('b) := 'b + dataset(//foo) a(1, 2)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(IncorrectArity(1, 2))
      }
      
      {
        val tree = compile("a('b, 'c, 'd) := 'b + 'c + 'd a(1, 2, 3, 4, 5)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(IncorrectArity(3, 5))
      }
      
      {
        val tree = compile("a('b, 'c, 'd) := 'b + 'c + 'd + dataset(//foo) a(1, 2, 3, 4, 5)")
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
      
      "dataset" >> {
        {
          val tree = compile("dataset")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(IncorrectArity(1, 0))
        }
        
        {
          val tree = compile("dataset(1, 2)")
          tree.provenance mustEqual NullProvenance
          tree.errors mustEqual Set(IncorrectArity(1, 2))
        }
        
        {
          val tree = compile("dataset(1, 2, 3)")
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
        val tree = compile("a('b) := 'b + dataset(//foo) a(dataset(//foo))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(SetFunctionAppliedToSet)
      }
      
      {
        val tree = compile("a('b) := 'b + dataset(//foo) a(dataset(//bar))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(SetFunctionAppliedToSet)
      }
      
      {
        val tree = compile("a('b, 'c) := 'b + 'c + dataset(//foo) a(dataset(//foo), dataset(//foo))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(SetFunctionAppliedToSet)
      }
      
      {
        val tree = compile("a('b, 'c) := 'b + 'c + dataset(//foo) a(dataset(//bar), dataset(//bar))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(SetFunctionAppliedToSet)
      }
      
      {
        val tree = compile("a('b, 'c) := 'b + 'c + dataset(//foo) a(new 2, 42)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(SetFunctionAppliedToSet)
      }
      
      {
        val tree = compile("a('b) := 'b + new 1 a(dataset(//foo))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(SetFunctionAppliedToSet)
      }
      
      {
        val tree = compile("a('b) := 'b + new 1 a(dataset(//bar))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(SetFunctionAppliedToSet)
      }
      
      {
        val tree = compile("a('b, 'c) := 'b + 'c + new 1 a(dataset(//foo), dataset(//foo))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(SetFunctionAppliedToSet)
      }
      
      {
        val tree = compile("a('b, 'c) := 'b + 'c + new 1 a(dataset(//bar), dataset(//bar))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(SetFunctionAppliedToSet)
      }
      
      {
        val tree = compile("a('b, 'c) := 'b + 'c + new 1 a(new 2, 42)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(SetFunctionAppliedToSet)
      }
    }
    
    "reject operation on different datasets" in {
      val tree = compile("dataset(//foo) where dataset(//bar)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject operation on static and dynamic provenances" in {
      val tree = compile("dataset(//foo) where new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject operation on differing dynamic provenances" in {
      val tree = compile("new 1 where new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject addition on different datasets" in {
      val tree = compile("dataset(//foo) + dataset(//bar)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject addition on static and dynamic provenances" in {
      val tree = compile("dataset(//foo) + new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject addition on differing dynamic provenances" in {
      val tree = compile("(new 1) + (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject subtraction on different datasets" in {
      val tree = compile("dataset(//foo) - dataset(//bar)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject subtraction on static and dynamic provenances" in {
      val tree = compile("dataset(//foo) - new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject subtraction on differing dynamic provenances" in {
      val tree = compile("(new 1) - (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject multiplication on different datasets" in {
      val tree = compile("dataset(//foo) * dataset(//bar)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject multiplication on static and dynamic provenances" in {
      val tree = compile("dataset(//foo) * new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject multiplication on differing dynamic provenances" in {
      val tree = compile("(new 1) * (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject division on different datasets" in {
      val tree = compile("dataset(//foo) / dataset(//bar)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject division on static and dynamic provenances" in {
      val tree = compile("dataset(//foo) / new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject division on differing dynamic provenances" in {
      val tree = compile("(new 1) / (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject less-than on different datasets" in {
      val tree = compile("dataset(//foo) < dataset(//bar)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject less-than on static and dynamic provenances" in {
      val tree = compile("dataset(//foo) < new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject less-than on differing dynamic provenances" in {
      val tree = compile("(new 1) < (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject less-than-equal on different datasets" in {
      val tree = compile("dataset(//foo) <= dataset(//bar)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject less-than-equal on static and dynamic provenances" in {
      val tree = compile("dataset(//foo) <= new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject less-than-equal on differing dynamic provenances" in {
      val tree = compile("(new 1) <= (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject greater-than on different datasets" in {
      val tree = compile("dataset(//foo) > dataset(//bar)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject greater-than on static and dynamic provenances" in {
      val tree = compile("dataset(//foo) > new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject greater-than on differing dynamic provenances" in {
      val tree = compile("(new 1) > (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject greater-than-equal on different datasets" in {
      val tree = compile("dataset(//foo) >= dataset(//bar)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject greater-than-equal on static and dynamic provenances" in {
      val tree = compile("dataset(//foo) >= new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject greater-than-equal on differing dynamic provenances" in {
      val tree = compile("(new 1) >= (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject equality on different datasets" in {
      val tree = compile("dataset(//foo) = dataset(//bar)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject equality on static and dynamic provenances" in {
      val tree = compile("dataset(//foo) = new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject equality on differing dynamic provenances" in {
      val tree = compile("(new 1) = (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject not-equality on different datasets" in {
      val tree = compile("dataset(//foo) != dataset(//bar)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject not-equality on static and dynamic provenances" in {
      val tree = compile("dataset(//foo) != new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject not-equality on differing dynamic provenances" in {
      val tree = compile("(new 1) != (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject boolean and on different datasets" in {
      val tree = compile("dataset(//foo) & dataset(//bar)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject boolean and on static and dynamic provenances" in {
      val tree = compile("dataset(//foo) & new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject boolean and on differing dynamic provenances" in {
      val tree = compile("(new 1) & (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject boolean or on different datasets" in {
      val tree = compile("dataset(//foo) | dataset(//bar)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject boolean or on static and dynamic provenances" in {
      val tree = compile("dataset(//foo) | new 1")
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
        val tree = compile("dataset(//a) :: dataset(//a) 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(AlreadyRelatedSets)
      }
      
      {
        val tree = compile("1 :: 2 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(AlreadyRelatedSets)
      }
      
      {
        val tree = compile("a := new 1 a :: a 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(AlreadyRelatedSets)
      }
    }
    
    "fail on explicitly related sets" in {
      val tree = compile("a := dataset(//a) b := dataset(//b) a :: b a :: b 42")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(AlreadyRelatedSets)
    }
    
    "accept object definition on different datasets when related" in {
      val tree = compile("dataset(//foo) :: dataset(//bar) { a: dataset(//foo), b: dataset(//bar) }")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept object definition on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 dataset(//foo) :: s { a: dataset(//foo), b: s }")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept object definition on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 :: s2 { a: s1, b: s2 }")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept array definition on different datasets when related" in {
      val tree = compile("dataset(//foo) :: dataset(//bar) [ dataset(//foo), dataset(//bar) ]")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept array definition on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 dataset(//foo) :: s [ dataset(//foo), s ]")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept array definition on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 :: s2 [ s1, s2 ]")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept deref on different datasets when related" in {
      val tree = compile("dataset(//foo) :: dataset(//bar) dataset(//foo)[dataset(//bar)]")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept deref on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 dataset(//foo) :: s dataset(//foo)[s]")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept deref on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 :: s2 s1[s2]")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept dispatch on different datasets when related" in {
      val tree = compile("dataset(//foo) :: dataset(//bar) fun('a, 'b) := 'a + 'b fun(dataset(//foo), dataset(//bar))")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty
    }
    
    "accept dispatch on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 dataset(//foo) :: s fun('a, 'b) := 'a + 'b fun(dataset(//foo), s)")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty
    }
    
    "accept dispatch on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 :: s2 fun('a, 'b) := 'a + 'b fun(s1, s2)")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty
    }
    
    "accept operation on different datasets when related" in {
      val tree = compile("dataset(//foo) :: dataset(//bar) dataset(//foo) where dataset(//bar)")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept operation on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 dataset(//foo) :: s dataset(//foo) where s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept operation on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 :: s2 s1 where s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept addition on different datasets when related" in {
      val tree = compile("dataset(//foo) :: dataset(//bar) dataset(//foo) + dataset(//bar)")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept addition on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 dataset(//foo) :: s dataset(//foo) + s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept addition on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 :: s2 s1 + s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept subtraction on different datasets when related" in {
      val tree = compile("dataset(//foo) :: dataset(//bar) dataset(//foo) - dataset(//bar)")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept subtraction on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 dataset(//foo) :: s dataset(//foo) - s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept subtraction on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 :: s2 s1 - s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept multiplication on different datasets when related" in {
      val tree = compile("dataset(//foo) :: dataset(//bar) dataset(//foo) * dataset(//bar)")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept multiplication on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 dataset(//foo) :: s dataset(//foo) * s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept multiplication on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 :: s2 s1 * s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept division on different datasets when related" in {
      val tree = compile("dataset(//foo) :: dataset(//bar) dataset(//foo) / dataset(//bar)")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept division on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 dataset(//foo) :: s dataset(//foo) / s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept division on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 :: s2 s1 / s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept less-than on different datasets when related" in {
      val tree = compile("dataset(//foo) :: dataset(//bar) dataset(//foo) < dataset(//bar)")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept less-than on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 dataset(//foo) :: s dataset(//foo) < s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept less-than on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 :: s2 s1 < s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept less-than-equal on different datasets when related" in {
      val tree = compile("dataset(//foo) :: dataset(//bar) dataset(//foo) <= dataset(//bar)")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept less-than-equal on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 dataset(//foo) :: s dataset(//foo) <= s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept less-than-equal on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 :: s2 s1 <= s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept greater-than on different datasets when related" in {
      val tree = compile("dataset(//foo) :: dataset(//bar) dataset(//foo) > dataset(//bar)")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept greater-than on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 dataset(//foo) :: s dataset(//foo) > s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept greater-than on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 :: s2 s1 > s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept greater-than-equal on different datasets when related" in {
      val tree = compile("dataset(//foo) :: dataset(//bar) dataset(//foo) >= dataset(//bar)")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept greater-than-equal on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 dataset(//foo) :: s dataset(//foo) >= s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept greater-than-equal on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 :: s2 s1 >= s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept equality on different datasets when related" in {
      val tree = compile("dataset(//foo) :: dataset(//bar) dataset(//foo) = dataset(//bar)")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept equality on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 dataset(//foo) :: s dataset(//foo) = s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept equality on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 :: s2 s1 = s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept not-equality on different datasets when related" in {
      val tree = compile("dataset(//foo) :: dataset(//bar) dataset(//foo) != dataset(//bar)")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept not-equality on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 dataset(//foo) :: s dataset(//foo) != s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept not-equality on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 :: s2 s1 != s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept boolean and on different datasets when related" in {
      val tree = compile("dataset(//foo) :: dataset(//bar) dataset(//foo) & dataset(//bar)")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept boolean and on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 dataset(//foo) :: s dataset(//foo) & s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept boolean and on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 :: s2 s1 & s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept boolean or on different datasets when related" in {
      val tree = compile("dataset(//foo) :: dataset(//bar) dataset(//foo) | dataset(//bar)")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept boolean or on static and dynamic provenances when related" in {
      val tree = compile("s := new 1 dataset(//foo) :: s dataset(//foo) | s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept boolean or on differing dynamic provenances when related" in {
      val tree = compile("s1 := new 1 s2 := new 1 s1 :: s2 s1 | s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "reject addition with unrelated relation" in {
      val tree = compile("dataset(//a) :: dataset(//b) dataset(//c) + dataset(//d)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "accept operations according to the commutative relation" in {
      {
        val input = """
          | foo := dataset(//foo)
          | bar := dataset(//bar)
          | 
          | foo :: bar
          |   foo + bar""".stripMargin
          
        val tree = compile(input)
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val input = """
          | foo := dataset(//foo)
          | bar := dataset(//bar)
          | 
          | foo :: bar
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
        | foo := dataset(//foo)
        | bar := dataset(//bar)
        | baz := dataset(//baz)
        | 
        | foo :: bar
        |   bar :: baz
        |     foo + baz""".stripMargin
        
      val tree = compile(input)
      tree.provenance must beLike {
        case DynamicProvenance(_) => ok
      }
      tree.errors must beEmpty
    }
    
    "accept operations according to the commutative-transitive relation" in {
      val input = """
        | foo := dataset(//foo)
        | bar := dataset(//bar)
        | baz := dataset(//baz)
        | 
        | foo :: bar
        |   bar :: baz
        |     baz + foo""".stripMargin
        
      val tree = compile(input)
      tree.provenance must beLike {
        case DynamicProvenance(_) => ok
      }
      tree.errors must beEmpty
    }
    
    "accept multiple nested expressions in relation" in {
      val input = """
        | foo := dataset(//foo)
        | bar := dataset(//bar)
        | 
        | foo :: bar
        |   foo + bar + foo""".stripMargin
        
      val tree = compile(input)
      tree.provenance must beLike {
        case DynamicProvenance(_) => ok
      }
      tree.errors must beEmpty
    }
  }
  
  "null provenance" should {
    "synthesize when name binding is null in dispatch" in {
      compile("fubar").provenance mustEqual NullProvenance
    }
    
    "propagate through let" in {
      {
        val tree = compile("a := dataset(//foo) + dataset(//b) a")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compile("a := dataset(//foo) a + dataset(//bar)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "not propagate through new" in {
      val tree = compile("new (dataset(//a) + dataset(//b))")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "propagate through relate" in {
      {
        val tree = compile("(dataset(//a) + dataset(//b)) :: dataset(//c) 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compile("dataset(//c) :: (dataset(//a) + dataset(//b)) 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through object definition" in {
      val tree = compile("{ a: dataset(//a) + dataset(//b), b: 42 }")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "propagate through array definition" in {
      val tree = compile("[dataset(//a) + dataset(//b), 42]")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "propagate through descent" in {
      val tree = compile("(dataset(//a) + dataset(//b)).foo")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "propagate through dereference" in {
      {
        val tree = compile("(dataset(//a) + dataset(//b))[42]")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compile("42[dataset(//a) + dataset(//b)]")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through dispatch" in {
      val tree = compile("a('b) := 'b a(dataset(//a) + dataset(//b))")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "propagate through operation" in {
      {
        val tree = compile("(dataset(//a) + dataset(//b)) where 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compile("42 + (dataset(//a) where dataset(//b))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through addition" in {
      {
        val tree = compile("(dataset(//a) + dataset(//b)) + 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compile("42 + (dataset(//a) + dataset(//b))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through subtraction" in {
      {
        val tree = compile("(dataset(//a) + dataset(//b)) - 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compile("42 - (dataset(//a) + dataset(//b))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through multiplication" in {
      {
        val tree = compile("(dataset(//a) + dataset(//b)) * 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compile("42 * (dataset(//a) + dataset(//b))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through division" in {
      {
        val tree = compile("(dataset(//a) + dataset(//b)) / 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compile("42 / (dataset(//a) + dataset(//b))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through less-than" in {
      {
        val tree = compile("(dataset(//a) + dataset(//b)) < 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compile("42 < (dataset(//a) + dataset(//b))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through less-than-equal" in {
      {
        val tree = compile("(dataset(//a) + dataset(//b)) <= 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compile("42 <= (dataset(//a) + dataset(//b))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through greater-than" in {
      {
        val tree = compile("(dataset(//a) + dataset(//b)) > 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compile("42 > (dataset(//a) + dataset(//b))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through greater-than-equal" in {
      {
        val tree = compile("(dataset(//a) + dataset(//b)) >= 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compile("42 >= (dataset(//a) + dataset(//b))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through equality" in {
      {
        val tree = compile("(dataset(//a) + dataset(//b)) = 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compile("42 = (dataset(//a) + dataset(//b))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through not-equality" in {
      {
        val tree = compile("(dataset(//a) + dataset(//b)) != 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compile("42 != (dataset(//a) + dataset(//b))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through boolean and" in {
      {
        val tree = compile("(dataset(//a) + dataset(//b)) & 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compile("42 & (dataset(//a) + dataset(//b))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through boolean or" in {
      {
        val tree = compile("(dataset(//a) + dataset(//b)) | 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compile("42 | (dataset(//a) + dataset(//b))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through complementation" in {
      val tree = compile("!(dataset(//a) + dataset(//b))")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "propagate through negation" in {
      val tree = compile("~(dataset(//a) + dataset(//b))")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "propagate through parenthetical" in {
      val tree = compile("(dataset(//a) + dataset(//b))")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
  }
  
  "specification examples" >> {
    "deviant-durations.qrl" >> {
      val input = """
        | interactions := dataset(//interactions)
        | 
        | big1z('userId) :=
        |   userInteractions := interactions where interactions.userId = 'userId
        |   
        |   m := mean(userInteractions.duration)
        |   sd := stdDev(userInteractions.duration)
        | 
        |   {
        |     userId: 'userId,
        |     interaction: userInteractions where userInteractions.duration > m + (sd * 3)
        |   }
        |   
        | big1z
        """.stripMargin
      
      val tree = compile(input)
      tree.provenance mustNotEqual NullProvenance
      tree.errors must beEmpty
    }
    
    "first-conversion.qrl" >> {
      val input = """
        | firstConversionAfterEachImpression('userId) :=
        |   conversions' := dataset(//conversions)
        |   impressions' := dataset(//impressions)
        | 
        |   conversions := conversions' where conversions'.userId = 'userId
        |   impressions := impressions' where impressions'.userId = 'userId
        | 
        |   greaterConversions('time) :=
        |     impressionTimes := impressions where impressions.time = 'time
        |     conversionTimes :=
        |       conversions where conversions.time = min(conversions where conversions.time > 'time).time
        |     
        |     conversionTimes :: impressionTimes
        |       { impression: impressions, nextConversion: conversions }
        | 
        |   greaterConversions
        | 
        | firstConversionAfterEachImpression
        """.stripMargin
      
      val tree = compile(input)
      tree.provenance mustNotEqual NullProvenance
      tree.errors must beEmpty
    }
    
    "histogram.qrl" >> {
      val input = """
        | clicks := dataset(//clicks)
        | 
        | histogram('value) :=
        |   { cnt: count(clicks where clicks = 'value), value: 'value }
        |   
        | histogram
        """.stripMargin
      
      val tree = compile(input)
      tree.provenance mustNotEqual NullProvenance
      tree.errors must beEmpty
    }
    
    "interaction-totals.qrl" >> {
      val input = """
        | interactions := dataset(//interactions)
        | 
        | hourOfDay('time) := 'time / 3600000           -- timezones, anyone?
        | dayOfWeek('time) := 'time / 604800000         -- not even slightly correct
        | 
        | total('hour, 'day) :=
        |   dayAndHour := dayOfWeek(interactions.time) = 'day & hourOfDay(interactions.time) = 'hour
        |   sum(interactions where dayAndHour)
        |   
        | total
        """.stripMargin
      
      val tree = compile(input)
      tree.provenance mustNotEqual NullProvenance
      tree.errors must beEmpty
    }
    
    "relative-durations.qrl" >> {
      val input = """
        | interactions := dataset(//interactions)
        | 
        | relativeDurations('userId, 'value) :=
        |   userInteractions := interactions where interactions.userId = 'userId
        |   interactionDurations := (userInteractions where userInteractions = 'value).duration
        |   totalDurations := sum(userInteractions.duration)
        | 
        |   { userId: 'userId, ratio: interactionDurations / totalDurations }
        | 
        | relativeDurations
        """.stripMargin
      
      val tree = compile(input)
      tree.provenance mustNotEqual NullProvenance
      tree.errors must beEmpty
    }
  }
}
