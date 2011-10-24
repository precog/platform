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
package com.reportgrid.quirrel
package typer

import edu.uwm.cs.gll.LineStream
import org.specs2.mutable.Specification
import parser._

object ProvenanceSpecs extends Specification with Parser with StubPhases with ProvenanceChecker {
  
  "provenance computation" should {
    "identify let according to its right expression" in {
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
      val tree = parse("new 1")
      tree.provenance must beLike {
        case DynamicProvenance(_) => ok
      }
      tree.errors must beEmpty
    }
    
    "identify relate according to its last expression" in {
      {
        val tree = parse("dataset(//a) :: dataset(//b) 3")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("dataset(//a) :: dataset(//b) dataset(//foo)")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("dataset(//a) :: dataset(//b) (new 1)")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify tic-var as value" in {
      val tree = parse("'foo")
      tree.provenance mustEqual ValueProvenance
      tree.errors must beEmpty
    }
    
    "identify string as value" in {
      val tree = parse("\"foo\"")
      tree.provenance mustEqual ValueProvenance
      tree.errors must beEmpty
    }
    
    "identify num as value" in {
      val tree = parse("42")
      tree.provenance mustEqual ValueProvenance
      tree.errors must beEmpty
    }
    
    "identify boolean as value" in {
      val tree = parse("true")
      tree.provenance mustEqual ValueProvenance
      tree.errors must beEmpty
    }
    
    "identify empty object definitions as value" in {
      val tree = parse("{}")
      tree.provenance mustEqual ValueProvenance
      tree.errors must beEmpty
    }
    
    "identify object definition according to its properties" in {
      {
        val tree = parse("{ a: 1, b: 2, c: 3}")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("{ a: 1, b: 2, c: dataset(//foo) }")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("{ a: 1, b: 2, c: new 2 }")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify empty array definitions as value" in {
      val tree = parse("[]")
      tree.provenance mustEqual ValueProvenance
      tree.errors must beEmpty
    }
    
    "identify array definition according to its values" in {
      {
        val tree = parse("[1, 2, 3]")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("[1, 2, dataset(//foo)]")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("[1, 2, new 3]")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify descent according to its child expression" in {
      {
        val tree = parse("1.foo")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("dataset(//bar).foo")
        tree.provenance mustEqual StaticProvenance("/bar")
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("(new 1).foo")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify dereference according to its children" in {
      {
        val tree = parse("1[2]")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("dataset(//foo)[2]")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("1[dataset(//foo)]")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("(new 1)[2]")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("1[new 2]")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify built-in reduce dispatch as value" in {
      {
        val tree = parse("count")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("max")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("mean")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("median")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("min")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("mode")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("stdDev")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("sum")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
    }
    
    "identify dataset dispatch with static params according to its path" in {
      {
        val tree = parse("dataset(//foo)")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("dataset(//bar)")
        tree.provenance mustEqual StaticProvenance("/bar")
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("dataset(//bar/baz)")
        tree.provenance mustEqual StaticProvenance("/bar/baz")
        tree.errors must beEmpty
      }
    }
    
    "identify dataset dispatch with non-static params as dynamic" in {
      {
        val tree = parse("dataset(42)")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("a := 42 dataset(a)")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("dataset(count)")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("dataset(new 42)")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify operation according to its children" in {
      {
        val tree = parse("1 where 2")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("dataset(//foo) where 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("1 where dataset(//foo)")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("new 1 where 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("1 where new 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify addition according to its children" in {
      {
        val tree = parse("1 + 2")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("dataset(//foo) + 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("1 + dataset(//foo)")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("new 1 + 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("1 + new 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify subtraction according to its children" in {
      {
        val tree = parse("1 - 2")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("dataset(//foo) - 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("1 - dataset(//foo)")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("new 1 - 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("1 - new 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify multiplication according to its children" in {
      {
        val tree = parse("1 * 2")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("dataset(//foo) * 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("1 * dataset(//foo)")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("new 1 * 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("1 * new 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify division according to its children" in {
      {
        val tree = parse("1 / 2")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("dataset(//foo) / 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("1 / dataset(//foo)")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("new 1 / 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("1 / new 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify less-than according to its children" in {
      {
        val tree = parse("1 < 2")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("dataset(//foo) < 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("1 < dataset(//foo)")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("new 1 < 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("1 < new 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify less-than-equal according to its children" in {
      {
        val tree = parse("1 <= 2")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("dataset(//foo) <= 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("1 <= dataset(//foo)")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("new 1 <= 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("1 <= new 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify greater-than according to its children" in {
      {
        val tree = parse("1 > 2")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("dataset(//foo) > 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("1 > dataset(//foo)")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("new 1 > 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("1 > new 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify greater-than-equal according to its children" in {
      {
        val tree = parse("1 >= 2")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("dataset(//foo) >= 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("1 >= dataset(//foo)")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("new 1 >= 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("1 >= new 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify equal according to its children" in {
      {
        val tree = parse("1 = 2")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("dataset(//foo) = 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("1 = dataset(//foo)")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("new 1 = 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("1 = new 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify not-equal according to its children" in {
      {
        val tree = parse("1 != 2")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("dataset(//foo) != 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("1 != dataset(//foo)")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("new 1 != 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("1 != new 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify boolean and according to its children" in {
      {
        val tree = parse("1 & 2")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("dataset(//foo) & 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("1 & dataset(//foo)")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("new 1 & 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("1 & new 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify boolean or according to its children" in {
      {
        val tree = parse("1 | 2")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("dataset(//foo) | 2")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("1 | dataset(//foo)")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("new 1 | 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("1 | new 2")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify complement according to its child" in {
      {
        val tree = parse("!1")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("!dataset(//foo)")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("!(new 1)")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify negation according to its child" in {
      {
        val tree = parse("~1")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("~dataset(//foo)")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("~(new 1)")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
    
    "identify parenthetical according to its child" in {
      {
        val tree = parse("(1)")
        tree.provenance mustEqual ValueProvenance
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("(dataset(//foo))")
        tree.provenance mustEqual StaticProvenance("/foo")
        tree.errors must beEmpty
      }
      
      {
        val tree = parse("(new 1)")
        tree.provenance must beLike {
          case DynamicProvenance(_) => ok
        }
        tree.errors must beEmpty
      }
    }
  }
  
  "provenance checking" should {
    "reject object definition on different datasets" in {
      val tree = parse("{ a: dataset(//foo), b: dataset(//bar) }")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject object definition on static and dynamic provenances" in {
      val tree = parse("{ a: dataset(//foo), b: new 1 }")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject object definition on differing dynamic provenances" in {
      val tree = parse("{ a: new 1, b: new 1 }")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject array definition on different datasets" in {
      val tree = parse("[ dataset(//foo), dataset(//bar) ]")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject array definition on static and dynamic provenances" in {
      val tree = parse("[ dataset(//foo), new 1 ]")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject array definition on differing dynamic provenances" in {
      val tree = parse("[ new 1, new 1 ]")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject deref on different datasets" in {
      val tree = parse("dataset(//foo)[dataset(//bar)]")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject deref on static and dynamic provenances" in {
      val tree = parse("dataset(//foo)[new 1]")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject deref on differing dynamic provenances" in {
      val tree = parse("(new 1)[new 1]")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject dispatch on different datasets" in {
      val tree = parse("fun('a, 'b) := 42 fun(dataset(//foo), dataset(//bar))")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject dispatch on static and dynamic provenances" in {
      val tree = parse("fun('a, 'b) := 42 fun(dataset(//foo), new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject dispatch on differing dynamic provenances" in {
      val tree = parse("fun('a, 'b) := 42 fun(new 1, new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject operation on different datasets" in {
      val tree = parse("dataset(//foo) where dataset(//bar)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject operation on static and dynamic provenances" in {
      val tree = parse("dataset(//foo) where new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject operation on differing dynamic provenances" in {
      val tree = parse("new 1 where new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject addition on different datasets" in {
      val tree = parse("dataset(//foo) + dataset(//bar)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject addition on static and dynamic provenances" in {
      val tree = parse("dataset(//foo) + new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject addition on differing dynamic provenances" in {
      val tree = parse("(new 1) + (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject subtraction on different datasets" in {
      val tree = parse("dataset(//foo) - dataset(//bar)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject subtraction on static and dynamic provenances" in {
      val tree = parse("dataset(//foo) - new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject subtraction on differing dynamic provenances" in {
      val tree = parse("(new 1) - (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject multiplication on different datasets" in {
      val tree = parse("dataset(//foo) * dataset(//bar)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject multiplication on static and dynamic provenances" in {
      val tree = parse("dataset(//foo) * new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject multiplication on differing dynamic provenances" in {
      val tree = parse("(new 1) * (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject division on different datasets" in {
      val tree = parse("dataset(//foo) / dataset(//bar)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject division on static and dynamic provenances" in {
      val tree = parse("dataset(//foo) / new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject division on differing dynamic provenances" in {
      val tree = parse("(new 1) / (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject less-than on different datasets" in {
      val tree = parse("dataset(//foo) < dataset(//bar)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject less-than on static and dynamic provenances" in {
      val tree = parse("dataset(//foo) < new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject less-than on differing dynamic provenances" in {
      val tree = parse("(new 1) < (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject less-than-equal on different datasets" in {
      val tree = parse("dataset(//foo) <= dataset(//bar)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject less-than-equal on static and dynamic provenances" in {
      val tree = parse("dataset(//foo) <= new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject less-than-equal on differing dynamic provenances" in {
      val tree = parse("(new 1) <= (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject greater-than on different datasets" in {
      val tree = parse("dataset(//foo) > dataset(//bar)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject greater-than on static and dynamic provenances" in {
      val tree = parse("dataset(//foo) > new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject greater-than on differing dynamic provenances" in {
      val tree = parse("(new 1) > (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject greater-than-equal on different datasets" in {
      val tree = parse("dataset(//foo) >= dataset(//bar)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject greater-than-equal on static and dynamic provenances" in {
      val tree = parse("dataset(//foo) >= new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject greater-than-equal on differing dynamic provenances" in {
      val tree = parse("(new 1) >= (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject equality on different datasets" in {
      val tree = parse("dataset(//foo) = dataset(//bar)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject equality on static and dynamic provenances" in {
      val tree = parse("dataset(//foo) = new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject equality on differing dynamic provenances" in {
      val tree = parse("(new 1) = (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject not-equality on different datasets" in {
      val tree = parse("dataset(//foo) != dataset(//bar)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject not-equality on static and dynamic provenances" in {
      val tree = parse("dataset(//foo) != new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject not-equality on differing dynamic provenances" in {
      val tree = parse("(new 1) != (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject boolean and on different datasets" in {
      val tree = parse("dataset(//foo) & dataset(//bar)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject boolean and on static and dynamic provenances" in {
      val tree = parse("dataset(//foo) & new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject boolean and on differing dynamic provenances" in {
      val tree = parse("(new 1) & (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject boolean or on different datasets" in {
      val tree = parse("dataset(//foo) | dataset(//bar)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject boolean or on static and dynamic provenances" in {
      val tree = parse("dataset(//foo) | new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject boolean or on differing dynamic provenances" in {
      val tree = parse("(new 1) | (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
  }
  
  "explicit relation" should {
    "fail on natively-related sets" in {
      {
        val tree = parse("dataset(//a) :: dataset(//a) 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(AlreadyRelatedSets)
      }
      
      {
        val tree = parse("1 :: 2 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(AlreadyRelatedSets)
      }
      
      {
        val tree = parse("a := new 1 a :: a 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(AlreadyRelatedSets)
      }
    }
    
    "fail on explicitly related sets" in {
      val tree = parse("a := dataset(//a) b := dataset(//b) a :: b a :: b 42")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(AlreadyRelatedSets)
    }
    
    "accept object definition on different datasets when related" in {
      val tree = parse("dataset(//foo) :: dataset(//bar) { a: dataset(//foo), b: dataset(//bar) }")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept object definition on static and dynamic provenances when related" in {
      val tree = parse("s := new 1 dataset(//foo) :: s { a: dataset(//foo), b: s }")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept object definition on differing dynamic provenances when related" in {
      val tree = parse("s1 := new 1 s2 := new 1 s1 :: s2 { a: s1, b: s2 }")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept array definition on different datasets when related" in {
      val tree = parse("dataset(//foo) :: dataset(//bar) [ dataset(//foo), dataset(//bar) ]")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept array definition on static and dynamic provenances when related" in {
      val tree = parse("s := new 1 dataset(//foo) :: s [ dataset(//foo), s ]")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept array definition on differing dynamic provenances when related" in {
      val tree = parse("s1 := new 1 s2 := new 1 s1 :: s2 [ s1, s2 ]")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept deref on different datasets when related" in {
      val tree = parse("dataset(//foo) :: dataset(//bar) dataset(//foo)[dataset(//bar)]")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept deref on static and dynamic provenances when related" in {
      val tree = parse("s := new 1 dataset(//foo) :: s dataset(//foo)[s]")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept deref on differing dynamic provenances when related" in {
      val tree = parse("s1 := new 1 s2 := new 1 s1 :: s2 s1[s2]")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept dispatch on different datasets when related" in {
      val tree = parse("dataset(//foo) :: dataset(//bar) fun('a, 'b) := 42 fun(dataset(//foo), dataset(//bar))")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty
    }
    
    "accept dispatch on static and dynamic provenances when related" in {
      val tree = parse("s := new 1 dataset(//foo) :: s fun('a, 'b) := 42 fun(dataset(//foo), s)")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty
    }
    
    "accept dispatch on differing dynamic provenances when related" in {
      val tree = parse("s1 := new 1 s2 := new 1 s1 :: s2 fun('a, 'b) := 42 fun(s1, s2)")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty
    }
    
    "accept operation on different datasets when related" in {
      val tree = parse("dataset(//foo) :: dataset(//bar) dataset(//foo) where dataset(//bar)")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept operation on static and dynamic provenances when related" in {
      val tree = parse("s := new 1 dataset(//foo) :: s dataset(//foo) where s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept operation on differing dynamic provenances when related" in {
      val tree = parse("s1 := new 1 s2 := new 1 s1 :: s2 s1 where s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept addition on different datasets when related" in {
      val tree = parse("dataset(//foo) :: dataset(//bar) dataset(//foo) + dataset(//bar)")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept addition on static and dynamic provenances when related" in {
      val tree = parse("s := new 1 dataset(//foo) :: s dataset(//foo) + s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept addition on differing dynamic provenances when related" in {
      val tree = parse("s1 := new 1 s2 := new 1 s1 :: s2 s1 + s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept subtraction on different datasets when related" in {
      val tree = parse("dataset(//foo) :: dataset(//bar) dataset(//foo) - dataset(//bar)")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept subtraction on static and dynamic provenances when related" in {
      val tree = parse("s := new 1 dataset(//foo) :: s dataset(//foo) - s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept subtraction on differing dynamic provenances when related" in {
      val tree = parse("s1 := new 1 s2 := new 1 s1 :: s2 s1 - s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept multiplication on different datasets when related" in {
      val tree = parse("dataset(//foo) :: dataset(//bar) dataset(//foo) * dataset(//bar)")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept multiplication on static and dynamic provenances when related" in {
      val tree = parse("s := new 1 dataset(//foo) :: s dataset(//foo) * s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept multiplication on differing dynamic provenances when related" in {
      val tree = parse("s1 := new 1 s2 := new 1 s1 :: s2 s1 * s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept division on different datasets when related" in {
      val tree = parse("dataset(//foo) :: dataset(//bar) dataset(//foo) / dataset(//bar)")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept division on static and dynamic provenances when related" in {
      val tree = parse("s := new 1 dataset(//foo) :: s dataset(//foo) / s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept division on differing dynamic provenances when related" in {
      val tree = parse("s1 := new 1 s2 := new 1 s1 :: s2 s1 / s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept less-than on different datasets when related" in {
      val tree = parse("dataset(//foo) :: dataset(//bar) dataset(//foo) < dataset(//bar)")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept less-than on static and dynamic provenances when related" in {
      val tree = parse("s := new 1 dataset(//foo) :: s dataset(//foo) < s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept less-than on differing dynamic provenances when related" in {
      val tree = parse("s1 := new 1 s2 := new 1 s1 :: s2 s1 < s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept less-than-equal on different datasets when related" in {
      val tree = parse("dataset(//foo) :: dataset(//bar) dataset(//foo) <= dataset(//bar)")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept less-than-equal on static and dynamic provenances when related" in {
      val tree = parse("s := new 1 dataset(//foo) :: s dataset(//foo) <= s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept less-than-equal on differing dynamic provenances when related" in {
      val tree = parse("s1 := new 1 s2 := new 1 s1 :: s2 s1 <= s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept greater-than on different datasets when related" in {
      val tree = parse("dataset(//foo) :: dataset(//bar) dataset(//foo) > dataset(//bar)")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept greater-than on static and dynamic provenances when related" in {
      val tree = parse("s := new 1 dataset(//foo) :: s dataset(//foo) > s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept greater-than on differing dynamic provenances when related" in {
      val tree = parse("s1 := new 1 s2 := new 1 s1 :: s2 s1 > s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept greater-than-equal on different datasets when related" in {
      val tree = parse("dataset(//foo) :: dataset(//bar) dataset(//foo) >= dataset(//bar)")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept greater-than-equal on static and dynamic provenances when related" in {
      val tree = parse("s := new 1 dataset(//foo) :: s dataset(//foo) >= s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept greater-than-equal on differing dynamic provenances when related" in {
      val tree = parse("s1 := new 1 s2 := new 1 s1 :: s2 s1 >= s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept equality on different datasets when related" in {
      val tree = parse("dataset(//foo) :: dataset(//bar) dataset(//foo) = dataset(//bar)")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept equality on static and dynamic provenances when related" in {
      val tree = parse("s := new 1 dataset(//foo) :: s dataset(//foo) = s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept equality on differing dynamic provenances when related" in {
      val tree = parse("s1 := new 1 s2 := new 1 s1 :: s2 s1 = s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept not-equality on different datasets when related" in {
      val tree = parse("dataset(//foo) :: dataset(//bar) dataset(//foo) != dataset(//bar)")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept not-equality on static and dynamic provenances when related" in {
      val tree = parse("s := new 1 dataset(//foo) :: s dataset(//foo) != s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept not-equality on differing dynamic provenances when related" in {
      val tree = parse("s1 := new 1 s2 := new 1 s1 :: s2 s1 != s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept boolean and on different datasets when related" in {
      val tree = parse("dataset(//foo) :: dataset(//bar) dataset(//foo) & dataset(//bar)")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept boolean and on static and dynamic provenances when related" in {
      val tree = parse("s := new 1 dataset(//foo) :: s dataset(//foo) & s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept boolean and on differing dynamic provenances when related" in {
      val tree = parse("s1 := new 1 s2 := new 1 s1 :: s2 s1 & s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept boolean or on different datasets when related" in {
      val tree = parse("dataset(//foo) :: dataset(//bar) dataset(//foo) | dataset(//bar)")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept boolean or on static and dynamic provenances when related" in {
      val tree = parse("s := new 1 dataset(//foo) :: s dataset(//foo) | s")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "accept boolean or on differing dynamic provenances when related" in {
      val tree = parse("s1 := new 1 s2 := new 1 s1 :: s2 s1 | s2")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty      
    }
    
    "reject addition with unrelated relation" in {
      val tree = parse("dataset(//a) :: dataset(//b) dataset(//c) + dataset(//d)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
  }
  
  "null provenance" should {
    "synthesize when name binding is null in dispatch" in {
      parse("fubar").provenance mustEqual NullProvenance
    }
    
    "propagate through let" in {
      {
        val tree = parse("a := dataset(//foo) + dataset(//b) a")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = parse("a := dataset(//foo) a + dataset(//bar)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "not propagate through new" in {
      val tree = parse("new (dataset(//a) + dataset(//b))")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "propagate through relate" in {
      {
        val tree = parse("(dataset(//a) + dataset(//b)) :: dataset(//c) 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = parse("dataset(//c) :: (dataset(//a) + dataset(//b)) 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through object definition" in {
      val tree = parse("{ a: dataset(//a) + dataset(//b), b: 42 }")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "propagate through array definition" in {
      val tree = parse("[dataset(//a) + dataset(//b), 42]")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "propagate through descent" in {
      val tree = parse("(dataset(//a) + dataset(//b)).foo")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "propagate through dereference" in {
      {
        val tree = parse("(dataset(//a) + dataset(//b))[42]")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = parse("42[dataset(//a) + dataset(//b)]")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through dispatch" in {
      val tree = parse("a('b) := 42 a(dataset(//a) + dataset(//b))")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "propagate through operation" in {
      {
        val tree = parse("(dataset(//a) + dataset(//b)) where 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = parse("42 + (dataset(//a) where dataset(//b))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through addition" in {
      {
        val tree = parse("(dataset(//a) + dataset(//b)) + 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = parse("42 + (dataset(//a) + dataset(//b))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through subtraction" in {
      {
        val tree = parse("(dataset(//a) + dataset(//b)) - 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = parse("42 - (dataset(//a) + dataset(//b))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through multiplication" in {
      {
        val tree = parse("(dataset(//a) + dataset(//b)) * 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = parse("42 * (dataset(//a) + dataset(//b))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through division" in {
      {
        val tree = parse("(dataset(//a) + dataset(//b)) / 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = parse("42 / (dataset(//a) + dataset(//b))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through less-than" in {
      {
        val tree = parse("(dataset(//a) + dataset(//b)) < 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = parse("42 < (dataset(//a) + dataset(//b))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through less-than-equal" in {
      {
        val tree = parse("(dataset(//a) + dataset(//b)) <= 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = parse("42 <= (dataset(//a) + dataset(//b))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through greater-than" in {
      {
        val tree = parse("(dataset(//a) + dataset(//b)) > 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = parse("42 > (dataset(//a) + dataset(//b))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through greater-than-equal" in {
      {
        val tree = parse("(dataset(//a) + dataset(//b)) >= 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = parse("42 >= (dataset(//a) + dataset(//b))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through equality" in {
      {
        val tree = parse("(dataset(//a) + dataset(//b)) = 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = parse("42 = (dataset(//a) + dataset(//b))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through not-equality" in {
      {
        val tree = parse("(dataset(//a) + dataset(//b)) != 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = parse("42 != (dataset(//a) + dataset(//b))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through boolean and" in {
      {
        val tree = parse("(dataset(//a) + dataset(//b)) & 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = parse("42 & (dataset(//a) + dataset(//b))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through boolean or" in {
      {
        val tree = parse("(dataset(//a) + dataset(//b)) | 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = parse("42 | (dataset(//a) + dataset(//b))")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through complementation" in {
      val tree = parse("!(dataset(//a) + dataset(//b))")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "propagate through negation" in {
      val tree = parse("~(dataset(//a) + dataset(//b))")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "propagate through parenthetical" in {
      val tree = parse("(dataset(//a) + dataset(//b))")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
  }
  
  def parse(str: String): Expr = parse(LineStream(str))
}
