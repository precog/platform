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

object NullProvenanceSpecs extends Specification
    with StubPhases
    with Compiler
    with ProvenanceChecker 
    with RandomLibrary {

  import ast._
  
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
    
    "propagate through metadescent" in {
      val tree = compile("(//a + //b)@foo")
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
}
