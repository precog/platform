package com.precog
package quirrel
package typer

import com.codecommit.gll.LineStream
import org.specs2.mutable.Specification

import java.io.File
import scala.io.Source

object NullProvenanceSpecs extends Specification
    with StubPhases
    with CompilerUtils
    with Compiler
    with ProvenanceChecker 
    with RandomLibrarySpec {

  import ast._
  
  "null provenance" should {
    "synthesize when name binding is null in dispatch" in {
      compileSingle("fubar").provenance mustEqual NullProvenance
    }
    
    "propagate through let" in {
      {
        val tree = compileSingle("a := //foo + //b a")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compileSingle("a := //foo a + //bar")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through import" in {
      val tree = compileSingle("import std (//a + //b)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "propagate through assert" in {
      "left" >> {
        val tree = compileSingle("assert (//a + //b) 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      "right" >> {
        val tree = compileSingle("assert 42 (//a + //b)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through new" in {
      val tree = compileSingle("new (//a + //b)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }

    "propagate through solve" in {
      val tree = compileSingle("solve 'foo = //a + //b 'foo")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "propagate through relate" in {
      {
        val tree = compileSingle("(//a + //b) ~ //c 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compileSingle("//c ~ (//a + //b) 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through object definition" in {
      val tree = compileSingle("{ a: //a + //b, b: 42 }")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "propagate through array definition" in {
      val tree = compileSingle("[//a + //b, 42]")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "propagate through descent" in {
      val tree = compileSingle("(//a + //b).foo")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "propagate through metadescent" in {
      val tree = compileSingle("(//a + //b)@foo")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "propagate through dereference" in {
      {
        val tree = compileSingle("(//a + //b)[42]")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compileSingle("42[//a + //b]")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through dispatch" in {
      val tree = compileSingle("a(b) := b a(//a + //b)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "propagate through where" in {
      {
        val tree = compileSingle("(//a + //b) where 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compileSingle("42 + (//a where //b)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through with" in {
      {
        val tree = compileSingle("(//a + //b) with 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compileSingle("42 + (//a with //b)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through union" in {
      {
        val tree = compileSingle("(//a + //b) union 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets) 
      }
      
      {
        val tree = compileSingle("42 union (//a + //b)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through intersect" in {
      {
        val tree = compileSingle("(//a + //b) intersect 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compileSingle("42 intersect (//a + //b)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }    

    "propagate through difference" in {
      {
        val tree = compileSingle("(//a + //b) difference 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compileSingle("42 difference (//a + //b)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through addition" in {
      {
        val tree = compileSingle("(//a + //b) + 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compileSingle("42 + (//a + //b)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through subtraction" in {
      {
        val tree = compileSingle("(//a + //b) - 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compileSingle("42 - (//a + //b)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through multiplication" in {
      {
        val tree = compileSingle("(//a + //b) * 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compileSingle("42 * (//a + //b)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through division" in {
      {
        val tree = compileSingle("(//a + //b) / 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compileSingle("42 / (//a + //b)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through mod" in {
      {
        val tree = compileSingle("(//a + //b) % 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compileSingle("42 % (//a + //b)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through less-than" in {
      {
        val tree = compileSingle("(//a + //b) < 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compileSingle("42 < (//a + //b)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through less-than-equal" in {
      {
        val tree = compileSingle("(//a + //b) <= 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compileSingle("42 <= (//a + //b)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through greater-than" in {
      {
        val tree = compileSingle("(//a + //b) > 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compileSingle("42 > (//a + //b)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through greater-than-equal" in {
      {
        val tree = compileSingle("(//a + //b) >= 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compileSingle("42 >= (//a + //b)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through equality" in {
      {
        val tree = compileSingle("(//a + //b) = 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compileSingle("42 = (//a + //b)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through not-equality" in {
      {
        val tree = compileSingle("(//a + //b) != 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compileSingle("42 != (//a + //b)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through boolean and" in {
      {
        val tree = compileSingle("(//a + //b) & 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compileSingle("42 & (//a + //b)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through boolean or" in {
      {
        val tree = compileSingle("(//a + //b) | 42")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
      
      {
        val tree = compileSingle("42 | (//a + //b)")
        tree.provenance mustEqual NullProvenance
        tree.errors mustEqual Set(OperationOnUnrelatedSets)
      }
    }
    
    "propagate through complementation" in {
      val tree = compileSingle("!(//a + //b)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "propagate through negation" in {
      val tree = compileSingle("neg (//a + //b)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "propagate through parenthetical" in {
      val tree = compileSingle("(//a + //b)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
  }
  
  "constraining expression determination" should {
    "leave values unconstrained" in {
      compileSingle("42").constrainingExpr must beNone
    }
    
    "leave loads unconstrained when outside a relation" in {
      compileSingle("//foo").constrainingExpr must beNone
    }
    
    "constrain loads within a relation" in {
      {
        val Relate(_, from, _, in) = compileSingle("//foo ~ //bar //foo")
        in.constrainingExpr must beSome(from)
      }
      
      {
        val Relate(_, _, to, in) = compileSingle("//foo ~ //bar //bar")
        in.constrainingExpr must beSome(to)
      }
    }
    
    "leave unconnected loads unconstrained within a relation" in {
      {
        val Relate(_, from, _, in) = compileSingle("//foo ~ //bar //baz")
        in.constrainingExpr must beNone
      }
      
      {
        val Relate(_, from, _, in) = compileSingle("./foo ~ ./bar //foo")
        in.constrainingExpr must beNone
      }
    }
    
    "leave relative loads unconstrained when outside a relation" in {
      compileSingle("./foo").constrainingExpr must beNone
    }
    
    "constrain relative loads within a relation" in {
      {
        val Relate(_, from, _, in) = compileSingle("./foo ~ ./bar ./foo")
        in.constrainingExpr must beSome(from)
      }
      
      {
        val Relate(_, _, to, in) = compileSingle("./foo ~ ./bar ./bar")
        in.constrainingExpr must beSome(to)
      }
    }
    
    "leave unconnected relative loads unconstrained within a relation" in {
      {
        val Relate(_, from, _, in) = compileSingle("./foo ~ ./bar ./baz")
        in.constrainingExpr must beNone
      }
      
      {
        val Relate(_, from, _, in) = compileSingle("//foo ~ //bar ./foo")
        in.constrainingExpr must beNone
      }
    }
    
    "propagate constraints through a nested relation" in {
      {
        val Relate(_, from1, to1, Relate(_, from2, to2, in)) = compileSingle("""
          | //foo ~ //bar
          |   //foo ~ //baz
          |     //foo""".stripMargin)
        
        in.constrainingExpr must beSome(from2)
      }
      
      {
        val Relate(_, from1, to1, Relate(_, from2, to2, in)) = compileSingle("""
          | //foo ~ //bar
          |   //foo ~ //baz
          |     //bar""".stripMargin)
        
        in.constrainingExpr must beSome(to1)
      }
      
      {
        val Relate(_, from1, to1, Relate(_, from2, to2, in)) = compileSingle("""
          | //foo ~ //bar
          |   //foo ~ //baz
          |     //baz""".stripMargin)
        
        in.constrainingExpr must beSome(to2)
      }
    }
  }
}
