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
      val tree = compile("fun(a, b) := a + b fun(//foo, //bar)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject dispatch on static and dynamic provenances" in {
      val tree = compile("fun(a, b) := a + b fun(//foo, new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject dispatch on differing dynamic provenances" in {
      val tree = compile("fun(a, b) := a + b fun(new 1, new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject dispatch to new-modified identity function with dynamic provenance" in {
      val tree = compile("fun(a) := a + new 42 fun(new 24)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject dispatch to new-modified identity function with static provenance" in {
      val tree = compile("fun(a) := a + new 42 fun(//foo)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject dispatch to load-modified identity function with dynamic provenance" in {
      val tree = compile("fun(a) := a + //foo fun(new 24)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject dispatch to load-modified identity function with static provenance" in {
      val tree = compile("fun(a) := a + //bar fun(//foo)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "accept dispatch to load-modified identity function with union provenance" in {
      val input = """
        | foo := //foo
        | bar := //bar
        |
        | id(a, b) := a + b + foo
        |
        | foo ~ bar
        |   id(foo, bar)
        """.stripMargin
      val tree = compile(input)
      tree.provenance.possibilities must containAllOf(List(StaticProvenance("/foo"), StaticProvenance("/bar")))
      tree.errors must beEmpty
    }
    
    "accept a dispatch to a function wrapping Add with related parameters" in {
      val tree = compile("a(b) := b + //foo a(//foo)")
      tree.provenance mustEqual StaticProvenance("/foo")
      tree.errors must beEmpty
    }
    
    "reject a dispatch to a function wrapping Add with unrelated parameters" in {
      val tree = compile("a(b) := b + //foo a(//bar)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "accept a dispatch to a function wrapping Add with explicitly related parameters" in {
      val tree = compile("a(b) := b + //foo //foo ~ //bar a(//bar)")
      tree.provenance.possibilities must containAllOf(List(StaticProvenance("/foo"), StaticProvenance("/bar")))
      tree.errors must beEmpty
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
    
    "propagate provenance through multiple let bindings" in {
      val tree @ Let(_, _, _, _, _) = compile("back := //foo ~ //bar //foo + //bar back")
      
      tree.resultProvenance must beLike {
        case UnionProvenance(StaticProvenance("/foo"), StaticProvenance("/bar")) => ok
        case UnionProvenance(StaticProvenance("/bar"), StaticProvenance("/foo")) => ok
      }
      
      tree.resultProvenance.isParametric mustEqual false
      
      tree.provenance must beLike {
        case UnionProvenance(StaticProvenance("/foo"), StaticProvenance("/bar")) => ok
        case UnionProvenance(StaticProvenance("/bar"), StaticProvenance("/foo")) => ok
      }
      
      tree.errors must beEmpty
    }
    
    "accept union on union provenances" in {
      val input = """
        | foo := //foo
        | bar := //bar
        | baz := //baz
        | 
        | fb := foo ~ bar foo + bar
        | bb := bar ~ baz bar + baz
        | 
        | fb union bb
        | """.stripMargin
        
      val tree = compile(input)
      
      tree.provenance must beLike {
        case UnionProvenance(DynamicProvenance(_), DynamicProvenance(_)) => ok
      }
      
      tree.errors must beEmpty
    }
    
    "accept intersect on union provenances" in {
      val input = """
        | foo := //foo
        | bar := //bar
        | baz := //baz
        | 
        | fb := foo ~ bar foo + bar
        | bb := bar ~ baz bar + baz
        | 
        | fb intersect bb
        | """.stripMargin
        
      val tree = compile(input)
      
      tree.provenance must beLike {
        case UnionProvenance(DynamicProvenance(_), DynamicProvenance(_)) => ok
      }
      
      tree.errors must beEmpty
    }
    
    "accept difference on union provenances" in {
      val input = """
        | foo := //foo
        | bar := //bar
        | baz := //baz
        | 
        | fb := foo ~ bar foo + bar
        | bb := bar ~ baz bar + baz
        | 
        | fb difference bb
        | """.stripMargin
        
      val tree = compile(input)
      
      tree.provenance must beLike {
        case UnionProvenance(StaticProvenance("/foo"), StaticProvenance("/bar")) => ok
        case UnionProvenance(StaticProvenance("/bar"), StaticProvenance("/foo")) => ok
      }
      
      tree.errors must beEmpty
    }
    
    "accept union through a function on static provenances" in {
      val input = """
        | f(a, b) := a union b
        | 
        | f(//foo, //bar)
        | """.stripMargin
      
      val tree = compile(input)
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty
    }
    
    "accept intersect through a function on static provenance" in {
      val input = """
        | f(a, b) := a intersect b
        | 
        | f(//foo, //bar)
        | """.stripMargin
      
      val tree = compile(input)
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty
    }
    
    "accept difference through a function on static provenances" in {
      val input = """
        | f(a, b) := a difference b
        | 
        | f(//foo, //bar)
        | """.stripMargin
      
      val tree = compile(input)
      tree.provenance mustEqual StaticProvenance("/foo")
      tree.errors must beEmpty
    }
    
    "accept union through a function on union provenances" in {
      val input = """
        | foo := //foo
        | bar := //bar
        | baz := //baz
        | 
        | fb := foo ~ bar foo + bar
        | bb := bar ~ baz bar + baz
        | 
        | f(a, b) := a union b
        | f(fb, bb)
        | """.stripMargin
        
      val tree = compile(input)
      
      tree.provenance must beLike {
        case UnionProvenance(DynamicProvenance(_), DynamicProvenance(_)) => ok
      }
      
      tree.errors must beEmpty
    }
    
    "accept intersect through a function on union provenances" in {
      val input = """
        | foo := //foo
        | bar := //bar
        | baz := //baz
        | 
        | fb := foo ~ bar foo + bar
        | bb := bar ~ baz bar + baz
        | 
        | f(a, b) := a intersect b
        | f(fb, bb)
        | """.stripMargin
        
      val tree = compile(input)
      
      tree.provenance must beLike {
        case UnionProvenance(DynamicProvenance(_), DynamicProvenance(_)) => ok
      }
      
      tree.errors must beEmpty
    }
    
    "accept difference through a function on union provenances" in {
      val input = """
        | foo := //foo
        | bar := //bar
        | baz := //baz
        | 
        | fb := foo ~ bar foo + bar
        | bb := bar ~ baz bar + baz
        | 
        | f(a, b) := a difference b
        | f(fb, bb)
        | """.stripMargin
        
      val tree = compile(input)
      
      tree.provenance must beLike {
        case UnionProvenance(StaticProvenance("/foo"), StaticProvenance("/bar")) => ok
        case UnionProvenance(StaticProvenance("/bar"), StaticProvenance("/foo")) => ok
      }
      
      tree.errors must beEmpty
    }
    
    "reject union on non-matching union provenances" in {
      val input = """
        | foo := //foo
        | bar := //bar
        | 
        | fb := foo ~ bar foo + bar
        | 
        | fb union //baz
        | """.stripMargin
        
      val tree = compile(input)
      
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(UnionProvenanceDifferentLength)
    }
    
    "reject intersect on non-matching union provenances" in {
      val input = """
        | foo := //foo
        | bar := //bar
        | baz := //baz
        | 
        | fb := foo ~ bar foo + bar
        | 
        | fb intersect baz
        | """.stripMargin
        
      val tree = compile(input)
      
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(IntersectProvenanceDifferentLength)
    }
    
    "reject difference on non-matching union provenances" in {
      val input = """
        | foo := //foo
        | bar := //bar
        | 
        | fb := foo ~ bar foo + bar
        | 
        | fb difference //baz
        | """.stripMargin
        
      val tree = compile(input)
      
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(DifferenceProvenanceDifferentLength)
    }
    
    "accept union through a function on non-matching union provenances" in {
      val input = """
        | foo := //foo
        | bar := //bar
        | baz := //baz
        | 
        | fb := foo ~ bar foo + bar
        | 
        | f(a, b) := a union b
        | f(fb, //baz)
        | """.stripMargin
        
      val tree = compile(input)
      
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(UnionProvenanceDifferentLength)
    }
    
    "accept intersect through a function on union provenances" in {
      val input = """
        | foo := //foo
        | bar := //bar
        | 
        | fb := foo ~ bar foo + bar
        | 
        | f(a, b) := a intersect b
        | f(fb, //baz)
        | """.stripMargin
        
      val tree = compile(input)
      
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(IntersectProvenanceDifferentLength)
    }
    
    "accept difference through a function on union provenances" in {
      val input = """
        | foo := //foo
        | bar := //bar
        | baz := //baz
        | 
        | fb := foo ~ bar foo + bar
        | 
        | f(a, b) := a difference b
        | f(fb, //baz)
        | """.stripMargin
        
      val tree = compile(input)
      
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(DifferenceProvenanceDifferentLength)
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
