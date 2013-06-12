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

import com.codecommit.gll.LineStream
import org.specs2.mutable.Specification

import java.io.File
import scala.io.Source

object ProvenanceCheckingSpecs extends Specification
    with StubPhases
    with CompilerUtils
    with Compiler
    with ProvenanceChecker 
    with RandomLibrarySpec {

  import ast._
  
  "provenance checking" should {
    "reject object definition on different loads" in {
      val tree = compileSingle("{ a: //foo, b: //bar }")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject object definition on absolute vs relative loads" in {
      val tree = compileSingle("{ a: //foo, b: ./foo }")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject object definition on static and dynamic provenances" in {
      val tree = compileSingle("{ a: //foo, b: new 1 }")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject object definition on differing dynamic provenances" in {
      val tree = compileSingle("{ a: new 1, b: new 1 }")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject array definition on different loads" in {
      val tree = compileSingle("[ //foo, //bar ]")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject array definition on absolute vs relative loads" in {
      val tree = compileSingle("[ //foo, ./foo ]")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject array definition on static and dynamic provenances" in {
      val tree = compileSingle("[ //foo, new 1 ]")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject array definition on differing dynamic provenances" in {
      val tree = compileSingle("[ new 1, new 1 ]")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject deref on different loads" in {
      val tree = compileSingle("//foo[//bar]")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject deref on different absolute vs relative loads" in {
      val tree = compileSingle("//foo[./foo]")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject deref on static and dynamic provenances" in {
      val tree = compileSingle("(//foo)[new 1]")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject deref on differing dynamic provenances" in {
      val tree = compileSingle("(new 1)[new 1]")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject dispatch on different loads" in {
      val tree = compileSingle("fun(a, b) := a + b fun(//foo, //bar)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject dispatch on absolute vs relative loads" in {
      val tree = compileSingle("fun(a, b) := a + b fun(//foo, ./foo)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject dispatch on static and dynamic provenances" in {
      val tree = compileSingle("fun(a, b) := a + b fun(//foo, new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject dispatch on differing dynamic provenances" in {
      val tree = compileSingle("fun(a, b) := a + b fun(new 1, new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject dispatch to new-modified identity function with dynamic provenance" in {
      val tree = compileSingle("fun(a) := a + new 42 fun(new 24)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject dispatch to new-modified identity function with static provenance" in {
      val tree = compileSingle("fun(a) := a + new 42 fun(//foo)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject dispatch to load-modified identity function with dynamic provenance" in {
      val tree = compileSingle("fun(a) := a + //foo fun(new 24)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject dispatch to load-modified identity function with static provenance" in {
      val tree = compileSingle("fun(a) := a + //bar fun(//foo)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "accept dispatch to load-modified identity function with product provenance" in {
      val input = """
        | foo := //foo
        | bar := //bar
        |
        | id(a, b) := a + b + foo
        |
        | foo ~ bar
        |   id(foo, bar)
        """.stripMargin
      val tree = compileSingle(input)
      tree.provenance.possibilities must containAllOf(List(StaticProvenance("/foo"), StaticProvenance("/bar")))
      tree.errors must beEmpty
    }
    
    "reject dispatch to relative load-modified identity function with dynamic provenance" in {
      val tree = compileSingle("fun(a) := a + ./foo fun(new 24)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject dispatch to relative load-modified identity function with static provenance" in {
      val tree = compileSingle("fun(a) := a + ./bar fun(//foo)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "accept dispatch to relative load-modified identity function with union provenance" in {
      val input = """
        | foo := ./foo
        | bar := ./bar
        |
        | id(a, b) := a + b + foo
        |
        | foo ~ bar
        |   id(foo, bar)
        """.stripMargin
      val tree = compileSingle(input)
      tree.provenance.possibilities must containAllOf(List(StaticProvenance("foo"), StaticProvenance("bar")))
      tree.errors must beEmpty
    }

    "reject sum of two news of same value" in {
      val input = """ (new 5) + (new 5) """

      val tree = compileSingle(input)
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }

    "reject sum of two unrelated news" in {
      val input = """ (new 5) + (new 6) """

      val tree = compileSingle(input)
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }

    "reject relation of two news" in {
      val input = """
        new 5 ~ new 5
          (new 5) + (new 5)
      """

      val tree = compileSingle(input)
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }

    "reject case when two already-related news are related through dispatch" in {
      val input = """
        five := new 5
        five ~ five 
          five + five 
      """

      val tree = compileSingle(input)
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(AlreadyRelatedSets)
    }

    "accept case when two news are related" in {
      val input = """
        five := new 5
        six := new 6
        five ~ six 
          five + six 
      """

      val tree = compileSingle(input)
      tree.provenance must beLike {
        case ProductProvenance(DynamicProvenance(_), DynamicProvenance(_)) => ok
      }
      tree.errors must beEmpty
    }

    "accept sum of two related news through dispatch" in {
      val input = """
        five := new 5
        five + five 
      """

      val tree = compileSingle(input)
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty
    }

    "reject relate of two dispatches from the same function" in {
      val input = """
        | f(x) := new x
        |
        | f(5) ~ f(6)
        |   f(5) + f(6)
        | """.stripMargin

      val tree = compileSingle(input)
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject relate of two `equivalent` dispatches from the same function" in {
      val input = """
        | f(x) := new x
        |
        | f(5) ~ f(5)
        |   f(5) + f(5)
        | """.stripMargin

      val tree = compileSingle(input)
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "accept relate of two equivalent dispatches from the same function" in {
      val input = """
        | f(x) := new x
        | y := f(5)
        | z := f(6)
        |
        | y ~ z
        |   y + z
        | """.stripMargin

      val tree = compileSingle(input)
      tree.provenance must beLike {
        case ProductProvenance(DynamicProvenance(_), DynamicProvenance(_)) => ok
      }
      tree.errors must beEmpty
    }
    
    "reject sum of two distinct dispatches from the same function" in {
      val input = """
        | f(x) := new x
        | y := f(5)
        | z := f(6)
        | y + z
        | """.stripMargin

      val tree = compileSingle(input)
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject sum of two `equivalent` dispatches from the same function" in {
      val input = """
        | f(x) := new x
        | y := f(5)
        | z := f(5)
        | y + z
        | """.stripMargin

      val tree = compileSingle(input)
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject sum of two dispatches from the same new function" in {
      val input = """
        | f(x) := new x
        |
        | f(5) + f(6)
        | """.stripMargin

      val tree = compileSingle(input)
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject sum of two equivalent dispatches from the same function" in {
      val input = """
        | f(x) := new x
        | f(5) + f(5)
        | """.stripMargin

      val tree = compileSingle(input)
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }

    "accept relate of two dispatches through dispatch from the same function" in {
      val input = """
        | f(x) :=
        |   y := new x
        |   z := new x
        |   y ~ z
        |     y + z
        | f(5)
        | """.stripMargin

      val tree = compileSingle(input)
      tree.provenance must beLike {
        case ProductProvenance(DynamicProvenance(_), DynamicProvenance(_)) => ok
      }
      tree.errors must beEmpty
    }
    
    "reject sum of two equivalent dispatches through dispatch from the same function" in {
      val input = """
        | f(x) :=
        |   y := new x
        |   z := new x
        |   y + z
        | f(5)
        | """.stripMargin

      val tree = compileSingle(input)
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }

    "reject sum of two news in the same function" in {
      val input = """
        | f(x) :=
        |   (new x) + (new x)
        | f(5)
        | """.stripMargin

      val tree = compileSingle(input)
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject relate of dispatch through relate" in {
      val input = """
        | f(x) :=
        |   new x ~ new x
        |   (new x) + (new x)
        | f(5)
        | """.stripMargin

      val tree = compileSingle(input)
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }.pendingUntilFixed  //doesn't compile correctly, see PLATFORM-1093
    
    "reject relate of dispatch with two equivalent parameters" in {
      val input = """
        | f(x, y) :=
        |   new x ~ new y
        |   (new x) + (new y)
        | f(5, 5)
        | """.stripMargin

      val tree = compileSingle(input)
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject relate of dispatch with two equivalent parameters" in {
      val input = """
        | f(x, y) := (new x) + (new y)
        | f(5, 5)
        | """.stripMargin

      val tree = compileSingle(input)
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }

    "reject sum of dispatch with two distinct parameters" in {
      val input = """
        | f(x, y) := (new x) + (new y)
        | f(5, 6)
        | """.stripMargin

      val tree = compileSingle(input)
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "accept relate of dispatch with two distinct parameters" in {
      val input = """
        | f(x, y) :=
        |   a := new x
        |   b := new y
        |   a ~ b
        |     a + b
        | f(5, 6)
        | """.stripMargin

      val tree = compileSingle(input)
      tree.provenance must beLike {
        case ProductProvenance(DynamicProvenance(_), DynamicProvenance(_)) => ok
      }
      tree.errors must beEmpty
    }
    
    "accept relate of dispatch with equal parameters" in {
      val input = """
        | f(x, y) :=
        | a := new x
        | b := new y
        | a ~ b
        |   a + b
        | f(5, 5)
        | """.stripMargin

      val tree = compileSingle(input)
      tree.provenance must beLike {
        case ProductProvenance(DynamicProvenance(_), DynamicProvenance(_)) => ok
      }
      tree.errors must beEmpty
    }

    "accept a dispatch to a function wrapping Add with related parameters" in {
      val tree = compileSingle("a(b) := b + //foo a(//foo)")
      tree.provenance mustEqual StaticProvenance("/foo")
      tree.errors must beEmpty
    }
    
    "reject a dispatch to a function wrapping Add with unrelated parameters" in {
      val tree = compileSingle("a(b) := b + //foo a(//bar)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "accept a dispatch to a function wrapping Add with explicitly related parameters" in {
      val tree = compileSingle("a(b) := b + //foo //foo ~ //bar a(//bar)")
      tree.provenance.possibilities must containAllOf(List(StaticProvenance("/foo"), StaticProvenance("/bar")))
      tree.errors must beEmpty
    }
    
    "reject where on different loads" in {
      val tree = compileSingle("//foo where //bar")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }

    "reject operations on different loads through where" in {
      val rawInput = """
        | a := //users
        | b := //heightWeight
        | { name: a.name, height: b.height } where a.userId = b.userId """.stripMargin
        
      val tree = compileSingle(rawInput)
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject where on different relative loads" in {
      val tree = compileSingle("./foo where ./bar")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }

    "reject operations on different relative loads through where" in {
      val rawInput = """
        | a := ./users
        | b := ./heightWeight
        | { name: a.name, height: b.height } where a.userId = b.userId """.stripMargin
        
      val tree = compileSingle(rawInput)
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject where on static and dynamic provenances" in {
      val tree = compileSingle("//foo where new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject where on differing dynamic provenances" in {
      val tree = compileSingle("new 1 where new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject with on different loads" in {
      val tree = compileSingle("//foo with //bar")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject with on different relative loads" in {
      val tree = compileSingle("./foo with ./bar")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject with on static and dynamic provenances" in {
      val tree = compileSingle("//foo with new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject with on differing dynamic provenances" in {
      val tree = compileSingle("(new 1) with (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "accept union on different loads" in {
      val tree = compileSingle("//foo union //bar")
      tree.provenance must beLike { case CoproductProvenance(StaticProvenance("/foo"), StaticProvenance("/bar")) => ok }
      tree.errors must beEmpty
    }
    
    "accept union on different relative loads" in {
      val tree = compileSingle("./foo union ./bar")
      tree.provenance must beLike { case CoproductProvenance(StaticProvenance("foo"), StaticProvenance("bar")) => ok }
      tree.errors must beEmpty
    }
    
    "accept union on static and dynamic provenances" in {
      val tree = compileSingle("//foo union new 1")
      tree.provenance must beLike { case CoproductProvenance(StaticProvenance("/foo"), DynamicProvenance(_)) => ok }
      tree.errors must beEmpty
    }
    
    "accept union on differing dynamic provenances" in {
      val tree = compileSingle("(new 1) union (new 1)")
      tree.provenance must beLike { case CoproductProvenance(DynamicProvenance(_), DynamicProvenance(_)) => ok }
      tree.errors must beEmpty
    }
    
    "reject intersect on different loads" in {
      val tree = compileSingle("//foo intersect //bar")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(IntersectWithNoCommonalities)
    }
    
    "reject intersect on different relative loads" in {
      val tree = compileSingle("./foo intersect ./bar")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(IntersectProvenanceDifferentLength)
    }
    
    "accept intersect on static and dynamic provenances" in {
      val tree = compileSingle("//foo intersect new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(IntersectWithNoCommonalities)
    }
    
    "accept intersect on differing dynamic provenances" in {
      val tree = compileSingle("(new 1) intersect (new 1)")
      tree.provenance must beLike { case CoproductProvenance(DynamicProvenance(_), DynamicProvenance(_)) => ok }
      tree.errors must beEmpty
    }    

    "reject difference on different loads" in {
      val tree = compileSingle("//foo difference //bar")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(DifferenceWithNoCommonalities)
    }
    
    "reject difference on different relative loads" in {
      val tree = compileSingle("./foo difference ./bar")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(DifferenceWithNoCommonalities)
    }
    
    "reject difference on static and dynamic provenances" in {
      val tree = compileSingle("//foo difference new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(DifferenceWithNoCommonalities)
    }

    "reject difference on static and dynamic provenances" in {
      val tree = compileSingle("(new 1) difference //foo")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(DifferenceWithNoCommonalities)
    }
    
    "accept difference on differing dynamic provenances" in {
      val tree = compileSingle("(new 1) difference (new 1)")
      tree.provenance must beLike { case DynamicProvenance(_) => ok }
      tree.errors must beEmpty
    }
    
    "give left for difference with coproducts containing commonality" in {
      val tree = compileSingle("(//foo union //bar) difference (//bar union //baz)")
      tree.provenance must beLike {
        case CoproductProvenance(StaticProvenance("/foo"), StaticProvenance("/bar")) => ok
        case CoproductProvenance(StaticProvenance("/bar"), StaticProvenance("/foo")) => ok
      }
      tree.errors must beEmpty
    }

    "give null provenance for difference with unrelated coproducts" in {
      val tree = compileSingle("(//foo union //bar) difference (//baz union //qux)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(DifferenceWithNoCommonalities)
    }

    "give null provenance for difference with coproducts containing dynamic provenance" in {
      val tree = compileSingle("(//foo union //bar) difference ((new //baz) union //qux)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(DifferenceWithNoCommonalities)
    }

    // the compiler doesn't know that `new //foo` and `new //bar` have different identities,
    // while different instances of `new //foo` and `new //foo` have the same identities
    "give coproduct provenance for difference with coproducts containing dynamic provenance" in {
      val tree = compileSingle("(//foo union (new //bar)) difference ((new //baz) union //qux)")
      tree.provenance must beLike {
        case CoproductProvenance(StaticProvenance("/foo"), DynamicProvenance(_)) => ok
      }
      tree.errors must beEmpty
    }

    "propagate provenance through multiple let bindings" in {
      val tree @ Let(_, _, _, _, _) = compileSingle("back := //foo ~ //bar //foo + //bar back")
      
      tree.resultProvenance must beLike {
        case ProductProvenance(StaticProvenance("/foo"), StaticProvenance("/bar")) => ok
        case ProductProvenance(StaticProvenance("/bar"), StaticProvenance("/foo")) => ok
      }
      
      tree.resultProvenance.isParametric mustEqual false
      
      tree.provenance must beLike {
        case ProductProvenance(StaticProvenance("/foo"), StaticProvenance("/bar")) => ok
        case ProductProvenance(StaticProvenance("/bar"), StaticProvenance("/foo")) => ok
      }
      
      tree.errors must beEmpty
    }
    
    "accept union on product provenances" in {
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
        
      val tree = compileSingle(input)
      
      tree.provenance must beLike {
        case CoproductProvenance(ProductProvenance(StaticProvenance("/foo"), StaticProvenance("/bar")), ProductProvenance(StaticProvenance("/bar"), StaticProvenance("/baz"))) => ok
      }
      
      tree.errors must beEmpty
    }

    "reject intersect of two loaded sets" in {
      val input = """//clicks intersect //conversions"""
      val tree = compileSingle(input)

      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(IntersectWithNoCommonalities)
    }
    
    "reject difference of two loaded sets" in {
      val input = """//clicks difference //conversions"""
      val tree = compileSingle(input)

      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(DifferenceWithNoCommonalities)
    }
    
    "reject intersect on distinct product provenances" in {
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
        
      val tree = compileSingle(input)
      
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(IntersectWithNoCommonalities)
    }

    "accept intersect on distinct product provenances containing coproducts" in {
      val input = """
        | foo := //foo
        | bar := //bar
        | baz := //baz
        |
        | union1 := foo union bar
        | 
        | cross1 := union1 ~ baz union1 + baz
        | cross2 := foo ~ baz foo + baz
        |
        | cross1 intersect cross2
        | """.stripMargin
        
      val tree = compileSingle(input)
      
      tree.provenance mustEqual ProductProvenance(StaticProvenance("/foo"), StaticProvenance("/baz"))
      tree.errors must beEmpty
    }

    "accept intersect on distinct product provenances containing coproducts through dispatch" in {
      val input = """
        | foo := //foo
        | bar := //bar
        | baz := //baz
        |
        | g(a, b, c) :=
        |   union1 := a union b
        |   
        |   cross1 := union1 ~ c union1 + c
        |   cross2 := a ~ c a + c
        |
        |   cross1 intersect cross2
        |
        | g(foo, bar, baz)
        | """.stripMargin
        
      val tree = compileSingle(input)
      
      tree.provenance mustEqual ProductProvenance(StaticProvenance("/foo"), StaticProvenance("/baz"))
      tree.errors must beEmpty
    }

    "reject difference on distinct product provenances" in {
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
        
      val tree = compileSingle(input)
      
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(DifferenceWithNoCommonalities)
    }

    "accept intersect on common coproduct provenances" in {
      val input = """
        | foo := //foo
        | bar := //bar
        | baz := //baz
        | 
        | fb := foo ~ bar foo union bar
        | bb := bar ~ baz bar union baz
        | 
        | fb intersect bb
        | """.stripMargin
        
      val tree = compileSingle(input)
      
      tree.provenance mustEqual StaticProvenance("/bar")
      tree.errors must beEmpty
    }

    "accept difference on common coproduct provenances" in {
      val input = """
        | foo := //foo
        | bar := //bar
        | baz := //baz
        | 
        | fb := foo ~ bar foo union bar
        | bb := bar ~ baz bar union baz
        | 
        | fb difference bb
        | """.stripMargin
        
      val tree = compileSingle(input)
      
      // in this case we are "removing" all of `bar` from `fb`
      // but the provenance alone does not tell us this
      tree.provenance mustEqual CoproductProvenance(StaticProvenance("/foo"), StaticProvenance("/bar"))
      tree.errors must beEmpty
    }
    
    "reject difference on distinct product provenances" in {
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
        
      val tree = compileSingle(input)
      
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(DifferenceWithNoCommonalities)
    }
    
    "accept union through a function on static provenances" in {
      val input = """
        | f(a, b) := a union b
        | 
        | f(//foo, //bar)
        | """.stripMargin
      
      val tree = compileSingle(input)
      tree.provenance must beLike { case CoproductProvenance(StaticProvenance("/foo"), StaticProvenance("/bar")) => ok }
      tree.errors must beEmpty
    }
    
    "accept union and operator through a function on static provenances" in {
      val input = """
        | f(a, b) := (a union b) + a
        |
        | f(//foo, //bar)
        | """.stripMargin

      val tree = compileSingle(input)
      tree.provenance must beLike { case StaticProvenance("/foo") => ok }
      tree.errors must beEmpty
    }

    "reject intersect through a function on static provenance" in {
      val input = """
        | f(a, b) := a intersect b
        | 
        | f(//foo, //bar)
        | """.stripMargin
      
      val tree = compileSingle(input)
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(IntersectWithNoCommonalities)
    }

    "reject if-then-else  through a function on static provenance" in {
      val input = """
        | f(a, b) := if true then a else b
        | 
        | f(//foo, 5)
        | """.stripMargin
      
      val tree = compileSingle(input)
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(CondProvenanceDifferentLength)
    }
    
    "accept intersect of same provenances and operator through a function on static provenance" in {
      val input = """
        | f(a, b) := (a intersect b) + a
        |
        | f(//foo, //foo)
        | """.stripMargin

      val tree = compileSingle(input)
      tree.provenance mustEqual StaticProvenance("/foo")
      tree.errors must beEmpty
    }

    "accept intersect and operator through a function on static provenance" in {
      val input = """
        | f(a, b) := (a intersect b) + a
        |
        | f(//foo, //bar)
        | """.stripMargin

      val tree = compileSingle(input)
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(IntersectWithNoCommonalities)
    }

    "accept difference of same provenance through a function on static provenances" in {
      val input = """
        | f(a, b) := a difference b
        | 
        | f(//foo, //foo)
        | """.stripMargin
      
      // we cannot tell statically that this is the empty set
      val tree = compileSingle(input)
      tree.provenance mustEqual StaticProvenance("/foo")
      tree.errors must beEmpty
    }
    
    "accept difference of different provenances through a function on static provenances" in {
      val input = """
        | f(a, b) := a difference b
        | 
        | f(//foo, //bar)
        | """.stripMargin
      
      val tree = compileSingle(input)
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(DifferenceWithNoCommonalities)
    }
    
    "accept union through a function on product provenances" in {
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
        
      val tree = compileSingle(input)

      tree.provenance must beLike {
        case CoproductProvenance(
          ProductProvenance(StaticProvenance("/foo"), StaticProvenance("/bar")), 
          ProductProvenance(StaticProvenance("/bar"), StaticProvenance("/baz"))) => ok
      }
      
      tree.errors must beEmpty
    }
    
    "accept intersect through a function on product provenances" in {
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
        
      val tree = compileSingle(input)
      
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(IntersectWithNoCommonalities)
    }
    
    "accept difference through a function on product provenances" in {
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
        
      val tree = compileSingle(input)
      
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(DifferenceWithNoCommonalities)
    }
    
    "reject union on product provenance with static provenance" in {
      val input = """
        | foo := //foo
        | bar := //bar
        | 
        | fb := foo ~ bar foo + bar
        | 
        | fb union //baz
        | """.stripMargin
        
      val tree = compileSingle(input)
      
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(UnionProvenanceDifferentLength)
    }
    
    "reject intersect on product provenance with static provenance" in {
      val input = """
        | foo := //foo
        | bar := //bar
        | baz := //baz
        | 
        | fb := foo ~ bar foo + bar
        | 
        | fb intersect baz
        | """.stripMargin
        
      val tree = compileSingle(input)
      
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(IntersectProvenanceDifferentLength)
    }
    
    "reject difference on product provenance with static provenance" in {
      val input = """
        | foo := //foo
        | bar := //bar
        | 
        | fb := foo ~ bar foo + bar
        | 
        | fb difference //baz
        | """.stripMargin
        
      val tree = compileSingle(input)
      
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(DifferenceProvenanceDifferentLength)
    }
    
    "reject union through a function on product provenances" in {
      val input = """
        | foo := //foo
        | bar := //bar
        | baz := //baz
        | 
        | fb := foo ~ bar foo + bar
        | 
        | f(a, b) := a union b
        | f(fb, baz)
        | """.stripMargin
        
      val tree = compileSingle(input)
      
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(UnionProvenanceDifferentLength)
    }
    
    "reject intersect through a function on product provenances" in {
      val input = """
        | foo := //foo
        | bar := //bar
        | baz := //baz
        | 
        | fb := foo ~ bar foo + bar
        | 
        | f(a, b) := a intersect b
        | f(fb, baz)
        | """.stripMargin
        
      val tree = compileSingle(input)
      
      tree.provenance mustEqual NullProvenance
      // both errors are propogated through here because we're handling parametric provenances
      tree.errors mustEqual Set(IntersectProvenanceDifferentLength, IntersectWithNoCommonalities)
    }
    
    "reject difference through a function on product provenances" in {
      val input = """
        | foo := //foo
        | bar := //bar
        | baz := //baz
        | 
        | fb := foo ~ bar foo + bar
        | 
        | f(a, b) := a difference b
        | f(fb, baz)
        | """.stripMargin
        
      val tree = compileSingle(input)
      
      tree.provenance mustEqual NullProvenance
      // both errors are propogated through here because we're handling parametric provenances
      tree.errors mustEqual Set(DifferenceProvenanceDifferentLength, DifferenceWithNoCommonalities)
    }

    "reject addition on different loads" in {
      val tree = compileSingle("//foo + //bar")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject addition on different relative loads" in {
      val tree = compileSingle("./foo + ./bar")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject addition on static and dynamic provenances" in {
      val tree = compileSingle("//foo + new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject addition on differing dynamic provenances" in {
      val tree = compileSingle("(new 1) + (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject subtraction on different loads" in {
      val tree = compileSingle("//foo - //bar")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject subtraction on different relative loads" in {
      val tree = compileSingle("./foo - ./bar")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject subtraction on static and dynamic provenances" in {
      val tree = compileSingle("//foo - new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject subtraction on differing dynamic provenances" in {
      val tree = compileSingle("(new 1) - (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject multiplication on different loads" in {
      val tree = compileSingle("//foo * //bar")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject multiplication on different relative loads" in {
      val tree = compileSingle("./foo * ./bar")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject multiplication on static and dynamic provenances" in {
      val tree = compileSingle("//foo * new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject multiplication on differing dynamic provenances" in {
      val tree = compileSingle("(new 1) * (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject division on different loads" in {
      val tree = compileSingle("//foo / //bar")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject division on different relative loads" in {
      val tree = compileSingle("./foo / ./bar")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject division on static and dynamic provenances" in {
      val tree = compileSingle("//foo / new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject division on differing dynamic provenances" in {
      val tree = compileSingle("(new 1) / (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject mod on different loads" in {
      val tree = compileSingle("//foo % //bar")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject mod on different relative loads" in {
      val tree = compileSingle("./foo % ./bar")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject mod on static and dynamic provenances" in {
      val tree = compileSingle("//foo % new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject mod on differing dynamic provenances" in {
      val tree = compileSingle("(new 1) % (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject less-than on different loads" in {
      val tree = compileSingle("//foo < //bar")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject less-than on different relative loads" in {
      val tree = compileSingle("./foo < ./bar")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject less-than on static and dynamic provenances" in {
      val tree = compileSingle("//foo < new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject less-than on differing dynamic provenances" in {
      val tree = compileSingle("(new 1) < (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject less-than-equal on different loads" in {
      val tree = compileSingle("//foo <= //bar")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject less-than-equal on different relative loads" in {
      val tree = compileSingle("./foo <= ./bar")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject less-than-equal on static and dynamic provenances" in {
      val tree = compileSingle("//foo <= new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject less-than-equal on differing dynamic provenances" in {
      val tree = compileSingle("(new 1) <= (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject greater-than on different loads" in {
      val tree = compileSingle("//foo > //bar")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject greater-than on different loads" in {
      val tree = compileSingle("./foo > ./bar")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject greater-than on static and dynamic provenances" in {
      val tree = compileSingle("//foo > new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject greater-than on differing dynamic provenances" in {
      val tree = compileSingle("(new 1) > (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject greater-than-equal on different loads" in {
      val tree = compileSingle("//foo >= //bar")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject greater-than-equal on different relative loads" in {
      val tree = compileSingle("./foo >= ./bar")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject greater-than-equal on static and dynamic provenances" in {
      val tree = compileSingle("//foo >= new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject greater-than-equal on differing dynamic provenances" in {
      val tree = compileSingle("(new 1) >= (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject equality on different loads" in {
      val tree = compileSingle("//foo = //bar")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject equality on different relative loads" in {
      val tree = compileSingle("./foo = ./bar")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject equality on static and dynamic provenances" in {
      val tree = compileSingle("//foo = new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject equality on differing dynamic provenances" in {
      val tree = compileSingle("(new 1) = (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject not-equality on different loads" in {
      val tree = compileSingle("//foo != //bar")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject not-equality on different relative loads" in {
      val tree = compileSingle("./foo != ./bar")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject not-equality on static and dynamic provenances" in {
      val tree = compileSingle("//foo != new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject not-equality on differing dynamic provenances" in {
      val tree = compileSingle("(new 1) != (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject boolean and on different loads" in {
      val tree = compileSingle("//foo & //bar")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject boolean and on different relative loads" in {
      val tree = compileSingle("./foo & ./bar")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject boolean and on static and dynamic provenances" in {
      val tree = compileSingle("//foo & new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject boolean and on differing dynamic provenances" in {
      val tree = compileSingle("(new 1) & (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject boolean or on different loads" in {
      val tree = compileSingle("//foo | //bar")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject boolean or on different relative loads" in {
      val tree = compileSingle("./foo | ./bar")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject boolean or on static and dynamic provenances" in {
      val tree = compileSingle("//foo | new 1")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }
    
    "reject boolean or on differing dynamic provenances" in {
      val tree = compileSingle("(new 1) | (new 1)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set(OperationOnUnrelatedSets)
    }

    // Regression test for #39656435
    "accept SnapEngage query" in {
      val input = """
        | agents := //snapEngage/agents
        |
        | getEvents(agent) :=
        |   firstData := agents where agents.agentId = agent
        |   previousEvents := firstData where firstData.millis < 100
        |   {a: firstData, b: previousEvents}
        |
        | getEvents("agent1") """.stripMargin

      val tree = compileSingle(input)
      tree.errors must beEmpty
    }
  }
}
