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

import bytecode.StaticLibrary
import com.codecommit.gll.LineStream
import org.specs2.ScalaCheck
import org.specs2.mutable.Specification
import parser._

import java.io.File
import scala.io.Source

object BinderSpecs extends Specification with ScalaCheck with Parser with StubPhases with Binder with StaticLibrary {
  import ast._
  
  "let binding" should {
    "bind name in resulting scope" in {
      val e @ Let(_, _, _, _, d: Dispatch) = parse("a := 42 a")
      d.binding mustEqual LetBinding(e)
      d.isReduction mustEqual false
      d.errors must beEmpty
    }
    
    "not bind name in expression scope" in {
      val Let(_, _, _, left: Dispatch, _) = parse("a := a 42")
      left.binding mustEqual NullBinding
      left.isReduction mustEqual false
      left.errors mustEqual Set(UndefinedFunction(Identifier(Vector(), "a")))
    }
    
    "bind all tic-variables in expression scope" in {
      {
        val e @ Let(_, _, _, t: TicVar, _) = parse("a('b) := 'b a")
        t.binding mustEqual LetBinding(e)
        t.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, Add(_, Add(_, tb: TicVar, tc: TicVar), td: TicVar), _) = parse("a('b, 'c, 'd) := 'b + 'c + 'd a")
        
        tb.binding mustEqual LetBinding(e)
        tc.binding mustEqual LetBinding(e)
        td.binding mustEqual LetBinding(e)
        
        tb.errors must beEmpty
        tc.errors must beEmpty
        td.errors must beEmpty
      }
    }
    
    "not bind tic-variable in resulting scope" in {
      val Let(_, _, _, _, t: TicVar) = parse("a('b) := 42 'b")
      t.binding mustEqual NullBinding
      t.errors mustEqual Set(UndefinedTicVariable("'b"))
    }
    
    "bind name in inner scope" in {
      val e1 @ Let(_, _, _, _, e2 @ Let(_, _, _, _, d: Dispatch)) = parse("a := 42 b := 24 b")
      
      d.binding mustEqual LetBinding(e2)
      d.isReduction mustEqual false
      d.errors must beEmpty
    }
    
    "bind tic-variable in inner scope" in {
      val e1 @ Let(_, _, _, _, e2 @ Let(_, _, _, t: TicVar, _)) = parse("a := 42 b('b) := 'b 24")
      
      t.binding mustEqual LetBinding(e2)
      t.errors must beEmpty
    }
    
    "reject unbound dispatch" in {
      {
        val d @ Dispatch(_, _, _) = parse("foo")
        d.binding mustEqual NullBinding
        d.isReduction mustEqual false
        d.errors mustEqual Set(UndefinedFunction(Identifier(Vector(), "foo")))
      }
      
      {
        val d @ Dispatch(_, _, _) = parse("foo(2, 1, 4)")
        d.binding mustEqual NullBinding
        d.isReduction mustEqual false
        d.errors mustEqual Set(UndefinedFunction(Identifier(Vector(), "foo")))
      }
    }

    "reject unbound dispatch with a namespace" in {
      {
        val d @ Dispatch(_, _, _) = parse("foo::bar::baz")
        d.binding mustEqual NullBinding
        d.isReduction mustEqual false
        d.errors mustEqual Set(UndefinedFunction(Identifier(Vector("foo", "bar"), "baz")))
      }
      
      {
        val d @ Dispatch(_, _, _) = parse("foo::bar::baz(7, 8, 9)")
        d.binding mustEqual NullBinding
        d.isReduction mustEqual false
        d.errors mustEqual Set(UndefinedFunction(Identifier(Vector("foo", "bar"), "baz")))
      }
    } 
    
    "reject unbound tic-variables" in {
      {
        val d @ TicVar(_, _) = parse("'foo")
        d.binding mustEqual NullBinding
        d.errors mustEqual Set(UndefinedTicVariable("'foo"))
      }
      
      {
        val d @ TicVar(_, _) = parse("'bar")
        d.binding mustEqual NullBinding
        d.errors mustEqual Set(UndefinedTicVariable("'bar"))
      }
      
      {
        val d @ TicVar(_, _) = parse("'baz")
        d.binding mustEqual NullBinding
        d.errors mustEqual Set(UndefinedTicVariable("'baz"))
      }
    }

    "allow a case when a tic variable is not solvable in all cases" in {
      {
        val tree = parse("""
        | a('b) :=
        |   k := //clicks.time where //clicks.time = 'b
        |   j := //views.time where //views.time > 'b
        |   k ~ j
        |   {kay: k, jay: j}
        | a""".stripMargin)

        tree.errors must beEmpty
      }
    }
    
    "reject multiple definitions of tic-variables" in {
      {
        val tree = parse("f('a, 'a) := 1 2")
        tree.errors mustEqual Set(MultiplyDefinedTicVariable("'a"))
      }
      
      {
        val tree = parse("f('a, 'b, 'c, 'a) := 1 2")
        tree.errors mustEqual Set(MultiplyDefinedTicVariable("'a"))
      }
      
      {
        val tree = parse("f('a, 'b, 'c, 'a, 'b, 'a) := 1 2")
        tree.errors mustEqual Set(MultiplyDefinedTicVariable("'a"), MultiplyDefinedTicVariable("'b"))
      }
    }
    
    "not leak dispatch into an adjacent scope" in {
      val Add(_, _, d: Dispatch) = parse("(a := 1 2) + a")
      d.binding mustEqual NullBinding
      d.isReduction mustEqual false
      d.errors mustEqual Set(UndefinedFunction(Identifier(Vector(), "a")))
    }
    
    "not leak tic-variable into an adjacent scope" in {
      val Add(_, _, t: TicVar) = parse("(a('b) := 1 2) + 'b")
      t.binding mustEqual NullBinding
      t.errors mustEqual Set(UndefinedTicVariable("'b"))
    }
    
    "allow shadowing of user-defined bindings" in {
      val Let(_, _, _, _, e @ Let(_, _, _, _, d: Dispatch)) = parse("a := 1 a := 2 a")
      d.binding mustEqual LetBinding(e)
      d.isReduction mustEqual false
      d.errors must beEmpty
    }
    
    "allow shadowing of tic-variables" in {
      val Let(_, _, _, _, e @ Let(_, _, _, t: TicVar, _)) = parse("a('c) := 1 b('c) := 'c 2")
      t.binding mustEqual LetBinding(e)
      t.errors must beEmpty
    }
    
    "allow shadowing of built-in bindings" in {
      {
        val e @ Let(_, _, _, _, d: Dispatch) = parse("count := 1 count")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, d: Dispatch) = parse("geometricMean := 1 geometricMean")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, d: Dispatch) = parse("load := 1 load")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, d: Dispatch) = parse("max := 1 max")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, d: Dispatch) = parse("mean := 1 mean")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, d: Dispatch) = parse("median := 1 median")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, d: Dispatch) = parse("mean := 1 mean")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, d: Dispatch) = parse("min := 1 min")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, d: Dispatch) = parse("mode := 1 mode")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, d: Dispatch) = parse("stdDev := 1 stdDev")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, d: Dispatch) = parse("sum := 1 sum")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }      

      {
        val e @ Let(_, _, _, _, d: Dispatch) = parse("sumSq := 1 sumSq")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, d: Dispatch) = parse("variance := 1 variance")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, d: Dispatch) = parse("distinct := 1 distinct")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "not leak shadowing into an adjacent scope" in {
      val e @ Let(_, _, _, _, Add(_, _, d: Dispatch)) = parse("a := 1 (a := 2 3) + a")
      d.binding mustEqual LetBinding(e)
      d.isReduction mustEqual false
      d.errors must beEmpty
    }
  }
  
  "inherited scoping" should {
    "forward direct binding" in {
      val e @ Let(_, _, _, _, d: Dispatch) = parse("a := 42 a")
      d.binding mustEqual LetBinding(e)
      d.isReduction mustEqual false
      d.errors must beEmpty
    }
    
    "forward binding through let" in {
      val e1 @ Let(_, _, _, _, e2 @ Let(_, _, _, d1: Dispatch, d2: Dispatch)) = parse("a := 42 b := a a")
      
      d1.binding mustEqual LetBinding(e1)
      d1.isReduction mustEqual false
      d1.errors must beEmpty
      
      d2.binding mustEqual LetBinding(e1)
      d2.isReduction mustEqual false
      d2.errors must beEmpty
    }
    
    "forward binding through new" in {
      val e @ Let(_, _, _, _, New(_, d: Dispatch)) = parse("a := 42 new a")
      d.binding mustEqual LetBinding(e)
      d.isReduction mustEqual false
      d.errors must beEmpty
    }
    
    "forward binding through relate" in {
      {
        val e @ Let(_, _, _, _, Relate(_, d: Dispatch, _, _)) = parse("a := 42 a ~ 1 2")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, Relate(_, _, d: Dispatch, _)) = parse("a := 42 1 ~ a 2")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, Relate(_, _, _, d: Dispatch)) = parse("a := 42 1 ~ 2 a")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "forward binding through object definition" in {
      val e @ Let(_, _, _, _, ObjectDef(_, Vector((_, d: Dispatch)))) = parse("a := 42 { a: a }")
      d.binding mustEqual LetBinding(e)
      d.isReduction mustEqual false
      d.errors must beEmpty
    }
    
    "forward binding through array definition" in {
      val e @ Let(_, _, _, _, ArrayDef(_, Vector(d: Dispatch))) = parse("a := 42 [a]")
      d.binding mustEqual LetBinding(e)
      d.isReduction mustEqual false
      d.errors must beEmpty
    }
    
    "forward binding through descent" in {
      val e @ Let(_, _, _, _, Descent(_, d: Dispatch, _)) = parse("a := 42 a.b")
      d.binding mustEqual LetBinding(e)
      d.isReduction mustEqual false
      d.errors must beEmpty
    }
    
    "forward binding through metadescent" in {
      val e @ Let(_, _, _, _, MetaDescent(_, d: Dispatch, _)) = parse("a := 42 a@b")
      d.binding mustEqual LetBinding(e)
      d.isReduction mustEqual false
      d.errors must beEmpty
    }
    
    "forward binding through dereference" in {
      val e @ Let(_, _, _, _, Deref(_, _, d: Dispatch)) = parse("a := 42 1[a]")
      d.binding mustEqual LetBinding(e)
      d.isReduction mustEqual false
      d.errors must beEmpty
    }
    
    "forward binding through dispatch" in {
      forall(libReduction) { f => 
        val e @ Let(_, _, _, _, Dispatch(_, _, Vector(d: Dispatch))) = parse("a := 42 %s(a)".format(f.fqn))
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "forward binding through where" in {
      {
        val e @ Let(_, _, _, _, Where(_, d: Dispatch, _)) = parse("a := 42 a where 1")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, Where(_, _, d: Dispatch)) = parse("a := 42 1 where a")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "forward binding through with" in {
      {
        val e @ Let(_, _, _, _, With(_, d: Dispatch, _)) = parse("a := 42 a with 1")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, With(_, _, d: Dispatch)) = parse("a := 42 1 with a")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "forward binding through union" in {
      {
        val e @ Let(_, _, _, _, Union(_, d: Dispatch, _)) = parse("a := 42 a union 1")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, Union(_, _, d: Dispatch)) = parse("a := 42 1 union a")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "forward binding through intersect" in {
      {
        val e @ Let(_, _, _, _, Intersect(_, d: Dispatch, _)) = parse("a := 42 a intersect 1")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, Intersect(_, _, d: Dispatch)) = parse("a := 42 1 intersect a")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "forward binding through difference" in {
      {
        val e @ Let(_, _, _, _, Difference(_, d: Dispatch, _)) = parse("a := 42 a difference 1")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, Difference(_, _, d: Dispatch)) = parse("a := 42 1 difference a")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "forward binding through addition" in {
      {
        val e @ Let(_, _, _, _, Add(_, d: Dispatch, _)) = parse("a := 42 a + 1")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, Add(_, _, d: Dispatch)) = parse("a := 42 1 + a")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "forward binding through subtraction" in {
      {
        val e @ Let(_, _, _, _, Sub(_, d: Dispatch, _)) = parse("a := 42 a - 1")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, Sub(_, _, d: Dispatch)) = parse("a := 42 1 - a")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "forward binding through multiplication" in {
      {
        val e @ Let(_, _, _, _, Mul(_, d: Dispatch, _)) = parse("a := 42 a * 1")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, Mul(_, _, d: Dispatch)) = parse("a := 42 1 * a")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "forward binding through division" in {
      {
        val e @ Let(_, _, _, _, Div(_, d: Dispatch, _)) = parse("a := 42 a / 1")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, Div(_, _, d: Dispatch)) = parse("a := 42 1 / a")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "forward binding through less-than" in {
      {
        val e @ Let(_, _, _, _, Lt(_, d: Dispatch, _)) = parse("a := 42 a < 1")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, Lt(_, _, d: Dispatch)) = parse("a := 42 1 < a")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "forward binding through less-than-equal" in {
      {
        val e @ Let(_, _, _, _, LtEq(_, d: Dispatch, _)) = parse("a := 42 a <= 1")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, LtEq(_, _, d: Dispatch)) = parse("a := 42 1 <= a")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "forward binding through greater-than" in {
      {
        val e @ Let(_, _, _, _, Gt(_, d: Dispatch, _)) = parse("a := 42 a > 1")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, Gt(_, _, d: Dispatch)) = parse("a := 42 1 > a")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "forward binding through greater-than-equal" in {
      {
        val e @ Let(_, _, _, _, GtEq(_, d: Dispatch, _)) = parse("a := 42 a >= 1")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, GtEq(_, _, d: Dispatch)) = parse("a := 42 1 >= a")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "forward binding through equality" in {
      {
        val e @ Let(_, _, _, _, Eq(_, d: Dispatch, _)) = parse("a := 42 a = 1")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, Eq(_, _, d: Dispatch)) = parse("a := 42 1 = a")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "forward binding through not-equality" in {
      {
        val e @ Let(_, _, _, _, NotEq(_, d: Dispatch, _)) = parse("a := 42 a != 1")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, NotEq(_, _, d: Dispatch)) = parse("a := 42 1 != a")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "forward binding through boolean and" in {
      {
        val e @ Let(_, _, _, _, And(_, d: Dispatch, _)) = parse("a := 42 a & 1")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, And(_, _, d: Dispatch)) = parse("a := 42 1 & a")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "forward binding through boolean or" in {
      {
        val e @ Let(_, _, _, _, Or(_, d: Dispatch, _)) = parse("a := 42 a | 1")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, Or(_, _, d: Dispatch)) = parse("a := 42 1 | a")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "forward binding through complement" in {
      val e @ Let(_, _, _, _, Comp(_, d: Dispatch)) = parse("a := 42 !a")
      d.binding mustEqual LetBinding(e)
      d.isReduction mustEqual false
      d.errors must beEmpty
    }
    
    "forward binding through negation" in {
      val e @ Let(_, _, _, _, Neg(_, d: Dispatch)) = parse("a := 42 neg a")
      d.binding mustEqual LetBinding(e)
      d.isReduction mustEqual false
      d.errors must beEmpty
    }
    
    "forward binding through parentheses" in {
      val e @ Let(_, _, _, _, Paren(_, d: Dispatch)) = parse("a := 42 (a)")
      d.binding mustEqual LetBinding(e)
      d.isReduction mustEqual false
      d.errors must beEmpty
    }
  }
  
  "pre-binding of load and distinct" should {
    "bind load" in {
      val d @ Dispatch(_, _, _) = parse("load")
      d.binding mustEqual LoadBinding(Identifier(Vector(), "load"))
      d.isReduction mustEqual false
      d.errors must beEmpty
    }
    
    "bind distinct" in {
      val d @ Dispatch(_, _, _) = parse("distinct")
      d.binding mustEqual DistinctBinding(Identifier(Vector(), "distinct"))
      d.isReduction mustEqual false
      d.errors must beEmpty
    }
  }

  "pre-binding of built-in functions" should {
    "bind unary morphisms" in {
      libMorphism1 must not(beEmpty)

      forall(libMorphism1) { f =>
        val d @ Dispatch(_, _, _) = parse(f.fqn)
        d.binding mustEqual Morphism1Binding(f)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }    

    "bind binary morphisms" in {
      libMorphism2 must not(beEmpty)

      forall(libMorphism2) { f =>
        val d @ Dispatch(_, _, _) = parse(f.fqn)
        d.binding mustEqual Morphism2Binding(f)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }

    "bind reductions" in {
      libReduction must not(beEmpty)

      forall(libReduction) { f =>
        val d @ Dispatch(_, _, _) = parse(f.fqn)
        d.binding mustEqual ReductionBinding(f)
        d.isReduction mustEqual true
        d.errors must beEmpty
      }
    }

    "bind unary functions" in {
      forall(lib1) { f =>
        val d @ Dispatch(_, _, _) = parse(f.fqn)
        d.binding mustEqual Op1Binding(f)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }

    "bind binary functions" in {
      forall(lib2) { f =>
        val d @ Dispatch(_, _, _) = parse(f.fqn)
        d.binding mustEqual Op2Binding(f)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
  }
  
  "complete hierarchical imports" should {
    "allow use of a unary function unqualified" in {
      val input = """
        | import std::lib::baz
        | baz""".stripMargin
        
      val Import(_, _, d: Dispatch) = parse(input)
      
      d.binding mustEqual Op1Binding(Op1(Vector("std", "lib"), "baz", 0x0003))
      d.errors must beEmpty
    }
    
    "allow use of a unary function unqualified in a hierarchical import" in {
      val input = """
        | import std::lib
        | import lib::baz
        | baz""".stripMargin
        
      val Import(_, _, Import(_, _, d: Dispatch)) = parse(input)
      
      d.binding mustEqual Op1Binding(Op1(Vector("std", "lib"), "baz", 0x0003))
      d.errors must beEmpty
    }
    
    "allow the use of a unary function partially-qualified" in {
      val input = """
        | import std::lib
        | lib::baz""".stripMargin
        
      val Import(_, _, d: Dispatch) = parse(input)
      
      d.binding mustEqual Op1Binding(Op1(Vector("std", "lib"), "baz", 0x0003))
      d.errors must beEmpty
    }
    
    "reject the use of a sub-package when parent has been singularly imported" in {
      val input = """
        | import std
        | import lib
        | lib::baz""".stripMargin
        
      val Import(_, _, Import(_, _, d: Dispatch)) = parse(input)
      
      d.errors must not(beEmpty)
    }
    
    "allow the use of a function that shadows a package" in {
      val input = """
        | import std::lib
        | lib""".stripMargin
        
      val Import(_, _, d: Dispatch) = parse(input)
      
      d.binding mustEqual Op1Binding(Op1(Vector("std"), "lib", 0x0004))
      d.errors must beEmpty
    }
    
    "bind most specific import in case of shadowing" in {
      val input = """
        | import std::bin
        | bin""".stripMargin
        
      val Import(_, _, d: Dispatch) = parse(input)
      
      d.binding mustEqual Op1Binding(Op1(Vector("std"), "bin", 0x0001))
      d.errors must beEmpty
    }
    
    "not affect outer scope" in {
      val input = """
        | bin +
        | import std::bin
        | bin""".stripMargin
        
      val Add(_, d1: Dispatch, Import(_, _, d2: Dispatch)) = parse(input)
      
      d1.binding mustEqual Op1Binding(Op1(Vector(), "bin", 0x0000))
      d1.errors must beEmpty
      
      d2.binding mustEqual Op1Binding(Op1(Vector("std"), "bin", 0x0001))
      d2.errors must beEmpty
    }
  }
  
  "wildcard hierarchical imports" should {
    "allow use of a unary function unqualified" in {
      val input = """
        | import std::lib::*
        | baz""".stripMargin
        
      val Import(_, _, d: Dispatch) = parse(input)
      
      d.binding mustEqual Op1Binding(Op1(Vector("std", "lib"), "baz", 0x0003))
      d.errors must beEmpty
    }
    
    "allow use of more than one unary function unqualified" in {
      val input = """
        | import std::lib::*
        | baz + baz2""".stripMargin
        
      val Import(_, _, Add(_, d1: Dispatch, d2: Dispatch)) = parse(input)
      
      d1.binding mustEqual Op1Binding(Op1(Vector("std", "lib"), "baz", 0x0003))
      d1.errors must beEmpty
      
      d2.binding mustEqual Op2Binding(Op2(Vector("std", "lib"), "baz2", 0x0003))
      d2.errors must beEmpty
    }
    
    "allow use of a unary function unqualified in a hierarchical import" in {
      val input = """
        | import std::*
        | import lib::*
        | baz""".stripMargin
        
      val Import(_, _, Import(_, _, d: Dispatch)) = parse(input)
      
      d.binding mustEqual Op1Binding(Op1(Vector("std", "lib"), "baz", 0x0003))
      d.errors must beEmpty
    }
    
    "allow the use of a unary function partially-qualified" in {
      val input = """
        | import std::*
        | lib::baz""".stripMargin
        
      val Import(_, _, d: Dispatch) = parse(input)
      
      d.binding mustEqual Op1Binding(Op1(Vector("std", "lib"), "baz", 0x0003))
      d.errors must beEmpty
    }
    
    "allow the use of a function that shadows a package" in {
      val input = """
        | import std::*
        | lib""".stripMargin
        
      val Import(_, _, d: Dispatch) = parse(input)
      
      d.binding mustEqual Op1Binding(Op1(Vector("std"), "lib", 0x0004))
      d.errors must beEmpty
    }
    
    "bind most specific import in case of shadowing" in {
      val input = """
        | import std::*
        | bin""".stripMargin
        
      val Import(_, _, d: Dispatch) = parse(input)
      
      d.binding mustEqual Op1Binding(Op1(Vector("std"), "bin", 0x0001))
      d.errors must beEmpty
    }
    
    "not affect outer scope" in {
      val input = """
        | bin +
        | import std::*
        | bin""".stripMargin
        
      val Add(_, d1: Dispatch, Import(_, _, d2: Dispatch)) = parse(input)
      
      d1.binding mustEqual Op1Binding(Op1(Vector(), "bin", 0x0000))
      d1.errors must beEmpty
      
      d2.binding mustEqual Op1Binding(Op1(Vector("std"), "bin", 0x0001))
      d2.errors must beEmpty
    }
  }
  
  val exampleDir = new File("quirrel/examples")
  
  if (exampleDir.exists) {
    "specification examples" >> {
      for (file <- exampleDir.listFiles if file.getName endsWith ".qrl") {
        file.getName >> {
          val result = parse(LineStream(Source.fromFile(file)))
          result.errors must beEmpty
        }
      }
    }
  } else {
    "specification examples" >> skipped
  }
}
