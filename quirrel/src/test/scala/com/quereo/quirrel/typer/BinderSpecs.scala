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
import org.specs2.ScalaCheck
import org.specs2.mutable.Specification
import parser._

object BinderSpecs extends Specification with ScalaCheck with Parser with StubPhases with Binder {
  
  "let binding" should {
    "bind name in resulting scope" in {
      val e @ Let(_, _, _, _, d: Dispatch) = parse("a := 42 a")
      d.binding mustEqual UserDef(e)
      d.isReduction mustEqual false
      d.errors must beEmpty
    }
    
    "not bind name in expression scope" in {
      val Let(_, _, _, left: Dispatch, _) = parse("a := a 42")
      left.binding mustEqual NullBinding
      left.isReduction mustEqual false
      left.errors mustEqual Set(UndefinedFunction("a"))
    }
    
    "bind all tic-variables in expression scope" in {
      {
        val e @ Let(_, _, _, t: TicVar, _) = parse("a('b) := 'b a")
        t.binding mustEqual UserDef(e)
        t.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, Add(_, Add(_, tb: TicVar, tc: TicVar), td: TicVar), _) = parse("a('b, 'c, 'd) := 'b + 'c + 'd a")
        
        tb.binding mustEqual UserDef(e)
        tc.binding mustEqual UserDef(e)
        td.binding mustEqual UserDef(e)
        
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
      
      d.binding mustEqual UserDef(e2)
      d.isReduction mustEqual false
      d.errors must beEmpty
    }
    
    "bind tic-variable in inner scope" in {
      val e1 @ Let(_, _, _, _, e2 @ Let(_, _, _, t: TicVar, _)) = parse("a := 42 b('b) := 'b 24")
      
      t.binding mustEqual UserDef(e2)
      t.errors must beEmpty
    }
    
    "reject unbound dispatch" in {
      {
        val d @ Dispatch(_, _, _) = parse("foo")
        d.binding mustEqual NullBinding
        d.isReduction mustEqual false
        d.errors mustEqual Set(UndefinedFunction("foo"))
      }
      
      {
        val d @ Dispatch(_, _, _) = parse("foo(2, 1, 4)")
        d.binding mustEqual NullBinding
        d.isReduction mustEqual false
        d.errors mustEqual Set(UndefinedFunction("foo"))
      }
      
      {
        val d @ Dispatch(_, _, _) = parse("bar")
        d.binding mustEqual NullBinding
        d.isReduction mustEqual false
        d.errors mustEqual Set(UndefinedFunction("bar"))
      }
      
      {
        val d @ Dispatch(_, _, _) = parse("baz")
        d.binding mustEqual NullBinding
        d.isReduction mustEqual false
        d.errors mustEqual Set(UndefinedFunction("baz"))
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
    
    "not leak dispatch into an adjacent scope" in {
      val Add(_, _, d: Dispatch) = parse("(a := 1 2) + a")
      d.binding mustEqual NullBinding
      d.isReduction mustEqual false
      d.errors mustEqual Set(UndefinedFunction("a"))
    }
    
    "not leak tic-variable into an adjacent scope" in {
      val Add(_, _, t: TicVar) = parse("(a('b) := 1 2) + 'b")
      t.binding mustEqual NullBinding
      t.errors mustEqual Set(UndefinedTicVariable("'b"))
    }
    
    "allow shadowing of user-defined bindings" in {
      val Let(_, _, _, _, e @ Let(_, _, _, _, d: Dispatch)) = parse("a := 1 a := 2 a")
      d.binding mustEqual UserDef(e)
      d.isReduction mustEqual false
      d.errors must beEmpty
    }
    
    "allow shadowing of tic-variables" in {
      val Let(_, _, _, _, e @ Let(_, _, _, t: TicVar, _)) = parse("a('c) := 1 b('c) := 'c 2")
      t.binding mustEqual UserDef(e)
      t.errors must beEmpty
    }
    
    "allow shadowing of built-in bindings" in {
      {
        val e @ Let(_, _, _, _, d: Dispatch) = parse("count := 1 count")
        d.binding mustEqual UserDef(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, d: Dispatch) = parse("dataset := 1 dataset")
        d.binding mustEqual UserDef(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, d: Dispatch) = parse("max := 1 max")
        d.binding mustEqual UserDef(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, d: Dispatch) = parse("mean := 1 mean")
        d.binding mustEqual UserDef(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, d: Dispatch) = parse("median := 1 median")
        d.binding mustEqual UserDef(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, d: Dispatch) = parse("mean := 1 mean")
        d.binding mustEqual UserDef(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, d: Dispatch) = parse("min := 1 min")
        d.binding mustEqual UserDef(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, d: Dispatch) = parse("mode := 1 mode")
        d.binding mustEqual UserDef(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, d: Dispatch) = parse("stdDev := 1 stdDev")
        d.binding mustEqual UserDef(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, d: Dispatch) = parse("sum := 1 sum")
        d.binding mustEqual UserDef(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "not leak shadowing into an adjacent scope" in {
      val e @ Let(_, _, _, _, Add(_, _, d: Dispatch)) = parse("a := 1 (a := 2 3) + a")
      d.binding mustEqual UserDef(e)
      d.isReduction mustEqual false
      d.errors must beEmpty
    }
  }
  
  "inherited scoping" should {
    "forward direct binding" in {
      val e @ Let(_, _, _, _, d: Dispatch) = parse("a := 42 a")
      d.binding mustEqual UserDef(e)
      d.isReduction mustEqual false
      d.errors must beEmpty
    }
    
    "forward binding through let" in {
      val e1 @ Let(_, _, _, _, e2 @ Let(_, _, _, d1: Dispatch, d2: Dispatch)) = parse("a := 42 b := a a")
      
      d1.binding mustEqual UserDef(e1)
      d1.isReduction mustEqual false
      d1.errors must beEmpty
      
      d2.binding mustEqual UserDef(e1)
      d2.isReduction mustEqual false
      d2.errors must beEmpty
    }
    
    "forward binding through new" in {
      val e @ Let(_, _, _, _, New(_, d: Dispatch)) = parse("a := 42 new a")
      d.binding mustEqual UserDef(e)
      d.isReduction mustEqual false
      d.errors must beEmpty
    }
    
    "forward binding through relate" in {
      {
        val e @ Let(_, _, _, _, Relate(_, d: Dispatch, _, _)) = parse("a := 42 a :: 1 2")
        d.binding mustEqual UserDef(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, Relate(_, _, d: Dispatch, _)) = parse("a := 42 1 :: a 2")
        d.binding mustEqual UserDef(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, Relate(_, _, _, d: Dispatch)) = parse("a := 42 1 :: 2 a")
        d.binding mustEqual UserDef(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "forward binding through object definition" in {
      val e @ Let(_, _, _, _, ObjectDef(_, Vector((_, d: Dispatch)))) = parse("a := 42 { a: a }")
      d.binding mustEqual UserDef(e)
      d.isReduction mustEqual false
      d.errors must beEmpty
    }
    
    "forward binding through array definition" in {
      val e @ Let(_, _, _, _, ArrayDef(_, Vector(d: Dispatch))) = parse("a := 42 [a]")
      d.binding mustEqual UserDef(e)
      d.isReduction mustEqual false
      d.errors must beEmpty
    }
    
    "forward binding through descent" in {
      val e @ Let(_, _, _, _, Descent(_, d: Dispatch, _)) = parse("a := 42 a.b")
      d.binding mustEqual UserDef(e)
      d.isReduction mustEqual false
      d.errors must beEmpty
    }
    
    "forward binding through dereference" in {
      val e @ Let(_, _, _, _, Deref(_, _, d: Dispatch)) = parse("a := 42 1[a]")
      d.binding mustEqual UserDef(e)
      d.isReduction mustEqual false
      d.errors must beEmpty
    }
    
    "forward binding through dispatch" in {
      val e @ Let(_, _, _, _, Dispatch(_, _, Vector(d: Dispatch))) = parse("a := 42 count(a)")
      d.binding mustEqual UserDef(e)
      d.isReduction mustEqual false
      d.errors must beEmpty
    }
    
    "forward binding through operation" in {
      {
        val e @ Let(_, _, _, _, Operation(_, d: Dispatch, _, _)) = parse("a := 42 a where 1")
        d.binding mustEqual UserDef(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, Operation(_, _, _, d: Dispatch)) = parse("a := 42 1 where a")
        d.binding mustEqual UserDef(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "forward binding through addition" in {
      {
        val e @ Let(_, _, _, _, Add(_, d: Dispatch, _)) = parse("a := 42 a + 1")
        d.binding mustEqual UserDef(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, Add(_, _, d: Dispatch)) = parse("a := 42 1 + a")
        d.binding mustEqual UserDef(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "forward binding through subtraction" in {
      {
        val e @ Let(_, _, _, _, Sub(_, d: Dispatch, _)) = parse("a := 42 a - 1")
        d.binding mustEqual UserDef(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, Sub(_, _, d: Dispatch)) = parse("a := 42 1 - a")
        d.binding mustEqual UserDef(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "forward binding through multiplication" in {
      {
        val e @ Let(_, _, _, _, Mul(_, d: Dispatch, _)) = parse("a := 42 a * 1")
        d.binding mustEqual UserDef(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, Mul(_, _, d: Dispatch)) = parse("a := 42 1 * a")
        d.binding mustEqual UserDef(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "forward binding through division" in {
      {
        val e @ Let(_, _, _, _, Div(_, d: Dispatch, _)) = parse("a := 42 a / 1")
        d.binding mustEqual UserDef(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, Div(_, _, d: Dispatch)) = parse("a := 42 1 / a")
        d.binding mustEqual UserDef(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "forward binding through less-than" in {
      {
        val e @ Let(_, _, _, _, Lt(_, d: Dispatch, _)) = parse("a := 42 a < 1")
        d.binding mustEqual UserDef(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, Lt(_, _, d: Dispatch)) = parse("a := 42 1 < a")
        d.binding mustEqual UserDef(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "forward binding through less-than-equal" in {
      {
        val e @ Let(_, _, _, _, LtEq(_, d: Dispatch, _)) = parse("a := 42 a <= 1")
        d.binding mustEqual UserDef(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, LtEq(_, _, d: Dispatch)) = parse("a := 42 1 <= a")
        d.binding mustEqual UserDef(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "forward binding through greater-than" in {
      {
        val e @ Let(_, _, _, _, Gt(_, d: Dispatch, _)) = parse("a := 42 a > 1")
        d.binding mustEqual UserDef(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, Gt(_, _, d: Dispatch)) = parse("a := 42 1 > a")
        d.binding mustEqual UserDef(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "forward binding through greater-than-equal" in {
      {
        val e @ Let(_, _, _, _, GtEq(_, d: Dispatch, _)) = parse("a := 42 a >= 1")
        d.binding mustEqual UserDef(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, GtEq(_, _, d: Dispatch)) = parse("a := 42 1 >= a")
        d.binding mustEqual UserDef(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "forward binding through equality" in {
      {
        val e @ Let(_, _, _, _, Eq(_, d: Dispatch, _)) = parse("a := 42 a = 1")
        d.binding mustEqual UserDef(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, Eq(_, _, d: Dispatch)) = parse("a := 42 1 = a")
        d.binding mustEqual UserDef(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "forward binding through not-equality" in {
      {
        val e @ Let(_, _, _, _, NotEq(_, d: Dispatch, _)) = parse("a := 42 a != 1")
        d.binding mustEqual UserDef(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, NotEq(_, _, d: Dispatch)) = parse("a := 42 1 != a")
        d.binding mustEqual UserDef(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "forward binding through boolean and" in {
      {
        val e @ Let(_, _, _, _, And(_, d: Dispatch, _)) = parse("a := 42 a & 1")
        d.binding mustEqual UserDef(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, And(_, _, d: Dispatch)) = parse("a := 42 1 & a")
        d.binding mustEqual UserDef(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "forward binding through boolean or" in {
      {
        val e @ Let(_, _, _, _, Or(_, d: Dispatch, _)) = parse("a := 42 a | 1")
        d.binding mustEqual UserDef(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, Or(_, _, d: Dispatch)) = parse("a := 42 1 | a")
        d.binding mustEqual UserDef(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "forward binding through complement" in {
      val e @ Let(_, _, _, _, Comp(_, d: Dispatch)) = parse("a := 42 !a")
      d.binding mustEqual UserDef(e)
      d.isReduction mustEqual false
      d.errors must beEmpty
    }
    
    "forward binding through negation" in {
      val e @ Let(_, _, _, _, Neg(_, d: Dispatch)) = parse("a := 42 ~a")
      d.binding mustEqual UserDef(e)
      d.isReduction mustEqual false
      d.errors must beEmpty
    }
    
    "forward binding through parentheses" in {
      val e @ Let(_, _, _, _, Paren(_, d: Dispatch)) = parse("a := 42 (a)")
      d.binding mustEqual UserDef(e)
      d.isReduction mustEqual false
      d.errors must beEmpty
    }
  }
  
  "pre-binding of built-in functions" should {
    "bind count" in {
      val d @ Dispatch(_, _, _) = parse("count")
      d.binding mustEqual BuiltIn("count", 1)
      d.isReduction mustEqual true
      d.errors must beEmpty
    }
    
    "bind dataset" in {
      val d @ Dispatch(_, _, _) = parse("dataset")
      d.binding mustEqual BuiltIn("dataset", 1)
      d.isReduction mustEqual false
      d.errors must beEmpty
    }
    
    "bind max" in {
      val d @ Dispatch(_, _, _) = parse("max")
      d.binding mustEqual BuiltIn("max", 1)
      d.isReduction mustEqual true
      d.errors must beEmpty
    }
    
    "bind mean" in {
      val d @ Dispatch(_, _, _) = parse("mean")
      d.binding mustEqual BuiltIn("mean", 1)
      d.isReduction mustEqual true
      d.errors must beEmpty
    }
    
    "bind median" in {
      val d @ Dispatch(_, _, _) = parse("median")
      d.binding mustEqual BuiltIn("median", 1)
      d.isReduction mustEqual true
      d.errors must beEmpty
    }
    
    "bind min" in {
      val d @ Dispatch(_, _, _) = parse("min")
      d.binding mustEqual BuiltIn("min", 1)
      d.isReduction mustEqual true
      d.errors must beEmpty
    }
    
    "bind mode" in {
      val d @ Dispatch(_, _, _) = parse("mode")
      d.binding mustEqual BuiltIn("mode", 1)
      d.isReduction mustEqual true
      d.errors must beEmpty
    }
    
    "bind stdDev" in {
      val d @ Dispatch(_, _, _) = parse("stdDev")
      d.binding mustEqual BuiltIn("stdDev", 1)
      d.isReduction mustEqual true
      d.errors must beEmpty
    }
    
    "bind sum" in {
      val d @ Dispatch(_, _, _) = parse("sum")
      d.binding mustEqual BuiltIn("sum", 1)
      d.isReduction mustEqual true
      d.errors must beEmpty
    }
  }
}
