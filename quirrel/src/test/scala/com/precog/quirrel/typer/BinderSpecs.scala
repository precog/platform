package com.precog
package quirrel
package typer

import com.codecommit.gll.LineStream
import org.specs2.ScalaCheck
import org.specs2.mutable.Specification
import parser._

import java.io.File
import scala.io.Source

object BinderSpecs extends Specification
    with ScalaCheck
    with Parser
    with StubPhases
    with Binder
    with StaticLibrarySpec {
      
  import ast._
  import library._
  
  "let binding" should {
    "bind name in resulting scope" in {
      val e @ Let(_, _, _, _, d: Dispatch) = parseSingle("a := 42 a")
      d.binding mustEqual LetBinding(e)
      d.isReduction mustEqual false
      d.errors must beEmpty
    }
    
    "bind name with non-zero arity" in {
      {
        val e @ Let(_, _, _, _, d: Dispatch) = parseSingle("a(b) := 42 a(true)")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, d: Dispatch) = parseSingle("a(b, c, d) := 42 a(true, false, 2)")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "not bind name in expression scope" in {
      val Let(_, _, _, left: Dispatch, _) = parseSingle("a := a 42")
      left.binding mustEqual NullBinding
      left.isReduction mustEqual false
      left.errors mustEqual Set(UndefinedFunction(Identifier(Vector(), "a")))
    }
    
    "bind all formals in expression scope" in {
      {
        val e @ Let(_, _, _, t: Dispatch, _) = parseSingle("a(b) := b a(1)")
        t.binding mustEqual FormalBinding(e)
        t.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, Add(_, Add(_, tb: Dispatch, tc: Dispatch), td: Dispatch), _) = parseSingle("a(b, c, d) := b + c + d a(1, 2, 3)")
        
        tb.binding mustEqual FormalBinding(e)
        tc.binding mustEqual FormalBinding(e)
        td.binding mustEqual FormalBinding(e)
        
        tb.errors must beEmpty
        tc.errors must beEmpty
        td.errors must beEmpty
      }
    }
    
    "not bind formal in resulting scope" in {
      val Let(_, _, _, _, t: Dispatch) = parseSingle("a(b) := 42 b")
      t.binding mustEqual NullBinding
      t.errors mustEqual Set(UndefinedFunction(Identifier(Vector(), "b")))
    }
    
    "bind all tic-variables in expression scope" in {
      {
        val e @ Solve(_, _, t: TicVar) = parseSingle("solve 'b 'b")
        t.binding mustEqual SolveBinding(e)
        t.errors must beEmpty
      }
      
      {
        val e @ Solve(_, _, Add(_, Add(_, tb: TicVar, tc: TicVar), td: TicVar)) = parseSingle("solve 'b, 'c, 'd 'b + 'c + 'd")
        
        tb.binding mustEqual SolveBinding(e)
        tc.binding mustEqual SolveBinding(e)
        td.binding mustEqual SolveBinding(e)
        
        tb.errors must beEmpty
        tc.errors must beEmpty
        td.errors must beEmpty
      }
    }
    
    "bind all tic-variables determined from free set on constraint exprs" in {
      {
        val e @ Solve(_, _, t: TicVar) = parseSingle("solve 'a 'a")
        t.binding mustEqual SolveBinding(e)
        t.errors must beEmpty
      }
      
      {
        val e @ Solve(_, _, t: TicVar) = parseSingle("solve 'a = 42 'a")
        t.binding mustEqual SolveBinding(e)
        t.errors must beEmpty
      }
      
      {
        val e @ Solve(_, _, Add(_, Add(_, ta: TicVar, tb: TicVar), tc: TicVar)) = parseSingle("solve 'a, 'b, 'c 'a + 'b + 'c")
        
        ta.binding mustEqual SolveBinding(e)
        tb.binding mustEqual SolveBinding(e)
        tc.binding mustEqual SolveBinding(e)
        
        ta.errors must beEmpty
        tb.errors must beEmpty
        tc.errors must beEmpty
      }
      
      {
        val e @ Solve(_, _, Add(_, Add(_, ta: TicVar, tb: TicVar), tc: TicVar)) = parseSingle("solve 'a * 42, 'b = false where count('c) 'a + 'b + 'c")
        
        ta.binding mustEqual SolveBinding(e)
        tb.binding mustEqual SolveBinding(e)
        tc.binding mustEqual SolveBinding(e)
        
        ta.errors must beEmpty
        tb.errors must beEmpty
        tc.errors must beEmpty
      }
    }
    
    "bind tic-variables in the constraint set" in {
      val e @ Solve(_, Vector(t: TicVar), _) = parseSingle("solve 'a 'a")
      t.binding mustEqual FreeBinding(e)
      t.errors must beEmpty
    }
    
    "bind formals in the constraint set" in {
      val e @ Let(_, _, _, Solve(_, Vector(Add(_, _, d: Dispatch)), _), _) = parseSingle("f(x) := solve 'a + x 'a 42")
      d.binding mustEqual FormalBinding(e)
      d.errors must beEmpty
    }
    
    "bind names in the constraint set" in {
      val e @ Let(_, _, _, _, Solve(_, Vector(Add(_, _, d: Dispatch)), _)) = parseSingle("f := 42 solve 'a + f 'a")
      d.binding mustEqual LetBinding(e)
      d.errors must beEmpty
    }
    
    "reject unbound name in the constraint set" in {
      val e @ Let(_, _, _, _, Solve(_, Vector(Add(_, _, d: Dispatch)), _)) = parseSingle("f := 42 solve 'a + g 'a")
      d.binding mustEqual NullBinding
      d.errors mustEqual Set(UndefinedFunction(Identifier(Vector(), "g")))
    }
    
    "reject unbound tic-variables" in {
      val e @ Solve(_, _, t: TicVar) = parseSingle("solve 'a 'b")
      t.binding mustEqual NullBinding
      t.errors mustEqual Set(UndefinedTicVariable("'b"))
    }
    
    "reject solve lacking free variables" in {
      val e @ Solve(_, _, _) = parseSingle("solve 42 12")
      e.errors mustEqual Set(SolveLackingFreeVariables)
    }
    
    "reject solve lacking free variables in one of many constraints" in {
      val e @ Solve(_, _, _) = parseSingle("solve 42, 'a, 'b, 'c 12")
      e.errors mustEqual Set(SolveLackingFreeVariables)
    }
    
    "bind name in inner scope" in {
      val e1 @ Let(_, _, _, _, e2 @ Let(_, _, _, _, d: Dispatch)) = parseSingle("a := 42 b := 24 b")
      
      d.binding mustEqual LetBinding(e2)
      d.isReduction mustEqual false
      d.errors must beEmpty
    }
    
    "bind formal in inner scope" in {
      val e1 @ Let(_, _, _, _, e2 @ Let(_, _, _, t: Dispatch, _)) = parseSingle("a := 42 b(b) := b 24")
      
      t.binding mustEqual FormalBinding(e2)
      t.errors must beEmpty
    }
    
    "attribute call sites onto functions" in {
      val e1 @ Let(_, _, _, _, Add(_, d1: Dispatch, d2: Dispatch)) = parseSingle("f(x) := x f(12) + f(16)")
      e1.dispatches mustEqual Set(d1, d2)
      e1.errors must beEmpty
    }
    
    "reject unbound dispatch" in {
      {
        val d @ Dispatch(_, _, _) = parseSingle("foo")
        d.binding mustEqual NullBinding
        d.isReduction mustEqual false
        d.errors mustEqual Set(UndefinedFunction(Identifier(Vector(), "foo")))
      }
      
      {
        val d @ Dispatch(_, _, _) = parseSingle("foo(2, 1, 4)")
        d.binding mustEqual NullBinding
        d.isReduction mustEqual false
        d.errors mustEqual Set(UndefinedFunction(Identifier(Vector(), "foo")))
      }
    }
    
    "reject unbound dispatch with a namespace" in {
      {
        val d @ Dispatch(_, _, _) = parseSingle("foo::bar::baz")
        d.binding mustEqual NullBinding
        d.isReduction mustEqual false
        d.errors mustEqual Set(UndefinedFunction(Identifier(Vector("foo", "bar"), "baz")))
      }
      
      {
        val d @ Dispatch(_, _, _) = parseSingle("foo::bar::baz(7, 8, 9)")
        d.binding mustEqual NullBinding
        d.isReduction mustEqual false
        d.errors mustEqual Set(UndefinedFunction(Identifier(Vector("foo", "bar"), "baz")))
      }
    }
    
    "reject dispatch with lesser arity" in {
      {
        val Let(_, _, _, _, d: Dispatch) = parseSingle("a(b) := 42 a")
        d.binding mustEqual NullBinding
        d.isReduction mustEqual false
        d.errors mustEqual Set(IncorrectArity(1, 0))
      }
      
      {
        val Let(_, _, _, _, d: Dispatch) = parseSingle("a(b, c, d) := 42 a")
        d.binding mustEqual NullBinding
        d.isReduction mustEqual false
        d.errors mustEqual Set(IncorrectArity(3, 0))
      }
    }
    
    "reject dispatch with greater arity" in {
      {
        val Let(_, _, _, _, d: Dispatch) = parseSingle("a(b) := 42 a(1, 2)")
        d.binding mustEqual NullBinding
        d.isReduction mustEqual false
        d.errors mustEqual Set(IncorrectArity(1, 2))
      }
      
      {
        val Let(_, _, _, _, d: Dispatch) = parseSingle("a(b, c, d) := 42 a(1, 2, 3, 4, 5, 6)")
        d.binding mustEqual NullBinding
        d.isReduction mustEqual false
        d.errors mustEqual Set(IncorrectArity(3, 6))
      }
    }
    
    "reject formal with non-zero arity" in {
      val Let(_, _, _, d: Dispatch, _) = parseSingle("a(b) := b(1, 2) 42")
      d.binding mustEqual NullBinding
      d.isReduction mustEqual false
      d.errors mustEqual Set(IncorrectArity(0, 2))
    }
    
    "reject unbound tic-variables" in {
      {
        val d @ TicVar(_, _) = parseSingle("'foo")
        d.binding mustEqual NullBinding
        d.errors mustEqual Set(UndefinedTicVariable("'foo"))
      }
      
      {
        val d @ TicVar(_, _) = parseSingle("'bar")
        d.binding mustEqual NullBinding
        d.errors mustEqual Set(UndefinedTicVariable("'bar"))
      }
      
      {
        val d @ TicVar(_, _) = parseSingle("'baz")
        d.binding mustEqual NullBinding
        d.errors mustEqual Set(UndefinedTicVariable("'baz"))
      }
    }

    "allow a case when a tic variable is not solvable in all cases" in {
      {
        val tree = parseSingle("""
        | solve 'b
        |   k := //clicks.time where //clicks.time = 'b
        |   j := //views.time where //views.time > 'b
        |   k ~ j
        |   {kay: k, jay: j}""".stripMargin)

        tree.errors must beEmpty
      }
    }
    
    "accept multiple definitions of tic-variables" in {
      {
        val tree = parseSingle("solve 'a, 'a 1")
        tree.errors must beEmpty
      }
      
      {
        val tree = parseSingle("solve 'a, 'b, 'c, 'a 1")
        tree.errors must beEmpty
      }
      
      {
        val tree = parseSingle("solve 'a, 'b, 'c, 'a, 'b, 'a 1")
        tree.errors must beEmpty
      }
    }
    
    "not leak dispatch into an adjacent scope" in {
      val Add(_, _, d: Dispatch) = parseSingle("(a := 1 2) + a")
      d.binding mustEqual NullBinding
      d.isReduction mustEqual false
      d.errors mustEqual Set(UndefinedFunction(Identifier(Vector(), "a")))
    }
    
    "not leak formal into an adjacent scope" in {
      val Add(_, _, t: Dispatch) = parseSingle("(a(b) := 1 2) + b")
      t.binding mustEqual NullBinding
      t.errors mustEqual Set(UndefinedFunction(Identifier(Vector(), "b")))
    }
    
    "allow shadowing of user-defined bindings" in {
      val Let(_, _, _, _, e @ Let(_, _, _, _, d: Dispatch)) = parseSingle("a := 1 a := 2 a")
      d.binding mustEqual LetBinding(e)
      d.isReduction mustEqual false
      d.errors must beEmpty
    }
    
    "allow shadowing of formals" in {
      val Let(_, _, _, _, e @ Let(_, _, _, t: Dispatch, _)) = parseSingle("a(c) := 1 b(c) := c 2")
      t.binding mustEqual FormalBinding(e)
      t.errors must beEmpty
    }
    
    "allow shadowing of built-in bindings" >> {
      "count" >> {
        val e @ Let(_, _, _, _, d: Dispatch) = parseSingle("count := 1 count")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      "geometricMean" >> {
        val e @ Let(_, _, _, _, d: Dispatch) = parseSingle("geometricMean := 1 geometricMean")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      "load" >> {
        val e @ Let(_, _, _, _, d: Dispatch) = parseSingle("load := 1 load")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      "relativeLoad" >> {
        val e @ Let(_, _, _, _, d: Dispatch) = parseSingle("relativeLoad := 1 relativeLoad")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      "max" >> {
        val e @ Let(_, _, _, _, d: Dispatch) = parseSingle("max := 1 max")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      "mean" >> {
        val e @ Let(_, _, _, _, d: Dispatch) = parseSingle("mean := 1 mean")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      "median" >> {
        val e @ Let(_, _, _, _, d: Dispatch) = parseSingle("median := 1 median")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      "mean" >> {
        val e @ Let(_, _, _, _, d: Dispatch) = parseSingle("mean := 1 mean")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      "min" >> {
        val e @ Let(_, _, _, _, d: Dispatch) = parseSingle("min := 1 min")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      "mode" >> {
        val e @ Let(_, _, _, _, d: Dispatch) = parseSingle("mode := 1 mode")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      "stdDev" >> {
        val e @ Let(_, _, _, _, d: Dispatch) = parseSingle("stdDev := 1 stdDev")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      "sum" >> {
        val e @ Let(_, _, _, _, d: Dispatch) = parseSingle("sum := 1 sum")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }      

      "sumSq" >> {
        val e @ Let(_, _, _, _, d: Dispatch) = parseSingle("sumSq := 1 sumSq")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      "variance" >> {
        val e @ Let(_, _, _, _, d: Dispatch) = parseSingle("variance := 1 variance")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      "distinct" >> {
        val e @ Let(_, _, _, _, d: Dispatch) = parseSingle("distinct := 1 distinct")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "not leak shadowing into an adjacent scope" in {
      val e @ Let(_, _, _, _, Add(_, _, d: Dispatch)) = parseSingle("a := 1 (a := 2 3) + a")
      d.binding mustEqual LetBinding(e)
      d.isReduction mustEqual false
      d.errors must beEmpty
    }
  }
  
  "inherited scoping" should {
    "forward direct binding" in {
      val e @ Let(_, _, _, _, d: Dispatch) = parseSingle("a := 42 a")
      d.binding mustEqual LetBinding(e)
      d.isReduction mustEqual false
      d.errors must beEmpty
    }
    
    "forward binding through let" in {
      val e1 @ Let(_, _, _, _, e2 @ Let(_, _, _, d1: Dispatch, d2: Dispatch)) = parseSingle("a := 42 b := a a")
      
      d1.binding mustEqual LetBinding(e1)
      d1.isReduction mustEqual false
      d1.errors must beEmpty
      
      d2.binding mustEqual LetBinding(e1)
      d2.isReduction mustEqual false
      d2.errors must beEmpty
    }
    
    "forward binding through import" in {
      val e @ Let(_, _, _, _, Import(_, _, d: Dispatch)) = parseSingle("a := 42 import std a")
      d.binding mustEqual LetBinding(e)
      d.isReduction mustEqual false
      d.errors must beEmpty
    }
    
    "forward binding through assert" in {
      val e @ Let(_, _, _, _, Assert(_, _, d: Dispatch)) = parseSingle("a := 42 assert true a")
      d.binding mustEqual LetBinding(e)
      d.isReduction mustEqual false
      d.errors must beEmpty
    }
    
    "forward binding through new" in {
      val e @ Let(_, _, _, _, New(_, d: Dispatch)) = parseSingle("a := 42 new a")
      d.binding mustEqual LetBinding(e)
      d.isReduction mustEqual false
      d.errors must beEmpty
    }
    
    "forward binding through relate" in {
      {
        val e @ Let(_, _, _, _, Relate(_, d: Dispatch, _, _)) = parseSingle("a := 42 a ~ 1 2")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, Relate(_, _, d: Dispatch, _)) = parseSingle("a := 42 1 ~ a 2")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, Relate(_, _, _, d: Dispatch)) = parseSingle("a := 42 1 ~ 2 a")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "forward binding through object definition" in {
      val e @ Let(_, _, _, _, ObjectDef(_, Vector((_, d: Dispatch)))) = parseSingle("a := 42 { a: a }")
      d.binding mustEqual LetBinding(e)
      d.isReduction mustEqual false
      d.errors must beEmpty
    }
    
    "forward binding through array definition" in {
      val e @ Let(_, _, _, _, ArrayDef(_, Vector(d: Dispatch))) = parseSingle("a := 42 [a]")
      d.binding mustEqual LetBinding(e)
      d.isReduction mustEqual false
      d.errors must beEmpty
    }
    
    "forward binding through descent" in {
      val e @ Let(_, _, _, _, Descent(_, d: Dispatch, _)) = parseSingle("a := 42 a.b")
      d.binding mustEqual LetBinding(e)
      d.isReduction mustEqual false
      d.errors must beEmpty
    }
    
    "forward binding through metadescent" in {
      val e @ Let(_, _, _, _, MetaDescent(_, d: Dispatch, _)) = parseSingle("a := 42 a@b")
      d.binding mustEqual LetBinding(e)
      d.isReduction mustEqual false
      d.errors must beEmpty
    }
    
    "forward binding through dereference" in {
      val e @ Let(_, _, _, _, Deref(_, _, d: Dispatch)) = parseSingle("a := 42 1[a]")
      d.binding mustEqual LetBinding(e)
      d.isReduction mustEqual false
      d.errors must beEmpty
    }
    
    "forward binding through dispatch" in {
      forall(libReduction) { f => 
        val e @ Let(_, _, _, _, Dispatch(_, _, Vector(d: Dispatch))) = parseSingle("a := 42 %s(a)".format(f.fqn))
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "forward binding through where" in {
      {
        val e @ Let(_, _, _, _, Where(_, d: Dispatch, _)) = parseSingle("a := 42 a where 1")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, Where(_, _, d: Dispatch)) = parseSingle("a := 42 1 where a")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "forward binding through with" in {
      {
        val e @ Let(_, _, _, _, With(_, d: Dispatch, _)) = parseSingle("a := 42 a with 1")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, With(_, _, d: Dispatch)) = parseSingle("a := 42 1 with a")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "forward binding through union" in {
      {
        val e @ Let(_, _, _, _, Union(_, d: Dispatch, _)) = parseSingle("a := 42 a union 1")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, Union(_, _, d: Dispatch)) = parseSingle("a := 42 1 union a")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "forward binding through intersect" in {
      {
        val e @ Let(_, _, _, _, Intersect(_, d: Dispatch, _)) = parseSingle("a := 42 a intersect 1")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, Intersect(_, _, d: Dispatch)) = parseSingle("a := 42 1 intersect a")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "forward binding through difference" in {
      {
        val e @ Let(_, _, _, _, Difference(_, d: Dispatch, _)) = parseSingle("a := 42 a difference 1")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, Difference(_, _, d: Dispatch)) = parseSingle("a := 42 1 difference a")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "forward binding through addition" in {
      {
        val e @ Let(_, _, _, _, Add(_, d: Dispatch, _)) = parseSingle("a := 42 a + 1")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, Add(_, _, d: Dispatch)) = parseSingle("a := 42 1 + a")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "forward binding through subtraction" in {
      {
        val e @ Let(_, _, _, _, Sub(_, d: Dispatch, _)) = parseSingle("a := 42 a - 1")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, Sub(_, _, d: Dispatch)) = parseSingle("a := 42 1 - a")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "forward binding through multiplication" in {
      {
        val e @ Let(_, _, _, _, Mul(_, d: Dispatch, _)) = parseSingle("a := 42 a * 1")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, Mul(_, _, d: Dispatch)) = parseSingle("a := 42 1 * a")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "forward binding through division" in {
      {
        val e @ Let(_, _, _, _, Div(_, d: Dispatch, _)) = parseSingle("a := 42 a / 1")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, Div(_, _, d: Dispatch)) = parseSingle("a := 42 1 / a")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "forward binding through mod" in {
      {
        val e @ Let(_, _, _, _, Mod(_, d: Dispatch, _)) = parseSingle("a := 42 a % 1")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, Mod(_, _, d: Dispatch)) = parseSingle("a := 42 1 % a")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "forward binding through pow" in {
      {
        val e @ Let(_, _, _, _, Pow(_, d: Dispatch, _)) = parseSingle("a := 42 a ^ 1")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }

      {
        val e @ Let(_, _, _, _, Pow(_, _, d: Dispatch)) = parseSingle("a := 42 1 ^ a")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }

    "forward binding through less-than" in {
      {
        val e @ Let(_, _, _, _, Lt(_, d: Dispatch, _)) = parseSingle("a := 42 a < 1")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, Lt(_, _, d: Dispatch)) = parseSingle("a := 42 1 < a")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "forward binding through less-than-equal" in {
      {
        val e @ Let(_, _, _, _, LtEq(_, d: Dispatch, _)) = parseSingle("a := 42 a <= 1")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, LtEq(_, _, d: Dispatch)) = parseSingle("a := 42 1 <= a")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "forward binding through greater-than" in {
      {
        val e @ Let(_, _, _, _, Gt(_, d: Dispatch, _)) = parseSingle("a := 42 a > 1")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, Gt(_, _, d: Dispatch)) = parseSingle("a := 42 1 > a")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "forward binding through greater-than-equal" in {
      {
        val e @ Let(_, _, _, _, GtEq(_, d: Dispatch, _)) = parseSingle("a := 42 a >= 1")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, GtEq(_, _, d: Dispatch)) = parseSingle("a := 42 1 >= a")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "forward binding through equality" in {
      {
        val e @ Let(_, _, _, _, Eq(_, d: Dispatch, _)) = parseSingle("a := 42 a = 1")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, Eq(_, _, d: Dispatch)) = parseSingle("a := 42 1 = a")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "forward binding through not-equality" in {
      {
        val e @ Let(_, _, _, _, NotEq(_, d: Dispatch, _)) = parseSingle("a := 42 a != 1")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, NotEq(_, _, d: Dispatch)) = parseSingle("a := 42 1 != a")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "forward binding through boolean and" in {
      {
        val e @ Let(_, _, _, _, And(_, d: Dispatch, _)) = parseSingle("a := 42 a & 1")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, And(_, _, d: Dispatch)) = parseSingle("a := 42 1 & a")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "forward binding through boolean or" in {
      {
        val e @ Let(_, _, _, _, Or(_, d: Dispatch, _)) = parseSingle("a := 42 a | 1")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
      
      {
        val e @ Let(_, _, _, _, Or(_, _, d: Dispatch)) = parseSingle("a := 42 1 | a")
        d.binding mustEqual LetBinding(e)
        d.isReduction mustEqual false
        d.errors must beEmpty
      }
    }
    
    "forward binding through complement" in {
      val e @ Let(_, _, _, _, Comp(_, d: Dispatch)) = parseSingle("a := 42 !a")
      d.binding mustEqual LetBinding(e)
      d.isReduction mustEqual false
      d.errors must beEmpty
    }
    
    "forward binding through negation" in {
      val e @ Let(_, _, _, _, Neg(_, d: Dispatch)) = parseSingle("a := 42 neg a")
      d.binding mustEqual LetBinding(e)
      d.isReduction mustEqual false
      d.errors must beEmpty
    }
    
    "forward binding through parentheses" in {
      val e @ Let(_, _, _, _, Paren(_, d: Dispatch)) = parseSingle("a := 42 (a)")
      d.binding mustEqual LetBinding(e)
      d.isReduction mustEqual false
      d.errors must beEmpty
    }
  }
  
  "pre-binding of load and distinct" should {
    "bind load" in {
      val d @ Dispatch(_, _, _) = parseSingle("load(42)")
      d.binding mustEqual LoadBinding
      d.isReduction mustEqual false
      d.errors must beEmpty
    }
    
    "bind relativeLoad" in {
      val d @ Dispatch(_, _, _) = parseSingle("relativeLoad(42)")
      d.binding mustEqual RelLoadBinding
      d.isReduction mustEqual false
      d.errors must beEmpty
    }
    
    "bind distinct" in {
      val d @ Dispatch(_, _, _) = parseSingle("distinct(12)")
      d.binding mustEqual DistinctBinding
      d.isReduction mustEqual false
      d.errors must beEmpty
    }
  }

  "pre-binding of built-in functions" should {
    "bind unary morphisms" in {
      libMorphism1 must not(beEmpty)

      forall(libMorphism1) { f =>
        val d @ Dispatch(_, _, _) = parseSingle(f.fqn + "(1)")
        d.binding mustEqual Morphism1Binding(f)
        d.isReduction mustEqual false
        d.errors filterNot isWarning must beEmpty
      }
    }    

    "bind binary morphisms" in {
      libMorphism2 must not(beEmpty)

      forall(libMorphism2) { f =>
        val d @ Dispatch(_, _, _) = parseSingle(f.fqn + "(1, 2)")
        d.binding mustEqual Morphism2Binding(f)
        d.isReduction mustEqual false
        d.errors filterNot isWarning must beEmpty
      }
    }

    "bind reductions" in {
      libReduction must not(beEmpty)

      forall(libReduction) { f =>
        val d @ Dispatch(_, _, _) = parseSingle(f.fqn + "(1)")
        d.binding mustEqual ReductionBinding(f)
        d.isReduction mustEqual true
        d.errors filterNot isWarning must beEmpty
      }
    }

    "bind unary functions" in {
      forall(lib1) { f =>
        val d @ Dispatch(_, _, _) = parseSingle(f.fqn + "(1)")
        d.binding mustEqual Op1Binding(f)
        d.isReduction mustEqual false
        d.errors filterNot isWarning must beEmpty
      }
    }

    "bind binary functions" in {
      forall(lib2) { f =>
        val d @ Dispatch(_, _, _) = parseSingle(f.fqn + "(1, 2)")
        d.binding mustEqual Op2Binding(f)
        d.isReduction mustEqual false
        d.errors filterNot isWarning must beEmpty
      }
    }
    
    "emit a warning on deprecated usage" in {
      val d @ Dispatch(_, _, _) = parseSingle("bin25(1)")
      d.errors must contain(DeprecatedFunction(Identifier(Vector(), "bin25"), "use bin5 instead"))
    }
  }
  
  "complete hierarchical imports" should {
    "allow use of a unary function unqualified" in {
      val input = """
        | import std::lib::baz
        | baz(1)""".stripMargin
        
      val Import(_, _, d: Dispatch) = parseSingle(input)
      
      d.binding mustEqual Op1Binding(Op1(Vector("std", "lib"), "baz", 0x0003))
      d.errors must beEmpty
    }
    
    "allow use of a unary function unqualified in a hierarchical import" in {
      val input = """
        | import std::lib
        | import lib::baz
        | baz(1)""".stripMargin
        
      val Import(_, _, Import(_, _, d: Dispatch)) = parseSingle(input)
      
      d.binding mustEqual Op1Binding(Op1(Vector("std", "lib"), "baz", 0x0003))
      d.errors must beEmpty
    }
    
    "allow the use of a unary function partially-qualified" in {
      val input = """
        | import std::lib
        | lib::baz(1)""".stripMargin
        
      val Import(_, _, d: Dispatch) = parseSingle(input)
      
      d.binding mustEqual Op1Binding(Op1(Vector("std", "lib"), "baz", 0x0003))
      d.errors must beEmpty
    }
    
    "reject the use of a sub-package when parent has been singularly imported" in {
      val input = """
        | import std
        | import lib
        | lib::baz(1)""".stripMargin
        
      val Import(_, _, Import(_, _, d: Dispatch)) = parseSingle(input)
      
      d.errors must not(beEmpty)
    }
    
    "allow the use of a function that shadows a package" in {
      val input = """
        | import std::lib
        | lib(1)""".stripMargin
        
      val Import(_, _, d: Dispatch) = parseSingle(input)
      
      d.binding mustEqual Op1Binding(Op1(Vector("std"), "lib", 0x0004))
      d.errors must beEmpty
    }
    
    "bind most specific import in case of shadowing" in {
      val input = """
        | import std::bin
        | bin(1)""".stripMargin
        
      val Import(_, _, d: Dispatch) = parseSingle(input)
      
      d.binding mustEqual Op1Binding(Op1(Vector("std"), "bin", 0x0001))
      d.errors must beEmpty
    }
    
    "not affect outer scope" in {
      val input = """
        | bin(1) +
        | import std::bin
        | bin(1)""".stripMargin
        
      val Add(_, d1: Dispatch, Import(_, _, d2: Dispatch)) = parseSingle(input)
      
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
        | baz(1)""".stripMargin
        
      val Import(_, _, d: Dispatch) = parseSingle(input)
      
      d.binding mustEqual Op1Binding(Op1(Vector("std", "lib"), "baz", 0x0003))
      d.errors must beEmpty
    }
    
    "allow use of more than one unary function unqualified" in {
      val input = """
        | import std::lib::*
        | baz(1) + baz2(1, 2)""".stripMargin
        
      val Import(_, _, Add(_, d1: Dispatch, d2: Dispatch)) = parseSingle(input)
      
      d1.binding mustEqual Op1Binding(Op1(Vector("std", "lib"), "baz", 0x0003))
      d1.errors must beEmpty
      
      d2.binding mustEqual Op2Binding(Op2(Vector("std", "lib"), "baz2", 0x0003))
      d2.errors must beEmpty
    }
    
    "allow use of a unary function unqualified in a hierarchical import" in {
      val input = """
        | import std::*
        | import lib::*
        | baz(1)""".stripMargin
        
      val Import(_, _, Import(_, _, d: Dispatch)) = parseSingle(input)
      
      d.binding mustEqual Op1Binding(Op1(Vector("std", "lib"), "baz", 0x0003))
      d.errors must beEmpty
    }
    
    "allow the use of a unary function partially-qualified" in {
      val input = """
        | import std::*
        | lib::baz(1)""".stripMargin
        
      val Import(_, _, d: Dispatch) = parseSingle(input)
      
      d.binding mustEqual Op1Binding(Op1(Vector("std", "lib"), "baz", 0x0003))
      d.errors must beEmpty
    }
    
    "allow the use of a function that shadows a package" in {
      val input = """
        | import std::*
        | lib(1)""".stripMargin
        
      val Import(_, _, d: Dispatch) = parseSingle(input)
      
      d.binding mustEqual Op1Binding(Op1(Vector("std"), "lib", 0x0004))
      d.errors must beEmpty
    }
    
    "bind most specific import in case of shadowing" in {
      val input = """
        | import std::*
        | bin(1)""".stripMargin
        
      val Import(_, _, d: Dispatch) = parseSingle(input)
      
      d.binding mustEqual Op1Binding(Op1(Vector("std"), "bin", 0x0001))
      d.errors must beEmpty
    }
    
    "not affect outer scope" in {
      val input = """
        | bin(1) +
        | import std::*
        | bin(1)""".stripMargin
        
      val Add(_, d1: Dispatch, Import(_, _, d2: Dispatch)) = parseSingle(input)
      
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
          val result = parseSingle(LineStream(Source.fromFile(file)))
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
