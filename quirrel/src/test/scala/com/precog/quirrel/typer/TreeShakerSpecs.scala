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
import org.specs2.mutable.Specification
import com.codecommit.gll.LineStream

object TreeShakerSpecs extends Specification with StubPhases with TreeShaker with RandomLibrary {
  import ast._
  
  "tree shaking" should {
    "bind root on result" in {
      val tree = New(LineStream(), StrLit(LineStream(), "testing"))
      bindRoot(tree, tree)
      
      val results = shakeTree(tree)
      tree.root mustEqual tree
    }
    
    "start from the root when invoked on a child" in {
      val tree = New(LineStream(), StrLit(LineStream(), "testing"))
      bindRoot(tree, tree)
      
      val results = shakeTree(tree.child)
      tree.root mustEqual tree
      
      results must beLike {
        case New(LineStream(), StrLit(LineStream(), "testing")) => ok
      }
    }
    
    "leave non-bindings unchanged" in {
      "new" >> {
        val tree = New(LineStream(), NumLit(LineStream(), "42"))
        bindRoot(tree, tree)
        
        shakeTree(tree) must beLike {
          case New(LineStream(), NumLit(LineStream(), "42")) => ok
        }
      }      
      
      "import" >> {
        val tree = Import(LineStream(), SpecificImport(Vector("std")), NumLit(LineStream(), "12"))
        bindRoot(tree, tree)
        
        shakeTree(tree) must beLike {
          case Import(LineStream(), SpecificImport(Vector("std")), NumLit(LineStream(), "12"))=> ok
        }
      }
      
      "relate" >> {
        val tree = Relate(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2"), NumLit(LineStream(), "3"))
        bindRoot(tree, tree)
        
        shakeTree(tree) must beLike {
          case Relate(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2"), NumLit(LineStream(), "3")) => ok
        }
      }
      
      "tic variable" >> {
        val tree = TicVar(LineStream(), "'a")
        bindRoot(tree, tree)
        
        shakeTree(tree) must beLike {
          case TicVar(LineStream(), "'a") => ok
        }
      }
      
      "string literal" >> {
        val tree = StrLit(LineStream(), "foo")
        bindRoot(tree, tree)
        
        shakeTree(tree) must beLike {
          case StrLit(LineStream(), "foo") => ok
        }
      }
      
      "num literal" >> {
        val tree = NumLit(LineStream(), "42")
        bindRoot(tree, tree)
        
        shakeTree(tree) must beLike {
          case NumLit(LineStream(), "42") => ok
        }
      }
      
      "bool literal" >> {
        val tree = BoolLit(LineStream(), true)
        bindRoot(tree, tree)
        
        shakeTree(tree) must beLike {
          case BoolLit(LineStream(), true) => ok
        }
      }      

      "null literal" >> {
        val tree = NullLit(LineStream())
        bindRoot(tree, tree)
        
        shakeTree(tree) must beLike {
          case NullLit(LineStream()) => ok
        }
      }
      
      "object with numeric fields" >> {
        val tree = ObjectDef(LineStream(), Vector(("a", NumLit(LineStream(), "1")), ("b", NumLit(LineStream(), "2"))))
        bindRoot(tree, tree)
        
        shakeTree(tree) must beLike {
          case ObjectDef(LineStream(), Vector(("a", NumLit(LineStream(), "1")), ("b", NumLit(LineStream(), "2")))) => ok
        }
      }
      
      "object with string and null fields" >> {
        val tree = ObjectDef(LineStream(), Vector(("a", NullLit(LineStream())), ("b", StrLit(LineStream(), "foo"))))
        bindRoot(tree, tree)
        
        shakeTree(tree) must beLike {
          case ObjectDef(LineStream(), Vector(("a", NullLit(LineStream())), ("b", StrLit(LineStream(), "foo")))) => ok
        }
      }
      
      "array with numeric fields" >> {
        val tree = ArrayDef(LineStream(), Vector(NumLit(LineStream(), "1"), NumLit(LineStream(), "2")))
        bindRoot(tree, tree)
        
        shakeTree(tree) must beLike {
          case ArrayDef(LineStream(), Vector(NumLit(LineStream(), "1"), NumLit(LineStream(), "2"))) => ok
        }
      }      

      "array with string and null fields" >> {
        val tree = ArrayDef(LineStream(), Vector(NullLit(LineStream()), StrLit(LineStream(), "foo")))
        bindRoot(tree, tree)
        
        shakeTree(tree) must beLike {
          case ArrayDef(LineStream(), Vector(NullLit(LineStream()), StrLit(LineStream(), "foo"))) => ok
        }
      }
      
      "descent" >> {
        val tree = Descent(LineStream(), NumLit(LineStream(), "1"), "a")
        bindRoot(tree, tree)
        
        shakeTree(tree) must beLike {
          case Descent(LineStream(), NumLit(LineStream(), "1"), "a") => ok
        }
      }
      
      "metadescent" >> {
        val tree = MetaDescent(LineStream(), NumLit(LineStream(), "1"), "a")
        bindRoot(tree, tree)
        
        shakeTree(tree) must beLike {
          case MetaDescent(LineStream(), NumLit(LineStream(), "1"), "a") => ok
        }
      }
      
      "deref" >> {
        val tree = Deref(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2"))
        bindRoot(tree, tree)
        
        shakeTree(tree) must beLike {
          case Deref(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2")) => ok
        }
      }
      
      "dispatch" >> {
        val tree = Dispatch(LineStream(), Identifier(Vector(), "foo"), Vector(NumLit(LineStream(), "1"), NumLit(LineStream(), "2")))
        bindRoot(tree, tree)
        
        shakeTree(tree) must beLike {
          case Dispatch(LineStream(), Identifier(Vector(), "foo"), Vector(NumLit(LineStream(), "1"), NumLit(LineStream(), "2"))) => ok
        }
      }
      
      "where" >> {
        val tree = Where(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2"))
        bindRoot(tree, tree)
        
        shakeTree(tree) must beLike {
          case Where(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2")) => ok
        }
      }      
      "with" >> {
        val tree = With(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2"))
        bindRoot(tree, tree)
        
        shakeTree(tree) must beLike {
          case With(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2")) => ok
        }
      }      
      "union" >> {
        val tree = Union(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2"))
        bindRoot(tree, tree)
        
        shakeTree(tree) must beLike {
          case Union(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2")) => ok
        }
      }      
      "intersect" >> {
        val tree = Intersect(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2"))
        bindRoot(tree, tree)
        
        shakeTree(tree) must beLike {
          case Intersect(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2")) => ok
        }
      }      
      "difference" >> {
        val tree = Difference(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2"))
        bindRoot(tree, tree)
        
        shakeTree(tree) must beLike {
          case Difference(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2")) => ok
        }
      }
      
      "addition" >> {
        val tree = Add(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2"))
        bindRoot(tree, tree)
        
        shakeTree(tree) must beLike {
          case Add(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2")) => ok
        }
      }
      
      "subtraction" >> {
        val tree = Sub(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2"))
        bindRoot(tree, tree)
        
        shakeTree(tree) must beLike {
          case Sub(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2")) => ok
        }
      }
      
      "multiplication" >> {
        val tree = Mul(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2"))
        bindRoot(tree, tree)
        
        shakeTree(tree) must beLike {
          case Mul(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2")) => ok
        }
      }
      
      "division" >> {
        val tree = Div(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2"))
        bindRoot(tree, tree)
        
        shakeTree(tree) must beLike {
          case Div(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2")) => ok
        }
      }
      
      "less-than" >> {
        val tree = Lt(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2"))
        bindRoot(tree, tree)
        
        shakeTree(tree) must beLike {
          case Lt(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2")) => ok
        }
      }
      
      "less-than-or-equal" >> {
        val tree = LtEq(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2"))
        bindRoot(tree, tree)
        
        shakeTree(tree) must beLike {
          case LtEq(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2")) => ok
        }
      }
      
      "greater-than" >> {
        val tree = Gt(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2"))
        bindRoot(tree, tree)
        
        shakeTree(tree) must beLike {
          case Gt(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2")) => ok
        }
      }
      
      "greater-than-equal" >> {
        val tree = GtEq(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2"))
        bindRoot(tree, tree)
        
        shakeTree(tree) must beLike {
          case GtEq(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2")) => ok
        }
      }
      
      "equality" >> {
        val tree = Eq(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2"))
        bindRoot(tree, tree)
        
        shakeTree(tree) must beLike {
          case Eq(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2")) => ok
        }
      }
      
      "not-equality" >> {
        val tree = NotEq(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2"))
        bindRoot(tree, tree)
        
        shakeTree(tree) must beLike {
          case NotEq(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2")) => ok
        }
      }
      
      "boolean and" >> {
        val tree = And(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2"))
        bindRoot(tree, tree)
        
        shakeTree(tree) must beLike {
          case And(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2")) => ok
        }
      }
      
      "boolean or" >> {
        val tree = Or(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2"))
        bindRoot(tree, tree)
        
        shakeTree(tree) must beLike {
          case Or(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2")) => ok
        }
      }
      
      "complementation" >> {
        val tree = Comp(LineStream(), NumLit(LineStream(), "1"))
        bindRoot(tree, tree)
        
        shakeTree(tree) must beLike {
          case Comp(LineStream(), NumLit(LineStream(), "1")) => ok
        }
      }
      
      "negation" >> {
        val tree = Neg(LineStream(), NumLit(LineStream(), "1"))
        bindRoot(tree, tree)
        
        shakeTree(tree) must beLike {
          case Neg(LineStream(), NumLit(LineStream(), "1")) => ok
        }
      }
    }
    
    "eliminate parentheticals" in {
      val tree = Paren(LineStream(), NumLit(LineStream(), "1"))
      bindRoot(tree, tree)
      
      shakeTree(tree) must beLike {
        case NumLit(LineStream(), "1") => ok
      }
    }
    
    "eliminate let when not found in scope" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), NumLit(LineStream(), "24"))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree) 
      result must beLike {
        case NumLit(LineStream(), "24") => ok
      }
      
      result.errors mustEqual Set(UnusedLetBinding(Identifier(Vector(), "a")))
    }
    
    "preserve let when found in scope" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result must beLike {
        case Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector())) => ok
      }
      
      result.errors must beEmpty
    }
    
    "accept a basic solve" in {
      val tree = Solve(LineStream(), Vector(TicVar(LineStream(), "'a")), Add(LineStream(), TicVar(LineStream(), "'a"), NumLit(LineStream(), "42")))
      bindRoot(tree, tree)

      val results = shakeTree(tree)
      results.errors must beEmpty
      
      results must beLike {
        case Solve(LineStream(), Vector(TicVar(LineStream(), "'a")), Add(LineStream(), TicVar(LineStream(), "'a"), NumLit(LineStream(), "42"))) => ok
      }
    }

    "detect unused tic variable in solve" in {
      val tree = Solve(LineStream(), Vector(TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b")), Add(LineStream(), TicVar(LineStream(), "'a"), NumLit(LineStream(), "42")))
      bindRoot(tree, tree)

      val results = shakeTree(tree)
      results.errors mustEqual Set(UnusedTicVariable("'b"))
    }  

    "detect unused formal in let" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector("b", "c"), Add(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "c"), Vector()), NumLit(LineStream(), "42")), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector(NullLit(LineStream()), NullLit(LineStream()))))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result.errors mustEqual Set(UnusedFormalBinding(Identifier(Vector(), "b")))
    }
    
    "avoid false negatives with used formal" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector("b", "c"), Add(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "b"), Vector()), Dispatch(LineStream(), Identifier(Vector(), "c"), Vector())), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector(NullLit(LineStream()), NullLit(LineStream()))))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result.errors must beEmpty
    }
  }
  
  "tree shaking at depth" should {
    "eliminate let when not found in scope in new" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), New(LineStream(), NumLit(LineStream(), "24")))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result must beLike {
        case New(LineStream(), NumLit(LineStream(), "24")) => ok
      }
      
      result.errors mustEqual Set(UnusedLetBinding(Identifier(Vector(), "a")))
    }
    
    "preserve let when found in scope in new" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), New(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector())))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result must beLike {
        case Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), New(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()))) => ok
      }
      
      result.errors must beEmpty
    }
    
    "detect unused formal in new" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector("a", "b"), New(LineStream(), Add(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "42"))), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector(NullLit(LineStream()), NullLit(LineStream()))))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result.errors mustEqual Set(UnusedFormalBinding(Identifier(Vector(), "b")))
    }    

    "detect unused tic-variable from solve in new" in {
      val tree = Solve(LineStream(), Vector(TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b")), New(LineStream(), Add(LineStream(), TicVar(LineStream(), "'a"), NumLit(LineStream(), "42"))))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result.errors mustEqual Set(UnusedTicVariable("'b"))
    }
    
    "eliminate let when not found in scope in relate" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Relate(LineStream(), NumLit(LineStream(), "24"), NumLit(LineStream(), "25"), NumLit(LineStream(), "26")))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result must beLike {
        case Relate(LineStream(), NumLit(LineStream(), "24"), NumLit(LineStream(), "25"), NumLit(LineStream(), "26")) => ok
      }
      
      result.errors mustEqual Set(UnusedLetBinding(Identifier(Vector(), "a")))
    }
    
    "preserve let when found in scope in relate" in {
      {
        val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Relate(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "24"), NumLit(LineStream(), "25")))
        bindRoot(tree, tree)
        
        val result = shakeTree(tree)
        result must beLike {
          case Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Relate(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "24"), NumLit(LineStream(), "25"))) => ok
        }
        
        result.errors must beEmpty
      }
      
      {
        val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Relate(LineStream(), NumLit(LineStream(), "24"), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "25")))
        bindRoot(tree, tree)
        
        val result = shakeTree(tree)
        result must beLike {
          case Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Relate(LineStream(), NumLit(LineStream(), "24"), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "25"))) => ok
        }
        
        result.errors must beEmpty
      }
      
      {
        val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Relate(LineStream(), NumLit(LineStream(), "24"), NumLit(LineStream(), "25"), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector())))
        bindRoot(tree, tree)
        
        val result = shakeTree(tree)
        result must beLike {
          case Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Relate(LineStream(), NumLit(LineStream(), "24"), NumLit(LineStream(), "25"), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()))) => ok
        }
        
        result.errors must beEmpty
      }
    }
    
    "detect unused formal parameter in relate" in {
      {
        val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector("a", "b"), Relate(LineStream(), Add(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "42")), NumLit(LineStream(), "24"), NumLit(LineStream(), "25")), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector(NullLit(LineStream()), NullLit(LineStream()))))
        bindRoot(tree, tree)
        
        val result = shakeTree(tree)
        result.errors mustEqual Set(UnusedFormalBinding(Identifier(Vector(), "b")))
      }
      
      {
        val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector("a", "b"), Relate(LineStream(), NumLit(LineStream(), "24"), Add(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "42")), NumLit(LineStream(), "25")), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector(NullLit(LineStream()), NullLit(LineStream()))))
        bindRoot(tree, tree)
        
        val result = shakeTree(tree)
        result.errors mustEqual Set(UnusedFormalBinding(Identifier(Vector(), "b")))
      }
      
      {
        val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector("a", "b"), Relate(LineStream(), NumLit(LineStream(), "24"), NumLit(LineStream(), "25"), Add(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "42"))), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector(NullLit(LineStream()), NullLit(LineStream()))))
        bindRoot(tree, tree)
        
        val result = shakeTree(tree)
        result.errors mustEqual Set(UnusedFormalBinding(Identifier(Vector(), "b")))
      }
    }
    
    "detect unused tic-variable from solve in relate" in {
      {
        val tree = Solve(LineStream(), Vector(TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b")), Relate(LineStream(), Add(LineStream(), TicVar(LineStream(), "'a"), NumLit(LineStream(), "42")), NumLit(LineStream(), "24"), NumLit(LineStream(), "25")))
        bindRoot(tree, tree)
        
        val result = shakeTree(tree)
        result.errors mustEqual Set(UnusedTicVariable("'b"))
      }
      
      {
        val tree = Solve(LineStream(), Vector(TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b")), Relate(LineStream(), NumLit(LineStream(), "24"), Add(LineStream(), TicVar(LineStream(), "'a"), NumLit(LineStream(), "42")), NumLit(LineStream(), "25")))
        bindRoot(tree, tree)
        
        val result = shakeTree(tree)
        result.errors mustEqual Set(UnusedTicVariable("'b"))
      }
      
      {
        val tree = Solve(LineStream(), Vector(TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b")), Relate(LineStream(), NumLit(LineStream(), "24"), NumLit(LineStream(), "25"), Add(LineStream(), TicVar(LineStream(), "'a"), NumLit(LineStream(), "42"))))
        bindRoot(tree, tree)
        
        val result = shakeTree(tree)
        result.errors mustEqual Set(UnusedTicVariable("'b"))
      }
    }
    
    "eliminate let when not found in scope in object definition" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), ObjectDef(LineStream(), Vector("foo" -> NumLit(LineStream(), "24"))))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result must beLike {
        case ObjectDef(LineStream(), Vector(("foo", NumLit(LineStream(), "24")))) => ok
      }
      
      result.errors mustEqual Set(UnusedLetBinding(Identifier(Vector(), "a")))
    }
    
    "preserve let when found in scope in object definition" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), ObjectDef(LineStream(), Vector("foo" -> Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()))))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result must beLike {
        case Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), ObjectDef(LineStream(), Vector(("foo", Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()))))) => ok
      }
      
      result.errors must beEmpty
    }
    
    "detect unused formal parameter in object definition" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector("a", "b"), ObjectDef(LineStream(), Vector("foo" -> Add(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "42")))), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector(NullLit(LineStream()), NullLit(LineStream()))))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result.errors mustEqual Set(UnusedFormalBinding(Identifier(Vector(), "b")))
    }    

    "detect unused tic-variable from solve in object definition" in {
      val tree = Solve(LineStream(), Vector(TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b")), ObjectDef(LineStream(), Vector("foo" -> Add(LineStream(), TicVar(LineStream(), "'a"), NumLit(LineStream(), "42")))))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result.errors mustEqual Set(UnusedTicVariable("'b"))
    }
    
    "eliminate let when not found in scope in array definition" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), ArrayDef(LineStream(), Vector(NumLit(LineStream(), "24"))))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result must beLike {
        case ArrayDef(LineStream(), Vector(NumLit(LineStream(), "24"))) => ok
      }
      
      result.errors mustEqual Set(UnusedLetBinding(Identifier(Vector(), "a")))
    }
    
    "preserve let when found in scope in array definition" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), ArrayDef(LineStream(), Vector(Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()))))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result must beLike {
        case Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), ArrayDef(LineStream(), Vector(Dispatch(LineStream(), Identifier(Vector(), "a"), Vector())))) => ok
      }
      
      result.errors must beEmpty
    }
    
    "detect unused formal parameter in array definition" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector("a", "b"), ArrayDef(LineStream(), Vector(Add(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "42")))), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector(NullLit(LineStream()), NullLit(LineStream()))))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result.errors mustEqual Set(UnusedFormalBinding(Identifier(Vector(), "b")))
    }

    "detect unused tic-variable from solve in array definition" in {
      val tree = Solve(LineStream(), Vector(TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b")), ArrayDef(LineStream(), Vector(Add(LineStream(), TicVar(LineStream(), "'a"), NumLit(LineStream(), "42")))))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result.errors mustEqual Set(UnusedTicVariable("'b"))
    }
    
    "eliminate let when not found in scope in descent" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Descent(LineStream(), NumLit(LineStream(), "24"), "foo"))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result must beLike {
        case Descent(LineStream(), NumLit(LineStream(), "24"), "foo") => ok
      }
      
      result.errors mustEqual Set(UnusedLetBinding(Identifier(Vector(), "a")))
    }
    
    "preserve let when found in scope in descent" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Descent(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), "foo"))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result must beLike {
        case Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Descent(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), "foo")) => ok
      }
      
      result.errors must beEmpty
    }
    
    "detect unused formal parameter in descent" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector("a", "b"), Descent(LineStream(), Add(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "42")), "foo"), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector(NullLit(LineStream()), NullLit(LineStream()))))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result.errors mustEqual Set(UnusedFormalBinding(Identifier(Vector(), "b")))
    }

    "detect unused tic-variable from solve in descent" in {
      val tree = Solve(LineStream(), Vector(TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b")), Descent(LineStream(), Add(LineStream(), TicVar(LineStream(), "'a"), NumLit(LineStream(), "42")), "foo"))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result.errors mustEqual Set(UnusedTicVariable("'b"))
    }
    
    "eliminate let when not found in scope in metadescent" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), MetaDescent(LineStream(), NumLit(LineStream(), "24"), "foo"))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result must beLike {
        case MetaDescent(LineStream(), NumLit(LineStream(), "24"), "foo") => ok
      }
      
      result.errors mustEqual Set(UnusedLetBinding(Identifier(Vector(), "a")))
    }
    
    "preserve let when found in scope in metadescent" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), MetaDescent(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), "foo"))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result must beLike {
        case Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), MetaDescent(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), "foo")) => ok
      }
      
      result.errors must beEmpty
    }
    
    "detect unused tic-variable from let in metadescent" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector("'a", "'b"), MetaDescent(LineStream(), Add(LineStream(), TicVar(LineStream(), "'a"), NumLit(LineStream(), "42")), "foo"), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result.errors mustEqual Set(UnusedTicVariable("'b"))
    }

    "detect unused tic-variable from solve in metadescent" in {
      val tree = Solve(LineStream(), Vector(TicVar(LineStream(), "'a")), Solve(LineStream(), Vector(TicVar(LineStream(), "'b")), MetaDescent(LineStream(), Add(LineStream(), TicVar(LineStream(), "'a"), NumLit(LineStream(), "42")), "foo")))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result.errors mustEqual Set(UnusedTicVariable("'b"))
    }
    
    "eliminate let when not found in scope in deref" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Deref(LineStream(), NumLit(LineStream(), "24"), NumLit(LineStream(), "25")))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result must beLike {
        case Deref(LineStream(), NumLit(LineStream(), "24"), NumLit(LineStream(), "25")) => ok
      }
      
      result.errors mustEqual Set(UnusedLetBinding(Identifier(Vector(), "a")))
    }
    
    "preserve let when found in scope in deref" in {
      {
        val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Deref(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "24")))
        bindRoot(tree, tree)
        
        val result = shakeTree(tree)
        result must beLike {
          case Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Deref(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "24"))) => ok
        }
        
        result.errors must beEmpty
      }
      
      {
        val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Deref(LineStream(), NumLit(LineStream(), "24"), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector())))
        bindRoot(tree, tree)
        
        val result = shakeTree(tree)
        result must beLike {
          case Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Deref(LineStream(), NumLit(LineStream(), "24"), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()))) => ok
        }
        
        result.errors must beEmpty
      }
    }
    
    "detect unused formal binding in deref" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector("a", "b"), Deref(LineStream(), Add(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "42")), NumLit(LineStream(), "42")), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector(NullLit(LineStream()), NullLit(LineStream()))))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result.errors mustEqual Set(UnusedFormalBinding(Identifier(Vector(), "b")))
    }    

    "detect unused tic-variable from solve in deref" in {
      val tree = Solve(LineStream(), Vector(TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b")), Deref(LineStream(), Add(LineStream(), TicVar(LineStream(), "'a"), NumLit(LineStream(), "42")), NumLit(LineStream(), "42")))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result.errors mustEqual Set(UnusedTicVariable("'b"))
    }
    
    "eliminate let when not found in scope in dispatch" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Dispatch(LineStream(), Identifier(Vector(), "count"), Vector(NumLit(LineStream(), "24"))))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result must beLike {
        case Dispatch(LineStream(), Identifier(Vector(), "count"), Vector(NumLit(LineStream(), "24"))) => ok
      }
      
      result.errors mustEqual Set(UnusedLetBinding(Identifier(Vector(), "a")))
    }
    
    "preserve let when found in scope in dispatch" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Dispatch(LineStream(), Identifier(Vector(), "count"), Vector(Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()))))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result must beLike {
        case Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Dispatch(LineStream(), Identifier(Vector(), "count"), Vector(Dispatch(LineStream(), Identifier(Vector(), "a"), Vector())))) => ok
      }
      
      result.errors must beEmpty
    }
    
    "detect unused formal parameter in dispatch" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector("a", "b"), Dispatch(LineStream(), Identifier(Vector(), "count"), Vector(Add(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "42")))), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector(NullLit(LineStream()), NullLit(LineStream()))))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result.errors mustEqual Set(UnusedFormalBinding(Identifier(Vector(), "b")))
    }   

    "detect unused tic-variable from solve in dispatch" in {
      val tree = Solve(LineStream(), Vector(TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b")), Dispatch(LineStream(), Identifier(Vector(), "count"), Vector(Add(LineStream(), TicVar(LineStream(), "'a"), NumLit(LineStream(), "42")))))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result.errors mustEqual Set(UnusedTicVariable("'b"))
    }
    
    "eliminate let when not found in scope in where" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Where(LineStream(), NumLit(LineStream(), "24"), NumLit(LineStream(), "25")))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result must beLike {
        case Where(LineStream(), NumLit(LineStream(), "24"), NumLit(LineStream(), "25")) => ok
      }
      
      result.errors mustEqual Set(UnusedLetBinding(Identifier(Vector(), "a")))
    }
    
    "preserve let when found in scope in where" in {
      {
        val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Where(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "24")))
        bindRoot(tree, tree)
        
        val result = shakeTree(tree)
        result must beLike {
          case Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Where(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "24"))) => ok
        }
        
        result.errors must beEmpty
      }
      
      {
        val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Where(LineStream(), NumLit(LineStream(), "24"), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector())))
        bindRoot(tree, tree)
        
        val result = shakeTree(tree)
        result must beLike {
          case Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Where(LineStream(), NumLit(LineStream(), "24"), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()))) => ok
        }
        
        result.errors must beEmpty
      }
    }
    
    "detect unused formal parameter in where" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector("a", "b"), Where(LineStream(), Add(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "42")), NumLit(LineStream(), "24")), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector(NullLit(LineStream()), NullLit(LineStream()))))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result.errors mustEqual Set(UnusedFormalBinding(Identifier(Vector(), "b")))
    }
        
    "detect unused tic-variable from solve in where" in {
      val tree = Solve(LineStream(), Vector(TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b")), Where(LineStream(), Add(LineStream(), TicVar(LineStream(), "'a"), NumLit(LineStream(), "42")), NumLit(LineStream(), "24")))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result.errors mustEqual Set(UnusedTicVariable("'b"))
    }
    
    "eliminate let when not found in scope in addition" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Add(LineStream(), NumLit(LineStream(), "24"), NumLit(LineStream(), "25")))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result must beLike {
        case Add(LineStream(), NumLit(LineStream(), "24"), NumLit(LineStream(), "25")) => ok
      }
      
      result.errors mustEqual Set(UnusedLetBinding(Identifier(Vector(), "a")))
    }
    
    "preserve let when found in scope in addition" in {
      {
        val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Add(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "24")))
        bindRoot(tree, tree)
        
        val result = shakeTree(tree)
        result must beLike {
          case Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Add(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "24"))) => ok
        }
        
        result.errors must beEmpty
      }
      
      {
        val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Add(LineStream(), NumLit(LineStream(), "24"), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector())))
        bindRoot(tree, tree)
        
        val result = shakeTree(tree)
        result must beLike {
          case Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Add(LineStream(), NumLit(LineStream(), "24"), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()))) => ok
        }
        
        result.errors must beEmpty
      }
    }
    
    "detect unused formal parameter in addition" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector("a", "b"), Add(LineStream(), Add(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "42")), NumLit(LineStream(), "24")), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector(NullLit(LineStream()), NullLit(LineStream()))))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result.errors mustEqual Set(UnusedFormalBinding(Identifier(Vector(), "b")))
    }
        
    "detect unused tic-variable from solve in addition" in {
      val tree = Solve(LineStream(), Vector(TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b")), Add(LineStream(), Add(LineStream(), TicVar(LineStream(), "'a"), NumLit(LineStream(), "42")), NumLit(LineStream(), "24")))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result.errors mustEqual Set(UnusedTicVariable("'b"))
    }
    
    "eliminate let when not found in scope in subtraction" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Sub(LineStream(), NumLit(LineStream(), "24"), NumLit(LineStream(), "25")))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result must beLike {
        case Sub(LineStream(), NumLit(LineStream(), "24"), NumLit(LineStream(), "25")) => ok
      }
      
      result.errors mustEqual Set(UnusedLetBinding(Identifier(Vector(), "a")))
    }
    
    "preserve let when found in scope in subtraction" in {
      {
        val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Sub(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "24")))
        bindRoot(tree, tree)
        
        val result = shakeTree(tree)
        result must beLike {
          case Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Sub(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "24"))) => ok
        }
        
        result.errors must beEmpty
      }
      
      {
        val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Sub(LineStream(), NumLit(LineStream(), "24"), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector())))
        bindRoot(tree, tree)
        
        val result = shakeTree(tree)
        result must beLike {
          case Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Sub(LineStream(), NumLit(LineStream(), "24"), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()))) => ok
        }
        
        result.errors must beEmpty
      }
    }
    
    "detect unused formal parameter in subtraction" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector("a", "b"), Sub(LineStream(), Sub(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "42")), NumLit(LineStream(), "24")), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector(NullLit(LineStream()), NullLit(LineStream()))))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result.errors mustEqual Set(UnusedFormalBinding(Identifier(Vector(), "b")))
    }
    
    
    "detect unused tic-variable from solve in subtraction" in {
      val tree = Solve(LineStream(), Vector(TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b")), Sub(LineStream(), Sub(LineStream(), TicVar(LineStream(), "'a"), NumLit(LineStream(), "42")), NumLit(LineStream(), "24")))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result.errors mustEqual Set(UnusedTicVariable("'b"))
    }
    
    "eliminate let when not found in scope in multiplication" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Mul(LineStream(), NumLit(LineStream(), "24"), NumLit(LineStream(), "25")))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result must beLike {
        case Mul(LineStream(), NumLit(LineStream(), "24"), NumLit(LineStream(), "25")) => ok
      }
      
      result.errors mustEqual Set(UnusedLetBinding(Identifier(Vector(), "a")))
    }
    
    "preserve let when found in scope in multiplication" in {
      {
        val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Mul(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "24")))
        bindRoot(tree, tree)
        
        val result = shakeTree(tree)
        result must beLike {
          case Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Mul(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "24"))) => ok
        }
        
        result.errors must beEmpty
      }
      
      {
        val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Mul(LineStream(), NumLit(LineStream(), "24"), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector())))
        bindRoot(tree, tree)
        
        val result = shakeTree(tree)
        result must beLike {
          case Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Mul(LineStream(), NumLit(LineStream(), "24"), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()))) => ok
        }
        
        result.errors must beEmpty
      }
    }
    
    "detect unused formal parameter in multiplication" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector("a", "b"), Mul(LineStream(), Mul(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "42")), NumLit(LineStream(), "24")), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector(NullLit(LineStream()), NullLit(LineStream()))))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result.errors mustEqual Set(UnusedFormalBinding(Identifier(Vector(), "b")))
    }
        
    "detect unused tic-variable from solve in multiplication" in {
      val tree = Solve(LineStream(), Vector(TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b")), Mul(LineStream(), Mul(LineStream(), TicVar(LineStream(), "'a"), NumLit(LineStream(), "42")), NumLit(LineStream(), "24")))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result.errors mustEqual Set(UnusedTicVariable("'b"))
    }
    
    "eliminate let when not found in scope in division" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Div(LineStream(), NumLit(LineStream(), "24"), NumLit(LineStream(), "25")))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result must beLike {
        case Div(LineStream(), NumLit(LineStream(), "24"), NumLit(LineStream(), "25")) => ok
      }
      
      result.errors mustEqual Set(UnusedLetBinding(Identifier(Vector(), "a")))
    }
    
    "preserve let when found in scope in division" in {
      {
        val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Div(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "24")))
        bindRoot(tree, tree)
        
        val result = shakeTree(tree)
        result must beLike {
          case Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Div(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "24"))) => ok
        }
        
        result.errors must beEmpty
      }
      
      {
        val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Div(LineStream(), NumLit(LineStream(), "24"), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector())))
        bindRoot(tree, tree)
        
        val result = shakeTree(tree)
        result must beLike {
          case Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Div(LineStream(), NumLit(LineStream(), "24"), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()))) => ok
        }
        
        result.errors must beEmpty
      }
    }
    
    "detect unused formal parameter in division" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector("a", "b"), Div(LineStream(), Div(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "42")), NumLit(LineStream(), "24")), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector(NullLit(LineStream()), NullLit(LineStream()))))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result.errors mustEqual Set(UnusedFormalBinding(Identifier(Vector(), "b")))
    }
        
    "detect unused tic-variable from solve in division" in {
      val tree = Solve(LineStream(), Vector(TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b")), Div(LineStream(), Div(LineStream(), TicVar(LineStream(), "'a"), NumLit(LineStream(), "42")), NumLit(LineStream(), "24")))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result.errors mustEqual Set(UnusedTicVariable("'b"))
    }
    
    "eliminate let when not found in scope in less-than" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Lt(LineStream(), NumLit(LineStream(), "24"), NumLit(LineStream(), "25")))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result must beLike {
        case Lt(LineStream(), NumLit(LineStream(), "24"), NumLit(LineStream(), "25")) => ok
      }
      
      result.errors mustEqual Set(UnusedLetBinding(Identifier(Vector(), "a")))
    }
    
    "preserve let when found in scope in less-than" in {
      {
        val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Lt(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "24")))
        bindRoot(tree, tree)
        
        val result = shakeTree(tree)
        result must beLike {
          case Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Lt(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "24"))) => ok
        }
        
        result.errors must beEmpty
      }
      
      {
        val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Lt(LineStream(), NumLit(LineStream(), "24"), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector())))
        bindRoot(tree, tree)
        
        val result = shakeTree(tree)
        result must beLike {
          case Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Lt(LineStream(), NumLit(LineStream(), "24"), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()))) => ok
        }
        
        result.errors must beEmpty
      }
    }
    
    "detect unused formal parameter in less-than" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector("a", "b"), Lt(LineStream(), Lt(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "42")), NumLit(LineStream(), "24")), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector(NullLit(LineStream()), NullLit(LineStream()))))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result.errors mustEqual Set(UnusedFormalBinding(Identifier(Vector(), "b")))
    }
        
    "detect unused tic-variable from solve in less-than" in {
      val tree = Solve(LineStream(), Vector(TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b")), Lt(LineStream(), Lt(LineStream(), TicVar(LineStream(), "'a"), NumLit(LineStream(), "42")), NumLit(LineStream(), "24")))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result.errors mustEqual Set(UnusedTicVariable("'b"))
    }
    
    "eliminate let when not found in scope in less-than-equal" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), LtEq(LineStream(), NumLit(LineStream(), "24"), NumLit(LineStream(), "25")))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result must beLike {
        case LtEq(LineStream(), NumLit(LineStream(), "24"), NumLit(LineStream(), "25")) => ok
      }
      
      result.errors mustEqual Set(UnusedLetBinding(Identifier(Vector(), "a")))
    }
    
    "preserve let when found in scope in less-than-equal" in {
      {
        val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), LtEq(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "24")))
        bindRoot(tree, tree)
        
        val result = shakeTree(tree)
        result must beLike {
          case Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), LtEq(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "24"))) => ok
        }
        
        result.errors must beEmpty
      }
      
      {
        val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), LtEq(LineStream(), NumLit(LineStream(), "24"), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector())))
        bindRoot(tree, tree)
        
        val result = shakeTree(tree)
        result must beLike {
          case Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), LtEq(LineStream(), NumLit(LineStream(), "24"), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()))) => ok
        }
        
        result.errors must beEmpty
      }
    }
    
    "detect unused formal parameter in less-than-equal" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector("a", "b"), LtEq(LineStream(), LtEq(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "42")), NumLit(LineStream(), "24")), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector(NullLit(LineStream()), NullLit(LineStream()))))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result.errors mustEqual Set(UnusedFormalBinding(Identifier(Vector(), "b")))
    }
        
    "detect unused tic-variable from solve in less-than-equal" in {
      val tree = Solve(LineStream(), Vector(TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b")), LtEq(LineStream(), LtEq(LineStream(), TicVar(LineStream(), "'a"), NumLit(LineStream(), "42")), NumLit(LineStream(), "24")))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result.errors mustEqual Set(UnusedTicVariable("'b"))
    }
    
    "eliminate let when not found in scope in greater-than" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Gt(LineStream(), NumLit(LineStream(), "24"), NumLit(LineStream(), "25")))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result must beLike {
        case Gt(LineStream(), NumLit(LineStream(), "24"), NumLit(LineStream(), "25")) => ok
      }
      
      result.errors mustEqual Set(UnusedLetBinding(Identifier(Vector(), "a")))
    }
    
    "preserve let when found in scope in greater-than" in {
      {
        val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Gt(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "24")))
        bindRoot(tree, tree)
        
        val result = shakeTree(tree)
        result must beLike {
          case Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Gt(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "24"))) => ok
        }
        
        result.errors must beEmpty
      }
      
      {
        val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Gt(LineStream(), NumLit(LineStream(), "24"), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector())))
        bindRoot(tree, tree)
        
        val result = shakeTree(tree)
        result must beLike {
          case Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Gt(LineStream(), NumLit(LineStream(), "24"), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()))) => ok
        }
        
        result.errors must beEmpty
      }
    }
    
    "detect unused formal parameter in greater-than" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector("a", "b"), Gt(LineStream(), Gt(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "42")), NumLit(LineStream(), "24")), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector(NullLit(LineStream()), NullLit(LineStream()))))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result.errors mustEqual Set(UnusedFormalBinding(Identifier(Vector(), "b")))
    }
        
    "detect unused tic-variable from solve in greater-than" in {
      val tree = Solve(LineStream(), Vector(TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b")), Gt(LineStream(), Gt(LineStream(), TicVar(LineStream(), "'a"), NumLit(LineStream(), "42")), NumLit(LineStream(), "24")))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result.errors mustEqual Set(UnusedTicVariable("'b"))
    }
    
    "eliminate let when not found in scope in greater-than-equal" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), GtEq(LineStream(), NumLit(LineStream(), "24"), NumLit(LineStream(), "25")))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result must beLike {
        case GtEq(LineStream(), NumLit(LineStream(), "24"), NumLit(LineStream(), "25")) => ok
      }
      
      result.errors mustEqual Set(UnusedLetBinding(Identifier(Vector(), "a")))
    }
    
    "preserve let when found in scope in greater-than-equal" in {
      {
        val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), GtEq(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "24")))
        bindRoot(tree, tree)
        
        val result = shakeTree(tree)
        result must beLike {
          case Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), GtEq(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "24"))) => ok
        }
        
        result.errors must beEmpty
      }
      
      {
        val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), GtEq(LineStream(), NumLit(LineStream(), "24"), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector())))
        bindRoot(tree, tree)
        
        val result = shakeTree(tree)
        result must beLike {
          case Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), GtEq(LineStream(), NumLit(LineStream(), "24"), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()))) => ok
        }
        
        result.errors must beEmpty
      }
    }
    
    "detect unused formal parameter in greater-than-equal" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector("a", "b"), GtEq(LineStream(), GtEq(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "42")), NumLit(LineStream(), "24")), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector(NullLit(LineStream()), NullLit(LineStream()))))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result.errors mustEqual Set(UnusedFormalBinding(Identifier(Vector(), "b")))
    }
        
    "detect unused tic-variable from solve in greater-than-equal" in {
      val tree = Solve(LineStream(), Vector(TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b")), GtEq(LineStream(), GtEq(LineStream(), TicVar(LineStream(), "'a"), NumLit(LineStream(), "42")), NumLit(LineStream(), "24")))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result.errors mustEqual Set(UnusedTicVariable("'b"))
    }
    
    "eliminate let when not found in scope in equality" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Eq(LineStream(), NumLit(LineStream(), "24"), NumLit(LineStream(), "25")))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result must beLike {
        case Eq(LineStream(), NumLit(LineStream(), "24"), NumLit(LineStream(), "25")) => ok
      }
      
      result.errors mustEqual Set(UnusedLetBinding(Identifier(Vector(), "a")))
    }
    
    "preserve let when found in scope in equality" in {
      {
        val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Eq(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "24")))
        bindRoot(tree, tree)
        
        val result = shakeTree(tree)
        result must beLike {
          case Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Eq(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "24"))) => ok
        }
        
        result.errors must beEmpty
      }
      
      {
        val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Eq(LineStream(), NumLit(LineStream(), "24"), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector())))
        bindRoot(tree, tree)
        
        val result = shakeTree(tree)
        result must beLike {
          case Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Eq(LineStream(), NumLit(LineStream(), "24"), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()))) => ok
        }
        
        result.errors must beEmpty
      }
    }
    
    "detect unused formal parameter in equality" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector("a", "b"), Eq(LineStream(), Eq(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "42")), NumLit(LineStream(), "24")), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector(NullLit(LineStream()), NullLit(LineStream()))))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result.errors mustEqual Set(UnusedFormalBinding(Identifier(Vector(), "b")))
    }
        
    "detect unused tic-variable from solve in equality" in {
      val tree = Solve(LineStream(), Vector(TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b")), Eq(LineStream(), Eq(LineStream(), TicVar(LineStream(), "'a"), NumLit(LineStream(), "42")), NumLit(LineStream(), "24")))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result.errors mustEqual Set(UnusedTicVariable("'b"))
    }
    
    "eliminate let when not found in scope in not equality" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), NotEq(LineStream(), NumLit(LineStream(), "24"), NumLit(LineStream(), "25")))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result must beLike {
        case NotEq(LineStream(), NumLit(LineStream(), "24"), NumLit(LineStream(), "25")) => ok
      }
      
      result.errors mustEqual Set(UnusedLetBinding(Identifier(Vector(), "a")))
    }
    
    "preserve let when found in scope in not equality" in {
      {
        val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), NotEq(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "24")))
        bindRoot(tree, tree)
        
        val result = shakeTree(tree)
        result must beLike {
          case Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), NotEq(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "24"))) => ok
        }
        
        result.errors must beEmpty
      }
      
      {
        val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), NotEq(LineStream(), NumLit(LineStream(), "24"), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector())))
        bindRoot(tree, tree)
        
        val result = shakeTree(tree)
        result must beLike {
          case Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), NotEq(LineStream(), NumLit(LineStream(), "24"), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()))) => ok
        }
        
        result.errors must beEmpty
      }
    }
    
    "detect unused formal parameter in not equality" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector("a", "b"), NotEq(LineStream(), NotEq(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "42")), NumLit(LineStream(), "24")), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector(NullLit(LineStream()), NullLit(LineStream()))))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result.errors mustEqual Set(UnusedFormalBinding(Identifier(Vector(), "b")))
    }
        
    "detect unused tic-variable from solve in not equality" in {
      val tree = Solve(LineStream(), Vector(TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b")), NotEq(LineStream(), NotEq(LineStream(), TicVar(LineStream(), "'a"), NumLit(LineStream(), "42")), NumLit(LineStream(), "24")))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result.errors mustEqual Set(UnusedTicVariable("'b"))
    }
    
    "eliminate let when not found in scope in boolean and" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), And(LineStream(), NumLit(LineStream(), "24"), NumLit(LineStream(), "25")))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result must beLike {
        case And(LineStream(), NumLit(LineStream(), "24"), NumLit(LineStream(), "25")) => ok
      }
      
      result.errors mustEqual Set(UnusedLetBinding(Identifier(Vector(), "a")))
    }
    
    "preserve let when found in scope in boolean and" in {
      {
        val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), And(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "24")))
        bindRoot(tree, tree)
        
        val result = shakeTree(tree)
        result must beLike {
          case Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), And(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "24"))) => ok
        }
        
        result.errors must beEmpty
      }
      
      {
        val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), And(LineStream(), NumLit(LineStream(), "24"), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector())))
        bindRoot(tree, tree)
        
        val result = shakeTree(tree)
        result must beLike {
          case Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), And(LineStream(), NumLit(LineStream(), "24"), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()))) => ok
        }
        
        result.errors must beEmpty
      }
    }
    
    "detect unused formal parameter in boolean and" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector("a", "b"), And(LineStream(), And(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "42")), NumLit(LineStream(), "24")), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector(NullLit(LineStream()), NullLit(LineStream()))))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result.errors mustEqual Set(UnusedFormalBinding(Identifier(Vector(), "b")))
    }
        
    "detect unused tic-variable from solve in boolean and" in {
      val tree = Solve(LineStream(), Vector(TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b")), And(LineStream(), And(LineStream(), TicVar(LineStream(), "'a"), NumLit(LineStream(), "42")), NumLit(LineStream(), "24")))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result.errors mustEqual Set(UnusedTicVariable("'b"))
    }
    
    "eliminate let when not found in scope in boolean or" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Or(LineStream(), NumLit(LineStream(), "24"), NumLit(LineStream(), "25")))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result must beLike {
        case Or(LineStream(), NumLit(LineStream(), "24"), NumLit(LineStream(), "25")) => ok
      }
      
      result.errors mustEqual Set(UnusedLetBinding(Identifier(Vector(), "a")))
    }
    
    "preserve let when found in scope in boolean or" in {
      {
        val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Or(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "24")))
        bindRoot(tree, tree)
        
        val result = shakeTree(tree)
        result must beLike {
          case Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Or(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "24"))) => ok
        }
        
        result.errors must beEmpty
      }
      
      {
        val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Or(LineStream(), NumLit(LineStream(), "24"), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector())))
        bindRoot(tree, tree)
        
        val result = shakeTree(tree)
        result must beLike {
          case Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Or(LineStream(), NumLit(LineStream(), "24"), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()))) => ok
        }
        
        result.errors must beEmpty
      }
    }
    
    "detect unused formal parameter in boolean or" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector("a", "b"), Or(LineStream(), Or(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "42")), NumLit(LineStream(), "24")), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector(NullLit(LineStream()), NullLit(LineStream()))))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result.errors mustEqual Set(UnusedFormalBinding(Identifier(Vector(), "b")))
    }
        
    "detect unused tic-variable from solve in boolean or" in {
      val tree = Solve(LineStream(), Vector(TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b")), Or(LineStream(), Or(LineStream(), TicVar(LineStream(), "'a"), NumLit(LineStream(), "42")), NumLit(LineStream(), "24")))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result.errors mustEqual Set(UnusedTicVariable("'b"))
    }
    
    "eliminate let when not found in scope in complement" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Comp(LineStream(), NumLit(LineStream(), "24")))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result must beLike {
        case Comp(LineStream(), NumLit(LineStream(), "24")) => ok
      }
      
      result.errors mustEqual Set(UnusedLetBinding(Identifier(Vector(), "a")))
    }
    
    "preserve let when found in scope in complement" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Comp(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector())))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result must beLike {
        case Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Comp(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()))) => ok
      }
      
      result.errors must beEmpty
    }
    
    "detect unused formal parameter in complement" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector("a", "b"), Comp(LineStream(), Add(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "42"))), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector(NullLit(LineStream()), NullLit(LineStream()))))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result.errors mustEqual Set(UnusedFormalBinding(Identifier(Vector(), "b")))
    }
        
    "detect unused tic-variable from solve in complement" in {
      val tree = Solve(LineStream(), Vector(TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b")), Comp(LineStream(), Add(LineStream(), TicVar(LineStream(), "'a"), NumLit(LineStream(), "42"))))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result.errors mustEqual Set(UnusedTicVariable("'b"))
    }
    
    "eliminate let when not found in scope in negation" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Neg(LineStream(), NumLit(LineStream(), "24")))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result must beLike {
        case Neg(LineStream(), NumLit(LineStream(), "24")) => ok
      }
      
      result.errors mustEqual Set(UnusedLetBinding(Identifier(Vector(), "a")))
    }
    
    "preserve let when found in scope in negation" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Neg(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector())))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result must beLike {
        case Let(LineStream(), Identifier(Vector(), "a"), Vector(), NumLit(LineStream(), "42"), Neg(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()))) => ok
      }
      
      result.errors must beEmpty
    }
    
    "detect unused formal parameter in negation" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector("a", "b"), Neg(LineStream(), Add(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), NumLit(LineStream(), "42"))), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector(NullLit(LineStream()), NullLit(LineStream()))))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result.errors mustEqual Set(UnusedFormalBinding(Identifier(Vector(), "b")))
    }    

    "detect unused tic-variable from solve in negation" in {
      val tree = Solve(LineStream(), Vector(TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b")), Neg(LineStream(), Add(LineStream(), TicVar(LineStream(), "'a"), NumLit(LineStream(), "42"))))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result.errors mustEqual Set(UnusedTicVariable("'b"))
    }
  }
  
  "name binding after tree shake" should {
    "re-bind formals" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector("b"), Paren(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "b"), Vector())), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector(NullLit(LineStream()))))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result.errors must beEmpty
      
      result must beLike {
        case Let(LineStream(), Identifier(Vector(), "a"), Vector("b"), t @ Dispatch(LineStream(), Identifier(Vector(), "b"), Vector()), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector(NullLit(LineStream())))) =>
          t.binding must beLike { case FormalBinding(`result`) => ok }
      }
    }

    "re-bind tic variables" in {
      val tree = Solve(LineStream(), Vector(TicVar(LineStream(), "'a")), Paren(LineStream(), TicVar(LineStream(), "'a")))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result.errors must beEmpty
      
      result must beLike {
        case Solve(LineStream(), Vector(TicVar(LineStream(), "'a")), t @ TicVar(LineStream(), "'a")) =>
          t.binding must beLike { case SolveBinding(`result`) => ok }
      }
    }
    
    "re-bind dispatch" in {
      val tree = Let(LineStream(), Identifier(Vector(), "a"), Vector("a"), Paren(LineStream(), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector())), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector(NullLit(LineStream()))))
      bindRoot(tree, tree)
      
      val result = shakeTree(tree)
      result.errors must beEmpty
      
      result must beLike {
        case Let(LineStream(), Identifier(Vector(), "a"), Vector("a"), Dispatch(LineStream(), Identifier(Vector(), "a"), Vector()), d @ Dispatch(LineStream(), Identifier(Vector(), "a"), Vector(NullLit(LineStream())))) =>
          d.binding must beLike { case LetBinding(`result`) => ok }
      }
    }
  }
}
