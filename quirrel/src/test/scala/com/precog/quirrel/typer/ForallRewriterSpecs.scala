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

object ForallRewriterSpecs extends Specification with StubPhases with Binder with ForallRewriter with RandomLibrary {
  import ast._

  "forall rewriting" should {
    "rewrite simple case with one tic var" in {
      val tree = Forall(LineStream(), "'a", TicVar(LineStream(), "'a"))
      bindRoot(tree, tree)

      val results = rewriteForall(tree)

      results must beLike {
        case Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a"), 
          TicVar(LineStream(), "'a"), 
          Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())) => ok
      }

      val results2 @ Let(_, _, _, t: TicVar, _) = results
      
      t.binding mustEqual LetBinding(results2)
    }    

    "rewrite expression with two non-adjacent foralls" in {
      val tree = Union(LineStream(), Forall(LineStream(), "'a", TicVar(LineStream(), "'a")), Forall(LineStream(), "'b", TicVar(LineStream(), "'b")))
      bindRoot(tree, tree)

      val results = rewriteForall(tree)

      results must beLike {
        case Union(LineStream(),
          Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a"), 
            TicVar(LineStream(), "'a"), 
            Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())),
          Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'b"), 
            TicVar(LineStream(), "'b"), 
            Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector()))) => ok
      }

      val results2 @ Union(_, l1 @ Let(_, _, _, t1: TicVar, _), l2 @ Let(_, _, _, t2: TicVar, _)) = results
      
      t1.binding mustEqual LetBinding(l1)
      t2.binding mustEqual LetBinding(l2)
    }    

    "leave non-foralls unchanged" in {
      "let" >> {
        val tree = Let(LineStream(), Identifier(Vector(), "foo"), Vector(), 
          NumLit(LineStream(), "42"),
          Dispatch(LineStream(), Identifier(Vector(), "foo"), Vector()))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)

        results must beLike {
          case `tree` => ok
        }
      }
 
      "new" >> {
        val tree = New(LineStream(), NumLit(LineStream(), "42"))
        bindRoot(tree, tree)
        
        rewriteForall(tree) must beLike {
          case New(LineStream(), NumLit(LineStream(), "42")) => ok
        }
      }      
      
      "import" >> {
        val tree = Import(LineStream(), SpecificImport(Vector("std")), NumLit(LineStream(), "12"))
        bindRoot(tree, tree)
        
        rewriteForall(tree) must beLike {
          case Import(LineStream(), SpecificImport(Vector("std")), NumLit(LineStream(), "12"))=> ok
        }
      }
      
      "relate" >> {
        val tree = Relate(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2"), NumLit(LineStream(), "3"))
        bindRoot(tree, tree)
        
        rewriteForall(tree) must beLike {
          case Relate(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2"), NumLit(LineStream(), "3")) => ok
        }
      }
      
      "tic variable" >> {
        val tree = TicVar(LineStream(), "'a")
        bindRoot(tree, tree)
        
        rewriteForall(tree) must beLike {
          case TicVar(LineStream(), "'a") => ok
        }
      }
      
      "string literal" >> {
        val tree = StrLit(LineStream(), "foo")
        bindRoot(tree, tree)
        
        rewriteForall(tree) must beLike {
          case StrLit(LineStream(), "foo") => ok
        }
      }
      
      "num literal" >> {
        val tree = NumLit(LineStream(), "42")
        bindRoot(tree, tree)
        
        rewriteForall(tree) must beLike {
          case NumLit(LineStream(), "42") => ok
        }
      }
      
      "bool literal" >> {
        val tree = BoolLit(LineStream(), true)
        bindRoot(tree, tree)
        
        rewriteForall(tree) must beLike {
          case BoolLit(LineStream(), true) => ok
        }
      }      

      "null literal" >> {
        val tree = NullLit(LineStream())
        bindRoot(tree, tree)
        
        rewriteForall(tree) must beLike {
          case NullLit(LineStream()) => ok
        }
      }
      
      "object with numeric fields" >> {
        val tree = ObjectDef(LineStream(), Vector(("a", NumLit(LineStream(), "1")), ("b", NumLit(LineStream(), "2"))))
        bindRoot(tree, tree)
        
        rewriteForall(tree) must beLike {
          case ObjectDef(LineStream(), Vector(("a", NumLit(LineStream(), "1")), ("b", NumLit(LineStream(), "2")))) => ok
        }
      }

      "object with string and null fields" >> {
        val tree = ObjectDef(LineStream(), Vector(("a", NullLit(LineStream())), ("b", StrLit(LineStream(), "foo"))))
        bindRoot(tree, tree)
        
        rewriteForall(tree) must beLike {
          case ObjectDef(LineStream(), Vector(("a", NullLit(LineStream())), ("b", StrLit(LineStream(), "foo")))) => ok
        }
      }
      
      "array with numeric fields" >> {
        val tree = ArrayDef(LineStream(), Vector(NumLit(LineStream(), "1"), NumLit(LineStream(), "2")))
        bindRoot(tree, tree)
        
        rewriteForall(tree) must beLike {
          case ArrayDef(LineStream(), Vector(NumLit(LineStream(), "1"), NumLit(LineStream(), "2"))) => ok
        }
      }      

      "array with string and null fields" >> {
        val tree = ArrayDef(LineStream(), Vector(NullLit(LineStream()), StrLit(LineStream(), "foo")))
        bindRoot(tree, tree)
        
        rewriteForall(tree) must beLike {
          case ArrayDef(LineStream(), Vector(NullLit(LineStream()), StrLit(LineStream(), "foo"))) => ok
        }
      }
      
      "descent" >> {
        val tree = Descent(LineStream(), NumLit(LineStream(), "1"), "a")
        bindRoot(tree, tree)
        
        rewriteForall(tree) must beLike {
          case Descent(LineStream(), NumLit(LineStream(), "1"), "a") => ok
        }
      }
      
      "metadescent" >> {
        val tree = MetaDescent(LineStream(), NumLit(LineStream(), "1"), "a")
        bindRoot(tree, tree)
        
        rewriteForall(tree) must beLike {
          case MetaDescent(LineStream(), NumLit(LineStream(), "1"), "a") => ok
        }
      }
      
      "deref" >> {
        val tree = Deref(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2"))
        bindRoot(tree, tree)
        
        rewriteForall(tree) must beLike {
          case Deref(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2")) => ok
        }
      }
      
      "dispatch" >> {
        val tree = Dispatch(LineStream(), Identifier(Vector(), "foo"), Vector(NumLit(LineStream(), "1"), NumLit(LineStream(), "2")))
        bindRoot(tree, tree)
        
        rewriteForall(tree) must beLike {
          case Dispatch(LineStream(), Identifier(Vector(), "foo"), Vector(NumLit(LineStream(), "1"), NumLit(LineStream(), "2"))) => ok
        }
      }
      
      "where" >> {
        val tree = Where(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2"))
        bindRoot(tree, tree)
        
        rewriteForall(tree) must beLike {
          case Where(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2")) => ok
        }
      }      
      "with" >> {
        val tree = With(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2"))
        bindRoot(tree, tree)
        
        rewriteForall(tree) must beLike {
          case With(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2")) => ok
        }
      }      
      "union" >> {
        val tree = Union(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2"))
        bindRoot(tree, tree)
        
        rewriteForall(tree) must beLike {
          case Union(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2")) => ok
        }
      }      
      "intersect" >> {
        val tree = Intersect(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2"))
        bindRoot(tree, tree)
        
        rewriteForall(tree) must beLike {
          case Intersect(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2")) => ok
        }
      }      
      "difference" >> {
        val tree = Difference(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2"))
        bindRoot(tree, tree)
        
        rewriteForall(tree) must beLike {
          case Difference(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2")) => ok
        }
      }
      
      "addition" >> {
        val tree = Add(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2"))
        bindRoot(tree, tree)
        
        rewriteForall(tree) must beLike {
          case Add(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2")) => ok
        }
      }
      
      "subtraction" >> {
        val tree = Sub(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2"))
        bindRoot(tree, tree)
        
        rewriteForall(tree) must beLike {
          case Sub(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2")) => ok
        }
      }
      
      "multiplication" >> {
        val tree = Mul(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2"))
        bindRoot(tree, tree)
        
        rewriteForall(tree) must beLike {
          case Mul(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2")) => ok
        }
      }
      
      "division" >> {
        val tree = Div(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2"))
        bindRoot(tree, tree)
        
        rewriteForall(tree) must beLike {
          case Div(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2")) => ok
        }
      }
      
      "less-than" >> {
        val tree = Lt(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2"))
        bindRoot(tree, tree)
        
        rewriteForall(tree) must beLike {
          case Lt(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2")) => ok
        }
      }
      
      "less-than-or-equal" >> {
        val tree = LtEq(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2"))
        bindRoot(tree, tree)
        
        rewriteForall(tree) must beLike {
          case LtEq(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2")) => ok
        }
      }
      
      "greater-than" >> {
        val tree = Gt(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2"))
        bindRoot(tree, tree)
        
        rewriteForall(tree) must beLike {
          case Gt(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2")) => ok
        }
      }
      
      "greater-than-equal" >> {
        val tree = GtEq(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2"))
        bindRoot(tree, tree)
        
        rewriteForall(tree) must beLike {
          case GtEq(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2")) => ok
        }
      }
      
      "equality" >> {
        val tree = Eq(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2"))
        bindRoot(tree, tree)
        
        rewriteForall(tree) must beLike {
          case Eq(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2")) => ok
        }
      }
      
      "not-equality" >> {
        val tree = NotEq(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2"))
        bindRoot(tree, tree)
        
        rewriteForall(tree) must beLike {
          case NotEq(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2")) => ok
        }
      }
      
      "boolean and" >> {
        val tree = And(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2"))
        bindRoot(tree, tree)
        
        rewriteForall(tree) must beLike {
          case And(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2")) => ok
        }
      }
      
      "boolean or" >> {
        val tree = Or(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2"))
        bindRoot(tree, tree)
        
        rewriteForall(tree) must beLike {
          case Or(LineStream(), NumLit(LineStream(), "1"), NumLit(LineStream(), "2")) => ok
        }
      }
      
      "complementation" >> {
        val tree = Comp(LineStream(), NumLit(LineStream(), "1"))
        bindRoot(tree, tree)
        
        rewriteForall(tree) must beLike {
          case Comp(LineStream(), NumLit(LineStream(), "1")) => ok
        }
      }
      
      "negation" >> {
        val tree = Neg(LineStream(), NumLit(LineStream(), "1"))
        bindRoot(tree, tree)
        
        rewriteForall(tree) must beLike {
          case Neg(LineStream(), NumLit(LineStream(), "1")) => ok
        }
      }
    }
    
    "rewrite forall with the expr as a" in {
      "let" >> {
        val tree = Forall(LineStream(), "'a", 
          Let(LineStream(), Identifier(Vector(), "foo"), Vector(), 
            NumLit(LineStream(), "42"),
            Add(LineStream(),
              Dispatch(LineStream(), Identifier(Vector(), "foo"), Vector()),
              TicVar(LineStream(), "'a"))))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)

        results must beLike {
          case Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a"),
            Let(LineStream(), Identifier(Vector(), "foo"), Vector(),
              NumLit(LineStream(), "42"),
              Add(LineStream(), 
                Dispatch(LineStream(), Identifier(Vector(), "foo"), Vector()),
                TicVar(LineStream(), "'a"))),
            Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())) => ok
        }

        val results2 @ Let(_, _, _, Let(_, _, _, _, Add(_, _, t: TicVar)), _) = results

        t.binding mustEqual LetBinding(results2)
      }      
      
      "new" >> {
        val tree = Forall(LineStream(), "'a", New(LineStream(), TicVar(LineStream(), "'a")))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)

        results must beLike {
          case Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a"),
            New(LineStream(), TicVar(LineStream(), "'a")),
            Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())) => ok
        }

        val results2 @ Let(_, _, _, New(_, t: TicVar), _) = results

        t.binding mustEqual LetBinding(results2)
      }      

      "import" >> {
        val tree = Forall(LineStream(), "'a", Import(LineStream(), SpecificImport(Vector("std")), TicVar(LineStream(), "'a")))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)
        
        results must beLike {
          case Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a"),
            Import(LineStream(), SpecificImport(Vector("std")), TicVar(LineStream(), "'a")),
            Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())) => ok
        }

        val results2 @ Let(_, _, _, Import(_, _, t: TicVar), _) = results

        t.binding mustEqual LetBinding(results2)
      }
      
      "relate" >> {
        val tree = Forall(LineStream(), "'a", Relate(LineStream(), TicVar(LineStream(), "'a"), NumLit(LineStream(), "2"), NumLit(LineStream(), "3")))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)
        
        results must beLike {
          case Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a"),
            Relate(LineStream(), TicVar(LineStream(), "'a"), NumLit(LineStream(), "2"), NumLit(LineStream(), "3")),
            Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())) => ok
        }

        val results2 @ Let(_, _, _, Relate(_, t: TicVar, _, _), _) = results

        t.binding mustEqual LetBinding(results2)
      }
      
      "object def" >> {
        val tree = Forall(LineStream(), "'a", ObjectDef(LineStream(), Vector(("a", TicVar(LineStream(), "'a")), ("b", NumLit(LineStream(), "2")))))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)
        
        results must beLike {
          case Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a"),
            ObjectDef(LineStream(), Vector(("a", TicVar(LineStream(), "'a")), ("b", NumLit(LineStream(), "2")))),
            Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())) => ok
        }

        val results2 @ Let(_, _, _, ObjectDef(_, Vector((_, t: TicVar), (_))), _) = results

        t.binding mustEqual LetBinding(results2)
      }
      
      "array def" >> {
        val tree = Forall(LineStream(), "'a", ArrayDef(LineStream(), Vector(NullLit(LineStream()), TicVar(LineStream(), "'a"))))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)
                
        results must beLike {
          case Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a"),
            ArrayDef(LineStream(), Vector(NullLit(LineStream()), TicVar(LineStream(), "'a"))),
            Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())) => ok
        }

        val results2 @ Let(_, _, _, ArrayDef(_, Vector(_, t: TicVar)), _) = results

        t.binding mustEqual LetBinding(results2)
      }
      
      "descent" >> {
        val tree = Forall(LineStream(), "'a", Descent(LineStream(), TicVar(LineStream(), "'a"), "b"))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)

        results must beLike {
          case Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a"),
            Descent(LineStream(), TicVar(LineStream(), "'a"), "b"),
            Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())) => ok
        }

        val results2 @ Let(_, _, _, Descent(_, t: TicVar, _), _) = results

        t.binding mustEqual LetBinding(results2)
      }
      
      "metadescent" >> {
        val tree = Forall(LineStream(), "'a", MetaDescent(LineStream(), TicVar(LineStream(), "'a"), "b"))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)

        results must beLike {
          case Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a"),
            MetaDescent(LineStream(), TicVar(LineStream(), "'a"), "b"),
            Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())) => ok
        }

        val results2 @ Let(_, _, _, MetaDescent(_, t: TicVar, _), _) = results

        t.binding mustEqual LetBinding(results2)
      }
      
      "deref" >> {
        val tree = Forall(LineStream(), "'a", Deref(LineStream(), TicVar(LineStream(), "'a"), StrLit(LineStream(), "b")))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)

        results must beLike {
          case Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a"),
            Deref(LineStream(), TicVar(LineStream(), "'a"), StrLit(LineStream(), "b")),
            Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())) => ok
        }

        val results2 @ Let(_, _, _, Deref(_, t: TicVar, _), _) = results

        t.binding mustEqual LetBinding(results2)
      }
      
      "dispatch" >> {
        val tree = Forall(LineStream(), "'a", Dispatch(LineStream(), Identifier(Vector(), "foo"), Vector(TicVar(LineStream(), "'a"), NumLit(LineStream(), "2"))))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)

        results must beLike {
          case Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a"),
            Dispatch(LineStream(), Identifier(Vector(), "foo"), Vector(TicVar(LineStream(), "'a"), NumLit(LineStream(), "2"))),
            Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())) => ok
        }

        val results2 @ Let(_, _, _, Dispatch(_, _, Vector(t: TicVar, _)), _) = results

        t.binding mustEqual LetBinding(results2)
      }

      "where" >> {
        val tree = Forall(LineStream(), "'a", Where(LineStream(), TicVar(LineStream(), "'a"), BoolLit(LineStream(), true)))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)

        results must beLike {
          case Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a"),
            Where(LineStream(), TicVar(LineStream(), "'a"), BoolLit(LineStream(), true)),
            Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())) => ok
        }

        val results2 @ Let(_, _, _, Where(_, t: TicVar, _), _) = results

        t.binding mustEqual LetBinding(results2)
      }
      
      "with" >> {
        val tree = Forall(LineStream(), "'a", With(LineStream(), TicVar(LineStream(), "'a"), BoolLit(LineStream(), true)))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)

        results must beLike {
          case Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a"),
            With(LineStream(), TicVar(LineStream(), "'a"), BoolLit(LineStream(), true)),
            Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())) => ok
        }

        val results2 @ Let(_, _, _, With(_, t: TicVar, _), _) = results

        t.binding mustEqual LetBinding(results2)
      }
      
      "union" >> {
        val tree = Forall(LineStream(), "'a", Union(LineStream(), TicVar(LineStream(), "'a"), BoolLit(LineStream(), true)))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)

        results must beLike {
          case Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a"),
            Union(LineStream(), TicVar(LineStream(), "'a"), BoolLit(LineStream(), true)),
            Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())) => ok
        }

        val results2 @ Let(_, _, _, Union(_, t: TicVar, _), _) = results

        t.binding mustEqual LetBinding(results2)
      }      

      "intersect" >> {
        val tree = Forall(LineStream(), "'a", Forall(LineStream(), "'b", Intersect(LineStream(), TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b"))))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)

        results must beLike {
          case Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a", "'b"), 
            Intersect(LineStream(), TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b")), 
            Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())) => ok
        }

        val results2 @ Let(_, _, _, Intersect(_, t1: TicVar, t2: TicVar), _) = results

        t1.binding mustEqual LetBinding(results2)
        t2.binding mustEqual LetBinding(results2)
      }

      "difference" >> {
        val tree = Forall(LineStream(), "'a", Forall(LineStream(), "'b", Difference(LineStream(), TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b"))))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)

        results must beLike {
          case Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a", "'b"), 
            Difference(LineStream(), TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b")), 
            Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())) => ok
        }

        val results2 @ Let(_, _, _, Difference(_, t1: TicVar, t2: TicVar), _) = results

        t1.binding mustEqual LetBinding(results2)
        t2.binding mustEqual LetBinding(results2)
      }

      "addition" >> {
        val tree = Forall(LineStream(), "'a", Forall(LineStream(), "'b", Add(LineStream(), TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b"))))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)

        results must beLike {
          case Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a", "'b"), 
            Add(LineStream(), TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b")), 
            Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())) => ok
        }

        val results2 @ Let(_, _, _, Add(_, t1: TicVar, t2: TicVar), _) = results

        t1.binding mustEqual LetBinding(results2)
        t2.binding mustEqual LetBinding(results2)
      }

      "subtraction" >> {
        val tree = Forall(LineStream(), "'a", Forall(LineStream(), "'b", Sub(LineStream(), TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b"))))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)

        results must beLike {
          case Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a", "'b"), 
            Sub(LineStream(), TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b")), 
            Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())) => ok
        }

        val results2 @ Let(_, _, _, Sub(_, t1: TicVar, t2: TicVar), _) = results

        t1.binding mustEqual LetBinding(results2)
        t2.binding mustEqual LetBinding(results2)
      }

      "multiplication" >> {
        val tree = Forall(LineStream(), "'a", Forall(LineStream(), "'b", Mul(LineStream(), TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b"))))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)

        results must beLike {
          case Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a", "'b"), 
            Mul(LineStream(), TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b")), 
            Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())) => ok
        }

        val results2 @ Let(_, _, _, Mul(_, t1: TicVar, t2: TicVar), _) = results

        t1.binding mustEqual LetBinding(results2)
        t2.binding mustEqual LetBinding(results2)
      }

      "division" >> {
        val tree = Forall(LineStream(), "'a", Forall(LineStream(), "'b", Div(LineStream(), TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b"))))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)

        results must beLike {
          case Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a", "'b"), 
            Div(LineStream(), TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b")), 
            Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())) => ok
        }

        val results2 @ Let(_, _, _, Div(_, t1: TicVar, t2: TicVar), _) = results

        t1.binding mustEqual LetBinding(results2)
        t2.binding mustEqual LetBinding(results2)
      }

      "less than" >> {
        val tree = Forall(LineStream(), "'a", Forall(LineStream(), "'b", Lt(LineStream(), TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b"))))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)

        results must beLike {
          case Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a", "'b"), 
            Lt(LineStream(), TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b")), 
            Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())) => ok
        }

        val results2 @ Let(_, _, _, Lt(_, t1: TicVar, t2: TicVar), _) = results

        t1.binding mustEqual LetBinding(results2)
        t2.binding mustEqual LetBinding(results2)
      }

      "less than equal" >> {
        val tree = Forall(LineStream(), "'a", Forall(LineStream(), "'b", LtEq(LineStream(), TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b"))))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)

        results must beLike {
          case Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a", "'b"), 
            LtEq(LineStream(), TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b")), 
            Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())) => ok
        }

        val results2 @ Let(_, _, _, LtEq(_, t1: TicVar, t2: TicVar), _) = results

        t1.binding mustEqual LetBinding(results2)
        t2.binding mustEqual LetBinding(results2)
      }

      "greater than" >> {
        val tree = Forall(LineStream(), "'a", Forall(LineStream(), "'b", Gt(LineStream(), TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b"))))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)

        results must beLike {
          case Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a", "'b"), 
            Gt(LineStream(), TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b")), 
            Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())) => ok
        }

        val results2 @ Let(_, _, _, Gt(_, t1: TicVar, t2: TicVar), _) = results

        t1.binding mustEqual LetBinding(results2)
        t2.binding mustEqual LetBinding(results2)
      }

      "greater than equal" >> {
        val tree = Forall(LineStream(), "'a", Forall(LineStream(), "'b", GtEq(LineStream(), TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b"))))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)

        results must beLike {
          case Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a", "'b"), 
            GtEq(LineStream(), TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b")), 
            Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())) => ok
        }

        val results2 @ Let(_, _, _, GtEq(_, t1: TicVar, t2: TicVar), _) = results

        t1.binding mustEqual LetBinding(results2)
        t2.binding mustEqual LetBinding(results2)
      }

      "equal" >> {
        val tree = Forall(LineStream(), "'a", Forall(LineStream(), "'b", Eq(LineStream(), TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b"))))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)

        results must beLike {
          case Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a", "'b"), 
            Eq(LineStream(), TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b")), 
            Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())) => ok
        }

        val results2 @ Let(_, _, _, Eq(_, t1: TicVar, t2: TicVar), _) = results

        t1.binding mustEqual LetBinding(results2)
        t2.binding mustEqual LetBinding(results2)
      }

      "not equal" >> {
        val tree = Forall(LineStream(), "'a", Forall(LineStream(), "'b", NotEq(LineStream(), TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b"))))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)

        results must beLike {
          case Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a", "'b"), 
            NotEq(LineStream(), TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b")), 
            Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())) => ok
        }

        val results2 @ Let(_, _, _, NotEq(_, t1: TicVar, t2: TicVar), _) = results

        t1.binding mustEqual LetBinding(results2)
        t2.binding mustEqual LetBinding(results2)
      }

      "boolean and" >> {
        val tree = Forall(LineStream(), "'a", Forall(LineStream(), "'b", And(LineStream(), TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b"))))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)

        results must beLike {
          case Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a", "'b"), 
            And(LineStream(), TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b")), 
            Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())) => ok
        }

        val results2 @ Let(_, _, _, And(_, t1: TicVar, t2: TicVar), _) = results

        t1.binding mustEqual LetBinding(results2)
        t2.binding mustEqual LetBinding(results2)
      }

      "boolean or" >> {
        val tree = Forall(LineStream(), "'a", Forall(LineStream(), "'b", Or(LineStream(), TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b"))))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)

        results must beLike {
          case Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a", "'b"), 
            Or(LineStream(), TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b")), 
            Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())) => ok
        }

        val results2 @ Let(_, _, _, Or(_, t1: TicVar, t2: TicVar), _) = results

        t1.binding mustEqual LetBinding(results2)
        t2.binding mustEqual LetBinding(results2)
      }

      "complementation" >> {
        val tree = Forall(LineStream(), "'a", Comp(LineStream(), TicVar(LineStream(), "'a")))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)

        results must beLike {
          case Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a"), 
            Comp(LineStream(), TicVar(LineStream(), "'a")), 
            Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())) => ok
        }

        val results2 @ Let(_, _, _, Comp(_, t: TicVar), _) = results

        t.binding mustEqual LetBinding(results2)
      }

      "negation" >> {
        val tree = Forall(LineStream(), "'a", Neg(LineStream(), TicVar(LineStream(), "'a")))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)

        results must beLike {
          case Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a"), 
            Neg(LineStream(), TicVar(LineStream(), "'a")), 
            Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())) => ok
        }

        val results2 @ Let(_, _, _, Neg(_, t: TicVar), _) = results

        t.binding mustEqual LetBinding(results2)
      }

      "parentheticalization" >> {
        val tree = Forall(LineStream(), "'a", Paren(LineStream(), TicVar(LineStream(), "'a")))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)

        results must beLike {
          case Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a"), 
            Paren(LineStream(), TicVar(LineStream(), "'a")), 
            Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())) => ok
        }

        val results2 @ Let(_, _, _, Paren(_, t: TicVar), _) = results

        t.binding mustEqual LetBinding(results2)
      }
    }

    "rewrite forall inside a" in {
      "let" >> {
        val tree = Let(LineStream(), Identifier(Vector(), "foo"), Vector(), 
          Forall(LineStream(), "'a", TicVar(LineStream(), "'a")),
          Dispatch(LineStream(), Identifier(Vector(), "foo"), Vector()))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)

        results must beLike {
          case Let(LineStream(), Identifier(Vector(), "foo"), Vector(),
            Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a"),
              TicVar(LineStream(), "'a"),
              Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())),
            Dispatch(LineStream(), Identifier(Vector(), "foo"), Vector())) => ok
        }

        val results2 @ Let(_, _, _, l @ Let(_, _, _, t: TicVar, _), _) = results

        t.binding mustEqual LetBinding(l)
      }      
      
      "new" >> {
        val tree = New(LineStream(), Forall(LineStream(), "'a", TicVar(LineStream(), "'a")))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)

        results must beLike {
          case New(LineStream(), 
            Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a"),
              TicVar(LineStream(), "'a"),
              Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector()))) => ok
        }

        val results2 @ New(_, l @ Let(_, _, _, t: TicVar, _)) = results

        t.binding mustEqual LetBinding(l)
      }      

      "import" >> {
        val tree = Import(LineStream(), SpecificImport(Vector("std")), Forall(LineStream(), "'a", TicVar(LineStream(), "'a")))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)
        
        results must beLike {
            case Import(LineStream(), SpecificImport(Vector("std")), 
              Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a"), 
                TicVar(LineStream(), "'a"),
                Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector()))) => ok
        }

        val results2 @ Import(_, _, l @ Let(_, _, _, t: TicVar, _)) = results

        t.binding mustEqual LetBinding(l)
      }
      
      "relate" >> {
        val tree = Relate(LineStream(), NumLit(LineStream(), "2"), NumLit(LineStream(), "3"), Forall(LineStream(), "'a", TicVar(LineStream(), "'a")))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)
        
        results must beLike {
          case Relate(LineStream(), NumLit(LineStream(), "2"), NumLit(LineStream(), "3") ,
            Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a"),
              TicVar(LineStream(), "'a"),
              Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector()))) => ok
        }

        val results2 @ Relate(_, _, _, l @ Let(_, _, _, t: TicVar, _)) = results

        t.binding mustEqual LetBinding(l)
      }
      
      "object def" >> {
        val tree = ObjectDef(LineStream(), Vector(("a", Forall(LineStream(), "'a", TicVar(LineStream(), "'a"))), ("b", NumLit(LineStream(), "2"))))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)
        
        results must beLike {
          case ObjectDef(LineStream(), Vector(("a", Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a"),
            TicVar(LineStream(), "'a"),
            Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector()))),
          ("b", NumLit(LineStream(), "2"))))=> ok
        }

        val results2 @ ObjectDef(_, Vector((_, l @ Let(_, _, _, t: TicVar, _)), (_))) = results

        t.binding mustEqual LetBinding(l)
      }      

      "array def" >> {
        val tree = ArrayDef(LineStream(), Vector(Forall(LineStream(), "'a", TicVar(LineStream(), "'a")), NumLit(LineStream(), "2")))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)
        
        results must beLike {
          case ArrayDef(LineStream(), Vector(Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a"),
            TicVar(LineStream(), "'a"),
            Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())),
          NumLit(LineStream(), "2")))=> ok
        }

        val results2 @ ArrayDef(_, Vector(l @ Let(_, _, _, t: TicVar, _), _)) = results

        t.binding mustEqual LetBinding(l)
      }
      
      
      "descent" >> {
        val tree = Descent(LineStream(), Forall(LineStream(), "'a", TicVar(LineStream(), "'a")), "b")
        bindRoot(tree, tree)

        val results = rewriteForall(tree)

        results must beLike {
          case Descent(LineStream(),
            Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a"),
              TicVar(LineStream(), "'a"),
              Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())),
            "b") => ok
        }

        val results2 @ Descent(_, l @ Let(_, _, _, t: TicVar, _), _) = results

        t.binding mustEqual LetBinding(l)
      }
      
      "deref" >> {
        val tree = Deref(LineStream(), Forall(LineStream(), "'a", TicVar(LineStream(), "'a")), StrLit(LineStream(), "b"))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)

        results must beLike {
          case Deref(LineStream(),
            Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a"),
              TicVar(LineStream(), "'a"),
              Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())),
            StrLit(LineStream(), "b")) => ok
        }

        val results2 @ Deref(_, l @ Let(_, _, _, t: TicVar, _), _) = results

        t.binding mustEqual LetBinding(l)
      }
      
      "dispatch" >> {
        val tree = Dispatch(LineStream(), Identifier(Vector(), "foo"), Vector(Forall(LineStream(), "'a", TicVar(LineStream(), "'a")), NumLit(LineStream(), "2")))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)

        results must beLike {
          case Dispatch(LineStream(), Identifier(Vector(), "foo"), Vector(
            Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a"),
              TicVar(LineStream(), "'a"),
              Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())),
            NumLit(LineStream(), "2"))) => ok
        }

        val results2 @ Dispatch(_, _, Vector(l @ Let(_, _, _, t: TicVar, _), _)) = results

        t.binding mustEqual LetBinding(l)
      }

      "where" >> {
        val tree = Where(LineStream(), Forall(LineStream(), "'a", TicVar(LineStream(), "'a")), BoolLit(LineStream(), true))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)

        results must beLike {
          case Where(LineStream(),
            Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a"),
              TicVar(LineStream(), "'a"),
              Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())),
            BoolLit(LineStream(), true)) => ok
        }

        val results2 @ Where(_, l @ Let(_, _, _, t: TicVar, _), _) = results

        t.binding mustEqual LetBinding(l)
      }

      "with" >> {
        val tree = With(LineStream(), Forall(LineStream(), "'a", TicVar(LineStream(), "'a")), BoolLit(LineStream(), true))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)

        results must beLike {
          case With(LineStream(),
            Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a"),
              TicVar(LineStream(), "'a"),
              Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())),
            BoolLit(LineStream(), true)) => ok
        }

        val results2 @ With(_, l @ Let(_, _, _, t: TicVar, _), _) = results

        t.binding mustEqual LetBinding(l)
      }

      "union" >> {
        val tree = Union(LineStream(), Forall(LineStream(), "'a", TicVar(LineStream(), "'a")), BoolLit(LineStream(), true))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)

        results must beLike {
          case Union(LineStream(),
            Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a"),
              TicVar(LineStream(), "'a"),
              Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())),
            BoolLit(LineStream(), true)) => ok
        }

        val results2 @ Union(_, l @ Let(_, _, _, t: TicVar, _), _) = results

        t.binding mustEqual LetBinding(l)
      }

      "intersect" >> {
        val tree = Intersect(LineStream(), Forall(LineStream(), "'a", TicVar(LineStream(), "'a")), BoolLit(LineStream(), true))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)

        results must beLike {
          case Intersect(LineStream(),
            Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a"),
              TicVar(LineStream(), "'a"),
              Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())),
            BoolLit(LineStream(), true)) => ok
        }

        val results2 @ Intersect(_, l @ Let(_, _, _, t: TicVar, _), _) = results

        t.binding mustEqual LetBinding(l)
      }

      "difference" >> {
        val tree = Difference(LineStream(), Forall(LineStream(), "'a", TicVar(LineStream(), "'a")), BoolLit(LineStream(), true))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)

        results must beLike {
          case Difference(LineStream(),
            Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a"),
              TicVar(LineStream(), "'a"),
              Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())),
            BoolLit(LineStream(), true)) => ok
        }

        val results2 @ Difference(_, l @ Let(_, _, _, t: TicVar, _), _) = results

        t.binding mustEqual LetBinding(l)
      }

      "addition" >> {
        val tree = Add(LineStream(), Forall(LineStream(), "'a", TicVar(LineStream(), "'a")), BoolLit(LineStream(), true))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)

        results must beLike {
          case Add(LineStream(),
            Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a"),
              TicVar(LineStream(), "'a"),
              Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())),
            BoolLit(LineStream(), true)) => ok
        }

        val results2 @ Add(_, l @ Let(_, _, _, t: TicVar, _), _) = results

        t.binding mustEqual LetBinding(l)
      }

      "subtraction" >> {
        val tree = Sub(LineStream(), Forall(LineStream(), "'a", TicVar(LineStream(), "'a")), BoolLit(LineStream(), true))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)

        results must beLike {
          case Sub(LineStream(),
            Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a"),
              TicVar(LineStream(), "'a"),
              Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())),
            BoolLit(LineStream(), true)) => ok
        }

        val results2 @ Sub(_, l @ Let(_, _, _, t: TicVar, _), _) = results

        t.binding mustEqual LetBinding(l)
      }

      "multiplication" >> {
        val tree = Mul(LineStream(), Forall(LineStream(), "'a", TicVar(LineStream(), "'a")), BoolLit(LineStream(), true))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)

        results must beLike {
          case Mul(LineStream(),
            Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a"),
              TicVar(LineStream(), "'a"),
              Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())),
            BoolLit(LineStream(), true)) => ok
        }

        val results2 @ Mul(_, l @ Let(_, _, _, t: TicVar, _), _) = results

        t.binding mustEqual LetBinding(l)
      }

      "division" >> {
        val tree = Div(LineStream(), Forall(LineStream(), "'a", TicVar(LineStream(), "'a")), BoolLit(LineStream(), true))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)

        results must beLike {
          case Div(LineStream(),
            Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a"),
              TicVar(LineStream(), "'a"),
              Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())),
            BoolLit(LineStream(), true)) => ok
        }

        val results2 @ Div(_, l @ Let(_, _, _, t: TicVar, _), _) = results

        t.binding mustEqual LetBinding(l)
      }

      "less than" >> {
        val tree = Lt(LineStream(), Forall(LineStream(), "'a", TicVar(LineStream(), "'a")), BoolLit(LineStream(), true))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)

        results must beLike {
          case Lt(LineStream(),
            Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a"),
              TicVar(LineStream(), "'a"),
              Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())),
            BoolLit(LineStream(), true)) => ok
        }

        val results2 @ Lt(_, l @ Let(_, _, _, t: TicVar, _), _) = results

        t.binding mustEqual LetBinding(l)
      }

      "less than equal" >> {
        val tree = LtEq(LineStream(), Forall(LineStream(), "'a", TicVar(LineStream(), "'a")), BoolLit(LineStream(), true))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)

        results must beLike {
          case LtEq(LineStream(),
            Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a"),
              TicVar(LineStream(), "'a"),
              Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())),
            BoolLit(LineStream(), true)) => ok
        }

        val results2 @ LtEq(_, l @ Let(_, _, _, t: TicVar, _), _) = results

        t.binding mustEqual LetBinding(l)
      }

      "greater than" >> {
        val tree = Gt(LineStream(), Forall(LineStream(), "'a", TicVar(LineStream(), "'a")), BoolLit(LineStream(), true))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)

        results must beLike {
          case Gt(LineStream(),
            Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a"),
              TicVar(LineStream(), "'a"),
              Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())),
            BoolLit(LineStream(), true)) => ok
        }

        val results2 @ Gt(_, l @ Let(_, _, _, t: TicVar, _), _) = results

        t.binding mustEqual LetBinding(l)
      }

      "greater than equal" >> {
        val tree = GtEq(LineStream(), Forall(LineStream(), "'a", TicVar(LineStream(), "'a")), BoolLit(LineStream(), true))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)

        results must beLike {
          case GtEq(LineStream(),
            Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a"),
              TicVar(LineStream(), "'a"),
              Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())),
            BoolLit(LineStream(), true)) => ok
        }

        val results2 @ GtEq(_, l @ Let(_, _, _, t: TicVar, _), _) = results

        t.binding mustEqual LetBinding(l)
      }

      "equal" >> {
        val tree = Eq(LineStream(), Forall(LineStream(), "'a", TicVar(LineStream(), "'a")), BoolLit(LineStream(), true))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)

        results must beLike {
          case Eq(LineStream(),
            Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a"),
              TicVar(LineStream(), "'a"),
              Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())),
            BoolLit(LineStream(), true)) => ok
        }

        val results2 @ Eq(_, l @ Let(_, _, _, t: TicVar, _), _) = results

        t.binding mustEqual LetBinding(l)
      }

      "not equal" >> {
        val tree = NotEq(LineStream(), Forall(LineStream(), "'a", TicVar(LineStream(), "'a")), BoolLit(LineStream(), true))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)

        results must beLike {
          case NotEq(LineStream(),
            Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a"),
              TicVar(LineStream(), "'a"),
              Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())),
            BoolLit(LineStream(), true)) => ok
        }

        val results2 @ NotEq(_, l @ Let(_, _, _, t: TicVar, _), _) = results

        t.binding mustEqual LetBinding(l)
      }

      "boolean and" >> {
        val tree = And(LineStream(), Forall(LineStream(), "'a", TicVar(LineStream(), "'a")), BoolLit(LineStream(), true))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)

        results must beLike {
          case And(LineStream(),
            Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a"),
              TicVar(LineStream(), "'a"),
              Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())),
            BoolLit(LineStream(), true)) => ok
        }

        val results2 @ And(_, l @ Let(_, _, _, t: TicVar, _), _) = results

        t.binding mustEqual LetBinding(l)
      }

      "boolean or" >> {
        val tree = Or(LineStream(), Forall(LineStream(), "'a", TicVar(LineStream(), "'a")), BoolLit(LineStream(), true))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)

        results must beLike {
          case Or(LineStream(),
            Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a"),
              TicVar(LineStream(), "'a"),
              Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector())),
            BoolLit(LineStream(), true)) => ok
        }

        val results2 @ Or(_, l @ Let(_, _, _, t: TicVar, _), _) = results

        t.binding mustEqual LetBinding(l)
      }

      "complementation" >> {
        val tree = Comp(LineStream(), Forall(LineStream(), "'a", TicVar(LineStream(), "'a")))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)

        results must beLike {
          case Comp(LineStream(),
            Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a"),
              TicVar(LineStream(), "'a"),
              Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector()))) => ok
        }

        val results2 @ Comp(_, l @ Let(_, _, _, t: TicVar, _)) = results

        t.binding mustEqual LetBinding(l)
      }

      "negation" >> {
        val tree = Neg(LineStream(), 
          Forall(LineStream(), "'a", 
            Forall(LineStream(), "'b", Add(LineStream(), TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b")))))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)

        results must beLike {
          case Neg(LineStream(),
            Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a", "'b"),
              Add(LineStream(), TicVar(LineStream(), "'a"), TicVar(LineStream(), "'b")),
              Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector()))) => ok
        }

        val results2 @ Neg(_, l @ Let(_, _, _, Add(_, t1: TicVar, t2: TicVar), _)) = results

        t1.binding mustEqual LetBinding(l)
        t2.binding mustEqual LetBinding(l)
      }

      "parentheticalization" >> {
        val tree = Paren(LineStream(), Forall(LineStream(), "'a", TicVar(LineStream(), "'a")))
        bindRoot(tree, tree)

        val results = rewriteForall(tree)

        results must beLike {
          case Paren(LineStream(),
            Let(LineStream(), Identifier(Vector(), "$forall"), Vector("'a"),
              TicVar(LineStream(), "'a"),
              Dispatch(LineStream(), Identifier(Vector(), "$forall"), Vector()))) => ok
        }

        val results2 @ Paren(_, l @ Let(_, _, _, t: TicVar, _)) = results

        t.binding mustEqual LetBinding(l)
      }
      
    }
  }
}

