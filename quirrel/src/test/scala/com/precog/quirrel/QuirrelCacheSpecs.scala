package com.precog
package quirrel

import org.specs2.mutable._

import parser._
import typer._
import emitter._

object QuirrelCacheSpecs extends Specification
    with Parser
    with Compiler
    with TreeShaker
    with GroupSolver
    with LineErrors 
    with RandomLibrarySpec {

  import ast._

  private def findNode[A](root: Expr)(pf: PartialFunction[Expr, A]): Option[A] = {
    def loop(expr: Expr): Option[A] = {
      if (pf.isDefinedAt(expr)) {
        Some(pf(expr))
      } else {
        expr.children.iterator map loop collectFirst {
          case Some(a) => a
        }
      }
    }

    loop(root)
  }

  "quirrel cache" should {
    "not modify locs for identical queries" in {
      val input = """a := 1
                    |b := "asdf"
                    |c := true
                    |d := //abc
                    |a + b + c + d
                    |""".stripMargin
      val dummy = compile(input)

      val result = compile(input)
      result must haveSize(1)
      val root = result.head

      val a = findNode(root) { case NumLit(loc, _) => (loc.lineNum, loc.colNum) }
      val b = findNode(root) { case StrLit(loc, "asdf") => (loc.lineNum, loc.colNum) }
      val c = findNode(root) { case BoolLit(loc, _) => (loc.lineNum, loc.colNum) }
      val d = findNode(root) { case StrLit(loc, "/abc") => (loc.lineNum, loc.colNum) }

      a must_== Some((1, 6))
      b must_== Some((2, 6))
      c must_== Some((3, 6))
      d must_== Some((4, 6))
    }

    "update locs" in {
      val input1 = """a := 1
                    |b := 999
                    |c := 1000
                    |d := 21345
                    |a + b + true + "abc" + 1234 + c + d
                    |""".stripMargin

      val input2 = """a := 1
                    |b := 999
                    |c := 1000
                    |d := 21345
                    |a + b + true + "abc" + 0 + c + d
                    |""".stripMargin

      val input3 = """a := 1
                    |b := 999
                    |c := 1000
                    |d := 21345
                    |a + b + false + "asdfzxcv" + 1000000 + c + d
                    |""".stripMargin

      val result1 = compile(input1)
      val result2 = compile(input2)
      val result3 = compile(input3)

      result2 must haveSize(1)
      result3 must haveSize(1)

      def varLoc(name: String)(e: Expr) = {
        findNode(e) { case Dispatch(loc, Identifier(_, `name`), _) =>
          (loc.lineNum, loc.colNum)
        }
      }

      val root2 = result2.head
      varLoc("a")(root2) must_== Some((5, 1))
      varLoc("b")(root2) must_== Some((5, 5))
      varLoc("c")(root2) must_== Some((5, 28))
      varLoc("d")(root2) must_== Some((5, 32))

      val root3 = result3.head
      varLoc("a")(root3) must_== Some((5, 1))
      varLoc("b")(root3) must_== Some((5, 5))
      varLoc("c")(root3) must_== Some((5, 40))
      varLoc("d")(root3) must_== Some((5, 44))
    }
  }
}
