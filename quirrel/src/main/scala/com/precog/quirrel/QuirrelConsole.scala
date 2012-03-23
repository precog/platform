package com.precog
package quirrel

import com.codecommit.gll.LineStream

import com.precog.quirrel._
import bytecode._
import emitter._
import parser._
import typer._

object QuirrelConsole {
  trait EmptyLibrary extends Library {
    type BIF1 = BuiltInFunc1
    type BIF2 = BuiltInFunc2
    def lib1 = Set()
    def lib2 = Set()
  }

  val compiler = new Parser with Binder with ProvenanceChecker with CriticalConditionSolver with Compiler with Emitter with LineErrors with EmptyLibrary {}


  trait StubPhases extends Phases with RawErrors {
    def bindNames(tree: Expr) = Set()
    def checkProvenance(tree: Expr) = Set()
    def findCriticalConditions(expr: Expr): Map[String, Set[ConditionTree]] = Map()
    def solveCriticalConditions(expr: Expr) = Set()
  }

  val solver = new Solver with Parser with StubPhases {}

  def solve(str: String, id: Symbol): Option[solver.Expr] = {
    import solver.ast._
    val f = solver.solve(solver.parse(LineStream(str))) { case TicVar(_, id2) => id.toString == id2 }
    f(NumLit(LineStream(), "0"))
  }
}

// vim: set ts=4 sw=4 et:
