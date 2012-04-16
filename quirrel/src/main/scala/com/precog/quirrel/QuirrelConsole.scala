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

  val compiler = new Parser
    with Binder
    with ProvenanceChecker
    with CriticalConditionSolver
    with GroupSolver
    with Compiler
    with Emitter
    with LineErrors
    with EmptyLibrary {}

  trait StubPhases extends Phases with RawErrors {
    protected def LoadId = Identifier(Vector(), "load")
    
    def bindNames(tree: Expr) = Set()
    def checkProvenance(tree: Expr) = Set()
    def findCriticalConditions(expr: Expr): Map[String, Set[ConditionTree]] = Map()
    def findGroups(expr: Expr): Map[String, Set[GroupTree]] = Map()
    def solveCriticalConditions(expr: Expr) = Set()
    def inferBuckets(expr: Expr) = Set()
  }

  val solver = new Solver with Parser with StubPhases {}

  def solve(str: String, id: Symbol): Option[solver.Expr] = {
    import solver.ast._
    val f = solver.solve(solver.parse(LineStream(str))) { case TicVar(_, id2) => id.toString == id2 }
    f(NumLit(LineStream(), "0"))
  }
}

// vim: set ts=4 sw=4 et:
