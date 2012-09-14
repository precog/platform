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
    type Morphism1 = Morphism1Like
    type Morphism2 = Morphism2Like
    type Op1 = Op1Like with Morphism1
    type Op2 = Op2Like
    type Reduction = ReductionLike with Morphism1

    def libMorphism1 = Set()
    def libMorphism2 = Set()
    def lib1 = Set()
    def lib2 = Set()
    def libReduction = Set()
  }

  val compiler = new Parser
    with StubPhases
    with ProvenanceChecker
    with Binder
    with Compiler
    with Emitter
    with LineErrors
    with EmptyLibrary {}

  trait StubPhases extends Phases {
    protected def LoadId = Identifier(Vector(), "load")
    protected def DistinctId = Identifier(Vector(), "distinct")
    
    def bindNames(tree: Expr) = Set()
    def checkProvenance(tree: Expr) = Set()
    def findCriticalConditions(expr: Expr): Map[String, Set[ConditionTree]] = Map()
    def findGroups(expr: Expr): Set[GroupTree] = Set()
    def inferBuckets(expr: Expr) = Set()
  }
}

// vim: set ts=4 sw=4 et:
