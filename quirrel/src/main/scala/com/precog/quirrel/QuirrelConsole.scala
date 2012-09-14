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
