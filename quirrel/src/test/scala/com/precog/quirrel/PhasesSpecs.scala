package com.precog
package quirrel

import org.specs2.mutable._

import bytecode.RandomLibrary
import parser._
import typer._
import emitter._

object PhasesSpecs extends Specification
    with StubPhases
    with Compiler
    with ProvenanceChecker
    with CriticalConditionSolver
    with RawErrors 
    with RandomLibrary {
  
  "full compiler" should {
    "self-populate AST errors atom" in {
      val tree = compile("fubar")
      tree.errors must not(beEmpty)
    }
  }
}
