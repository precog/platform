package com.precog
package quirrel

import org.specs2.mutable._

import parser._
import typer._
import emitter._

object PhasesSpecs extends Specification
    with StubPhases
    with CompilerUtils
    with Compiler
    with ProvenanceChecker
    with GroupSolver
    with RawErrors
    with RandomLibrarySpec {

  "full compiler" should {
    "self-populate AST errors atom" in {
      val tree = compileSingle("fubar")
      tree.errors must not(beEmpty)
    }
    
    "propagate binder errors through tree shaking" in {
      val input = """
        | f( x ) :=
        |   new x ~ new x
        |   (new x) + (new x)
        | f(5)
        | """.stripMargin
        
      val forest = compile(input)
      val validForest = forest filter { tree =>
        tree.errors forall isWarning
      }
      
      validForest must beEmpty
    }
  }
}
