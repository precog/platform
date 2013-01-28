package com.precog
package quirrel

import org.specs2.mutable._

import parser._
import typer._
import emitter._

object LineErrorsSpecs extends Specification
    with Parser
    with TreeShaker
    with GroupSolver
    with LineErrors 
    with RandomLibrarySpec {

  "line errors" should {
    "be correct" in {
      val input = """
        |
        | a := 1
        |
        |
        |
        | a := 1
        | 10""".stripMargin

      val tree = parse(input).head
      bindRoot(tree, tree)

      val result = shakeTree(tree)
      result.errors.map(e => e.loc.lineNum -> e.loc.colNum) mustEqual Set(3 -> 2, 7 -> 2)
    }
  }
}
