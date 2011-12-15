package com.querio.quirrel
package emitter

import org.scalacheck.Prop
import org.specs2.ScalaCheck
import org.specs2.mutable._

import typer._

object EmitterSpecs extends Specification
    with ScalaCheck
    with StubPhases
    with Compiler
    with ProvenanceChecker
    with CriticalConditionFinder 
    with Emitter {

  import ast._
}