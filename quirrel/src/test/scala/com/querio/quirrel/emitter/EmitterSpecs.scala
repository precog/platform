package com.querio.quirrel
package emitter

import com.querio.bytecode.Instructions

import org.scalacheck.Prop
import org.specs2.ScalaCheck
import org.specs2.mutable._

import typer._

import scalaz.Success
import scalaz.Scalaz._

// import scalaz.std.function._
// import scalaz.syntax.arrow._

object EmitterSpecs extends Specification
    with ScalaCheck
    with StubPhases
    with Compiler
    with ProvenanceChecker
    with CriticalConditionFinder 
    with Emitter {

  import instructions._

  val compileEmit = ((_: String).stripMargin) >>> (compile(_: String)) >>> (emit _)

  "emitter" should {
    "emit load of literal dataset" in {
      compileEmit("""dataset("foo")""") mustEqual 
        Success(Vector(
          PushString("foo"),
          LoadLocal(Het)
        ))
    }
  }
}