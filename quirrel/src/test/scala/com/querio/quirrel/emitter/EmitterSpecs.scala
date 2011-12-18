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

  val compileEmit = ((_: String).stripMargin) >>> (compile _) >>> (emit _)

  def testEmit(v: String)(i: Instruction*) = compileEmit(v) mustEqual Vector(i: _*)

  "emitter" should {
    "emit literal string" in {
      testEmit("\"foo\"")(
        PushString("foo")
      )
    }

    "emit literal boolean" in {
      testEmit("true")(
        PushTrue
      )

      testEmit("false")(
        PushFalse
      )
    }

    "emit literal number" in {
      testEmit("23.23123")(
        PushNum("23.23123")
      )
    }

    "emit cross-addition of two added datasets with value provenance" in {
      testEmit("5 + 2")(
        PushNum("5"),
        PushNum("2"),
        Map2Cross(Add)
      )
    }

    "emit cross < for datasets with value provenance" in {
      testEmit("5 < 2")(
        PushNum("5"),
        PushNum("2"),
        Map2Cross(Lt)
      )
    }

    "emit cross <= for datasets with value provenance" in {
      testEmit("5 <= 2")(
        PushNum("5"),
        PushNum("2"),
        Map2Cross(LtEq)
      )
    }

    "emit cross > for datasets with value provenance" in {
      testEmit("5 > 2")(
        PushNum("5"),
        PushNum("2"),
        Map2Cross(Gt)
      )
    }

    "emit cross >= for datasets with value provenance" in {
      testEmit("5 >= 2")(
        PushNum("5"),
        PushNum("2"),
        Map2Cross(GtEq)
      )
    }

    "emit cross != for datasets with value provenance" in {
      testEmit("5 != 2")(
        PushNum("5"),
        PushNum("2"),
        Map2Cross(NotEq)
      )
    }

    "emit cross = for datasets with value provenance" in {
      testEmit("5 = 2")(
        PushNum("5"),
        PushNum("2"),
        Map2Cross(Eq)
      )
    }

    "emit cross ! for dataset with value provenance" in {
      testEmit("!5")(
        PushNum("5"),
        Map1(Comp)
      )
    }

    "emit cross-subtraction of two subtracted datasets with value provenance" in {
      testEmit("5 - 2")(
        PushNum("5"),
        PushNum("2"),
        Map2Cross(Sub)
      )
    }

    "emit cross-division of two divided datasets with value provenance" in {
      testEmit("5 / 2")(
        PushNum("5"),
        PushNum("2"),
        Map2Cross(Div)
      )
    }

    "emit left-cross for division of dataset in static provenance with dataset in value provenance" in {
      testEmit("dataset(\"foo\") * 2")(
        PushString("foo"),
        LoadLocal(Het),
        PushNum("2"),
        Map2CrossLeft(Mul)
      )
    }

    "emit right-cross for division of dataset in static provenance with dataset in value provenance" in {
      testEmit("2 * dataset(\"foo\")")(
        PushNum("2"),
        PushString("foo"),
        LoadLocal(Het),
        Map2CrossRight(Mul)
      )
    }

    "emit negation of literal numeric dataset with value provenance" in {
      testEmit("~5")(
        PushNum("5"),
        Map1(Neg)
      )
    }

    "emit negation of sum of two literal numeric datasets with value provenance" in {
      testEmit("~(5 + 2)")(
        PushNum("5"),
        PushNum("2"),
        Map2Cross(Add),
        Map1(Neg)
      )
    }

    "emit load of literal dataset" in {
      testEmit("""dataset("foo")""")(
        PushString("foo"),
        LoadLocal(Het)
      )
    }

    "use dup bytecode to duplicate the same dataset" in {
      testEmit("""clicks := dataset("foo") clicks + clicks""")(
        PushString("foo"),
        LoadLocal(Het),
        PushString("foo"), // TODO: DELETE (should be DUP)
        LoadLocal(Het),    // TODO: DELETE
        Map2Match(Add)
      )
    }
  }
}