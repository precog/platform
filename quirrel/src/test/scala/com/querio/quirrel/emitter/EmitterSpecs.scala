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

    "emit wrap object for object with single field having constant value" in {
      testEmit("{foo: 1}")(
        PushString("foo"),
        PushNum("1"),
        Map2Cross(WrapObject)
      )
    }

    "emit wrap array for array with single element having constant value" in {
      testEmit("[\"foo\"]")(
        PushString("foo"),
        Map1(WrapArray)
      )
    }

    "emit join of wrapped arrays for array with two elements having constant values" in {
      testEmit("[\"foo\", true]")(
        PushString("foo"),
        Map1(WrapArray),
        PushTrue,
        Map1(WrapArray),
        Map2Cross(JoinArray)
      )
    }

    "emit descent for object dataset" in {
      testEmit("clicks := dataset(//clicks) clicks.foo")(
        PushString("/clicks"),
        LoadLocal(Het),
        PushString("foo"),
        Map2Cross(DerefObject)
      )
    }

    "emit descent for array dataset" in {
      testEmit("clicks := dataset(//clicks) clicks[1]")(
        PushString("/clicks"),
        LoadLocal(Het),
        PushNum("1"),
        Map2CrossLeft(DerefArray)
      )
    }

    "emit load of literal dataset" in {
      testEmit("""dataset("foo")""")(
        PushString("foo"),
        LoadLocal(Het)
      )
    }

    "emit filter cross for where datasets from value provenance" in {
      testEmit("""1 where true""")(
        PushNum("1"),
        PushTrue,
        FilterCross(0, None)
      )
    }

    "emit descent for array dataset with non-constant indices" in {
      testEmit("clicks := dataset(//clicks) clicks[clicks]")(
        PushString("/clicks"),
        LoadLocal(Het),
        PushString("/clicks"), // TODO: DELETE (should be DUP)
        LoadLocal(Het),        // TODO: DELETE
        Map2Match(DerefArray)
      )
    }

    "emit filter match for where datasets from same provenance" in {
      testEmit("""foo := dataset("foo") foo where foo""")(
        PushString("foo"),
        LoadLocal(Het),
        PushString("foo"), // TODO: DELETE (should be DUP)
        LoadLocal(Het),    // TODO: DELETE
        FilterMatch(0, None)
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