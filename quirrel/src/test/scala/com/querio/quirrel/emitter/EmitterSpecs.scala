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
      testEmit("!true")(
        PushTrue,
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

    "emit join of wrapped object for object with two fields having constant values" in {
      testEmit("{foo: 2, bar: true}")(
        PushString("foo"),
        PushNum("2"),
        Map2Cross(WrapObject),
        PushString("bar"),
        PushTrue,
        Map2Cross(WrapObject),
        Map2Cross(JoinObject)
      )
    }

    "emit matched join of wrapped object for object with two fields having same provenance" in {
      testEmit("clicks := dataset(//clicks) {foo: clicks, bar: clicks}")(
        PushString("foo"),
        PushString("/clicks"),
        LoadLocal(Het),
        Dup,
        Swap(2),
        Swap(1),
        Map2Cross(WrapObject),
        PushString("bar"),
        Swap(1),
        Swap(2),
        Map2Cross(WrapObject),
        Map2Match(JoinObject)
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

    "emit join of wrapped arrays for array with four elements having values from two static provenances" in {
      testEmit("foo := dataset(//foo) bar := dataset(//bar) [foo.a, bar.a, foo.b, bar.b]")(
        PushString("/foo"),
        LoadLocal(Het),
        Dup, 
        PushString("a"),
        Map2CrossLeft(DerefObject),
        Map1(WrapArray),
        Swap(1),
        PushString("b"),
        Map2CrossLeft(DerefObject),
        Map1(WrapArray),
        Map2Match(JoinArray),
        PushString("/bar"),
        LoadLocal(Het),
        Dup,
        Swap(2),
        Swap(1),
        PushString("a"),
        Map2CrossLeft(DerefObject),
        Map1(WrapArray),
        Swap(1),
        Swap(2),
        PushString("b"),
        Map2CrossLeft(DerefObject),
        Map1(WrapArray),
        Map2Match(JoinArray),
        Map2Cross(JoinArray),
        PushNum("1"),
        Map2CrossLeft(ArraySwap)
      )
    }

    "emit descent for object dataset" in {
      testEmit("clicks := dataset(//clicks) clicks.foo")(
        PushString("/clicks"),
        LoadLocal(Het),
        PushString("foo"),
        Map2CrossLeft(DerefObject)
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
        Dup,
        Map2Match(DerefArray)
      )
    }

    "emit filter match for where datasets from same provenance" in {
      testEmit("""foo := dataset("foo") foo where foo""")(
        PushString("foo"),
        LoadLocal(Het),
        Dup,
        FilterMatch(0, None)
      )
    }

    "emit filter match for datasets from same provenance when performing equality filter" in {
      testEmit("foo := dataset(//foo) foo where foo.id = 2")(
        PushString("/foo"),
        LoadLocal(Het),
        Dup,
        PushString("id"),
        Map2CrossLeft(DerefObject),
        PushNum("2"),
        Map2CrossLeft(Eq),
        FilterMatch(0, None)
      )
    }

    "use dup bytecode to duplicate the same dataset" in {
      testEmit("""clicks := dataset("foo") clicks + clicks""")(
        PushString("foo"),
        LoadLocal(Het),
        Dup,
        Map2Match(Add)
      )
    }

    "use dup bytecode non-locally" in {
      testEmit("""clicks := dataset("foo") two := 2 * clicks two + clicks""")(
        PushNum("2"),
        PushString("foo"),
        LoadLocal(Het),
        Dup,
        Swap(2),
        Swap(1),
        Map2CrossRight(Mul),
        Swap(1),
        Map2Match(Add)
      )
    }

    "emit count reduction" in {
      testEmit("count(1)")(
        PushNum("1"),
        Reduce(Count)
      )
    }

    "emit count reduction" in {
      testEmit("count(1)")(
        PushNum("1"),
        Reduce(Count)
      )
    }

    "emit count reduction" in {
      testEmit("mean(1)")(
        PushNum("1"),
        Reduce(Mean)
      )
    }

    "emit count reduction" in {
      testEmit("median(1)")(
        PushNum("1"),
        Reduce(Median)
      )
    }

    "emit count reduction" in {
      testEmit("mode(1)")(
        PushNum("1"),
        Reduce(Mode)
      )
    }

    "emit count reduction" in {
      testEmit("max(1)")(
        PushNum("1"),
        Reduce(Max)
      )
    }

    "emit count reduction" in {
      testEmit("min(1)")(
        PushNum("1"),
        Reduce(Min)
      )
    }

    "emit count reduction" in {
      testEmit("stdDev(1)")(
        PushNum("1"),
        Reduce(StdDev)
      )
    }

    "emit count reduction" in {
      testEmit("sum(1)")(
        PushNum("1"),
        Reduce(Sum)
      )
    }

    /*"emit body of characteristic function substituted for concrete parameters" in {
      testEmit("clicks := dataset(//clicks) clicksFor('userId) := clicks where clicks.userId = 'userId clicksFor(\"foo\")"(
        PushString("/clicks"),
        LoadLocal(Het),
        Dup,

      )
    }*/
  }
}