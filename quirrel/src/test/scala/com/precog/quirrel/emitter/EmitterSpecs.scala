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
package emitter

import org.specs2.mutable._

import java.io.File
import scala.io.Source
import com.codecommit.gll.LineStream

import typer._

import scalaz.Success
import scalaz.Scalaz._

// import scalaz.std.function._
// import scalaz.syntax.arrow._

object EmitterSpecs extends Specification
    with StubPhases
    with CompilerUtils
    with Compiler
    with Emitter
    with RawErrors 
    with StaticLibrarySpec {

  import instructions._
  import library._

  def compileEmit(input: String) = {
    val tree = compileSingle(input.stripMargin)
    tree.errors filterNot isWarning must beEmpty
    emit(tree)
  }

  def testEmit(v: String)(head: Vector[Instruction],
          streams: Vector[Instruction]*) =
    compileEmit(v).filter { case _ : Line => false; case _ => true } must beOneOf((head +: streams): _*)

  def testEmitLine(v: String)(head: Vector[Instruction],
          streams: Vector[Instruction]*) =
    compileEmit(v) must beOneOf((head +: streams): _*)

  "emitter" should {
    "emit literal string" in {
      testEmit("\"foo\"")(
        Vector(
          PushString("foo")))
    }
    
    "emit literal boolean" in {
      testEmit("true")(
        Vector(
          PushTrue))

      testEmit("false")(
        Vector(
          PushFalse))
    }

    "emit literal number" in {
      testEmit("23.23123")(
        Vector(
          PushNum("23.23123")))
    }

    "emit literal null" in {
      testEmit("null")(
        Vector(
          PushNull))
    }

    "emit literal undefined" in {
      testEmit("undefined")(
        Vector(
          PushUndefined))
    }
    
    "emit child of import" in {
      testEmit("import std 42")(
        Vector(PushNum("42")))
    }
    
    "emit assert" in {
      testEmit("assert true 42")(
        Vector(
          PushTrue,
          PushNum("42"),
          Assert))
    }
    
    "emit cond" in {
      testEmit("if true then 1 else 2")(
        Vector(
          PushNum("1"),
          PushTrue,
          FilterCross,
          PushNum("2"),
          PushTrue,
          Map1(Comp),
          FilterCross,
          IUnion))
    }

    "emit filter of two where'd loads with value provenance" >> {
      "which are numerics" >> {
        testEmit("5 where 2")(
          Vector(
            PushNum("5"),
            PushNum("2"),
            FilterCross))
      }

      "which are null and string" >> {
        testEmit("""null where "foo" """)(
          Vector(
            PushNull,
            PushString("foo"),
            FilterCross))
      }

    }

    "emit cross-join of two with'ed loads with value provenance" in {
      testEmit("5 with 2")(
        Vector(
          PushNum("5"),
          PushNum("2"),
          Map2Cross(JoinObject)))
    }

    "emit instruction for two unioned loads" in {
      testEmit("""load("foo") union load("bar")""")(
        Vector(
          PushString("foo"),
          AbsoluteLoad,
          PushString("bar"),
          AbsoluteLoad,
          IUnion))
    }

    "emit instruction for two unioned relative loads" in {
      testEmit("""relativeLoad("foo") union relativeLoad("bar")""")(
        Vector(
          PushString("foo"),
          RelativeLoad,
          PushString("bar"),
          RelativeLoad,
          IUnion))
    }
    
    "emit instruction for two intersected loads" in {
      testEmit("""load("foo") intersect load("foo")""")(
        Vector(
          PushString("foo"),
          AbsoluteLoad,
          PushString("foo"),
          AbsoluteLoad,
          IIntersect))
    }

    "emit instruction for two intersected relative loads" in {
      testEmit("""relativeLoad("foo") intersect relativeLoad("foo")""")(
        Vector(
          PushString("foo"),
          RelativeLoad,
          PushString("foo"),
          RelativeLoad,
          IIntersect))
    }    

    "emit instruction for two set differenced loads" in {
      testEmit("""load("foo") difference load("foo")""")(
        Vector(
          PushString("foo"),
          AbsoluteLoad,
          PushString("foo"),
          AbsoluteLoad,
          SetDifference))
    }
    
    "emit instruction for two set differenced relative loads" in {
      testEmit("""relativeLoad("foo") difference relativeLoad("foo")""")(
        Vector(
          PushString("foo"),
          RelativeLoad,
          PushString("foo"),
          RelativeLoad,
          SetDifference))
    }

    "emit cross-addition of two added loads with value provenance" in {
      testEmit("5 + 2")(
        Vector(
          PushNum("5"),
          PushNum("2"),
          Map2Cross(Add)))
    }

    "emit cross < for loads with value provenance" in {
      testEmit("5 < 2")(
        Vector(
          PushNum("5"),
          PushNum("2"),
          Map2Cross(Lt)))
    }

    "emit cross <= for loads with value provenance" in {
      testEmit("5 <= 2")(
        Vector(
          PushNum("5"),
          PushNum("2"),
          Map2Cross(LtEq)))
    }

    "emit cross > for loads with value provenance" in {
      testEmit("5 > 2")(
        Vector(
          PushNum("5"),
          PushNum("2"),
          Map2Cross(Gt)))
    }

    "emit cross >= for loads with value provenance" in {
      testEmit("5 >= 2")(
        Vector(
          PushNum("5"),
          PushNum("2"),
          Map2Cross(GtEq)))
    }

    "emit cross != for loads with value provenance" in {
      testEmit("5 != 2")(
        Vector(
          PushNum("5"),
          PushNum("2"),
          Map2Cross(NotEq)))
    }

    "emit cross = for loads with value provenance" in {
      testEmit("5 = 2")(
        Vector(
          PushNum("5"),
          PushNum("2"),
          Map2Cross(Eq)))
    }

    "emit cross ! for load with value provenance" in {
      testEmit("!true")(
        Vector(
          PushTrue,
          Map1(Comp)))
    }

    "emit cross-subtraction of two subtracted loads with value provenance" in {
      testEmit("5 - 2")(
        Vector(
          PushNum("5"),
          PushNum("2"),
          Map2Cross(Sub)))
    }

    "emit cross-division of two divided loads with value provenance" in {
      testEmit("5 / 2")(
        Vector(
          PushNum("5"),
          PushNum("2"),
          Map2Cross(Div)))
    }

    "emit cross for division of load in static provenance with load in value provenance" in {
      testEmit("load(\"foo\") * 2")(
        Vector(
          PushString("foo"),
          AbsoluteLoad,
          PushNum("2"),
          Map2Cross(Mul)))
    }
    
    "emit cross for division of relative load in static provenance with load in value provenance" in {
      testEmit("relativeLoad(\"foo\") * 2")(
        Vector(
          PushString("foo"),
          RelativeLoad,
          PushNum("2"),
          Map2Cross(Mul)))
    }

    "emit line information for cross for division of load in static provenance with load in value provenance" in {
      testEmitLine("load(\"foo\") * 2")(
        Vector(
          Line(1, 6, "load(\"foo\") * 2"),
          PushString("foo"),
          Line(1, 1, "load(\"foo\") * 2"),
          AbsoluteLoad,
          Line(1, 15, "load(\"foo\") * 2"),
          PushNum("2"),
          Line(1, 1, "load(\"foo\") * 2"),
          Map2Cross(Mul)))
    }

    "emit cross for division of load in static provenance with load in value provenance" in {
      testEmit("2 * load(\"foo\")")(
        Vector(
          PushNum("2"),
          PushString("foo"),
          AbsoluteLoad,
          Map2Cross(Mul)))
    }
    
    "emit cross for division of relative load in static provenance with load in value provenance" in {
      testEmit("2 * relativeLoad(\"foo\")")(
        Vector(
          PushNum("2"),
          PushString("foo"),
          RelativeLoad,
          Map2Cross(Mul)))
    }

    "emit negation of literal numeric load with value provenance" in {
      testEmit("neg 5")(
        Vector(
          PushNum("5"),
          Map1(Neg))
      )
    }

    "emit negation of sum of two literal numeric loads with value provenance" in {
      testEmit("neg (5 + 2)")(
        Vector(
          PushNum("5"),
          PushNum("2"),
          Map2Cross(Add),
          Map1(Neg)))
    }
    
    "emit and mark new expression" in {
      testEmit("new 5")(
        Vector(
          PushNum("5"),
          Map1(New)))
    }
    
    "emit empty object" in {
      testEmit("{}")(Vector(PushObject))
    }

    "emit wrap object for object with single field having constant numeric value" in {
      testEmit("{foo: 1}")(
        Vector(
          PushString("foo"),
          PushNum("1"),
          Map2Cross(WrapObject)))
    }

    "emit wrap object for object with single field having null value" in {
      testEmit("{foo: null}")(
        Vector(
          PushString("foo"),
          PushNull,
          Map2Cross(WrapObject)))
    }

    "emit join of wrapped object for object with two fields having constant values" in {
      testEmit("{foo: 2, bar: true}")(
        Vector(
          PushString("foo"),
          PushNum("2"),
          Map2Cross(WrapObject),
          PushString("bar"),
          PushTrue,
          Map2Cross(WrapObject),
          Map2Cross(JoinObject)))
    }

    "emit nested dispatches of the same function" in {
      val input = """
        | not(x) := !x
        | not(not(true))
        """.stripMargin

      testEmit(input)(
        Vector(
          PushTrue,
          Map1(Comp),
          Map1(Comp)
        ))
    }

    "emit a match after a cross in a join-object" in {
      val input = """
        | medals := //summer_games/london_medals
        | five := new 5 
        |
        | five ~ medals
        |   fivePlus := five + medals.Weight
        |   { five: five, increasedWeight: fivePlus }
        | """.stripMargin

      testEmit(input)(
        Vector(
          PushString("five"),
          PushNum("5"),
          Map1(New),
          Dup,
          Swap(2),
          Swap(1),
          Map2Cross(WrapObject),
          PushString("increasedWeight"),
          Swap(1),
          Swap(2),
          PushString("/summer_games/london_medals"),
          Morph1(BuiltInMorphism1(expandGlob)),
          AbsoluteLoad,
          PushString("Weight"),
          Map2Cross(DerefObject),
          Map2Cross(Add),
          Map2Cross(WrapObject),
          Map2Match(JoinObject)))
    }

    "emit match and cross after a cross in a join-object" in {
      val input = """
        | medals := //summer_games/london_medals
        | five := new 5 
        |
        | five ~ medals
        |   fivePlus := five + medals.Weight
        |   { five: five, increasedWeight: fivePlus, weight: medals.Weight }
        | """.stripMargin

      testEmit(input)(
        Vector(
          PushString("five"),
          PushNum("5"),
          Map1(New),
          Dup,
          Swap(2),
          Swap(1),
          Map2Cross(WrapObject),
          PushString("weight"),
          PushString("/summer_games/london_medals"),
          Morph1(BuiltInMorphism1(expandGlob)),
          AbsoluteLoad,
          Dup,
          Swap(4),
          Swap(3),
          Swap(2),
          Swap(1),
          PushString("Weight"),
          Map2Cross(DerefObject),
          Map2Cross(WrapObject),
          PushString("increasedWeight"),
          Swap(1),
          Swap(2),
          Swap(3),
          Swap(1),
          Swap(2),
          Swap(3),
          Swap(4),
          PushString("Weight"),
          Map2Cross(DerefObject),
          Map2Cross(Add),
          Map2Cross(WrapObject),
          Map2Match(JoinObject),
          Map2Match(JoinObject)))
    }

    "emit a match and a cross in the correct order, after a cross, in a join-object" in {
      val input = """
        | medals := //summer_games/london_medals
        | five := new 5 
        |
        | five ~ medals
        |   fivePlus := five + medals.Weight
        |   six := new 6
        |
        |   six ~ fivePlus
        |     { five: five, increasedWeight: fivePlus, six: six }
        | """.stripMargin

      testEmit(input)(
        Vector(
          PushString("five"),
          PushNum("5"),
          Map1(New),
          Dup,
          Swap(2),
          Swap(1),
          Map2Cross(WrapObject),
          PushString("six"),
          PushNum("6"),
          Map1(New),
          Map2Cross(WrapObject),
          PushString("increasedWeight"),
          Swap(1),
          Swap(2),
          Swap(3),
          PushString("/summer_games/london_medals"),
          Morph1(BuiltInMorphism1(expandGlob)),
          AbsoluteLoad,
          PushString("Weight"),
          Map2Cross(DerefObject),
          Map2Cross(Add),
          Map2Cross(WrapObject),
          Map2Cross(JoinObject),
          Map2Match(JoinObject)))
    }

    "emit a match and a cross in the correct order, after a cross, from a dispatch, in a join-object" in {
      val input = """
        | medals := //summer_games/london_medals
        |
        | makeNew(x) := new x
        | five := makeNew(5)
        | six := makeNew(6)
        |
        | five ~ medals
        |   fivePlus := five + medals.Weight
        |
        |   six ~ fivePlus
        |     { five: five, increasedWeight: fivePlus, six: six }
        | """.stripMargin

      testEmit(input)(
        Vector(
          PushString("five"),
          PushNum("5"),
          Map1(New),
          Dup,
          Swap(2),
          Swap(1),
          Map2Cross(WrapObject),
          PushString("six"),
          PushNum("6"),
          Map1(New),
          Map2Cross(WrapObject),
          PushString("increasedWeight"),
          Swap(1),
          Swap(2),
          Swap(3),
          PushString("/summer_games/london_medals"),
          Morph1(BuiltInMorphism1(expandGlob)),
          AbsoluteLoad,
          PushString("Weight"),
          Map2Cross(DerefObject),
          Map2Cross(Add),
          Map2Cross(WrapObject),
          Map2Cross(JoinObject),
          Map2Match(JoinObject)))
    }

    "emit a match after a cross in a join-array" in {
      val input = """
        | medals := //summer_games/london_medals
        | five := new 5 
        |
        | five ~ medals
        |   fivePlus := five + medals.Weight
        |   [medals.Weight, fivePlus]
        | """.stripMargin

      testEmit(input)(
        Vector(
          PushString("/summer_games/london_medals"),
          Morph1(BuiltInMorphism1(expandGlob)),
          AbsoluteLoad,
          Dup,
          PushString("Weight"),
          Map2Cross(DerefObject),
          Map1(WrapArray),
          PushNum("5"),
          Map1(New),
          Swap(1),
          Swap(2),
          PushString("Weight"),
          Map2Cross(DerefObject),
          Map2Cross(Add),
          Map1(WrapArray),
          Map2Match(JoinArray)))
    }

    "emit two distinct callsites of the same function" in {
      val input = """
        | medals := //summer_games/london_medals 
        |   stats(x) := max(x) 
        |   [stats(medals.Weight), stats(medals.HeightIncm)]
        """.stripMargin

      testEmit(input)(
        Vector(
          PushString("/summer_games/london_medals"),
          Morph1(BuiltInMorphism1(expandGlob)),
          AbsoluteLoad,
          PushString("Weight"),
          Map2Cross(DerefObject),
          Reduce(BuiltInReduction(Reduction(Vector(), "max", 0x2001))),
          Map1(WrapArray),
          PushString("/summer_games/london_medals"),
          Morph1(BuiltInMorphism1(expandGlob)),
          AbsoluteLoad,
          PushString("HeightIncm"),
          Map2Cross(DerefObject),
          Reduce(BuiltInReduction(Reduction(Vector(), "max", 0x2001))),
          Map1(WrapArray),
          Map2Cross(JoinArray)))
    }

    "solve with a generic where inside a function" in {
      val input = """
        | medals := //summer_games/london_medals
        | athletes := //summer_games/athletes
        | 
        | data := athletes union (medals with { winner: medals."Medal winner" })
        | 
        | f(x, y) := x where y
        | 
        | solve 'winner 
        |   count(f(data.winner, data.winner = 'winner))
      """.stripMargin

      testEmit(input)(
        Vector(
          PushString("/summer_games/athletes"),
          Morph1(BuiltInMorphism1(expandGlob)),
          AbsoluteLoad,
          PushString("/summer_games/london_medals"),
          Morph1(BuiltInMorphism1(expandGlob)),
          AbsoluteLoad,
          Dup,
          Swap(2),
          Swap(1),
          PushString("winner"),
          Swap(1),
          Swap(2),
          Swap(3),
          PushString("Medal winner"),
          Map2Cross(DerefObject),
          Map2Cross(WrapObject),
          Map2Match(JoinObject),
          IUnion,
          Dup,
          PushString("winner"),
          Map2Cross(DerefObject),
          KeyPart(1),
          Swap(1),
          PushString("winner"),
          Map2Cross(DerefObject),
          Group(0),
          Split,
          PushGroup(0),
          Reduce(BuiltInReduction(Reduction(Vector(), "count", 0x2000))),
          Merge))
    }

    "emit two distinct callsites of the same function version 2" in {
      val input = """
        | medals := //summer_games/london_medals 
        | stats(x) := max(x) + min(x)
        | stats(medals.Weight) - stats(medals.HeightIncm)
        """.stripMargin

      testEmit(input)(
        Vector(
          PushString("/summer_games/london_medals"),
          Morph1(BuiltInMorphism1(expandGlob)),
          AbsoluteLoad,
          PushString("Weight"),
          Map2Cross(DerefObject),
          Dup,
          Reduce(BuiltInReduction(Reduction(Vector(), "max", 0x2001))),
          Swap(1),
          Reduce(BuiltInReduction(Reduction(Vector(), "min", 0x2004))),
          Map2Cross(Add),
          PushString("/summer_games/london_medals"),
          Morph1(BuiltInMorphism1(expandGlob)),
          AbsoluteLoad,
          PushString("HeightIncm"),
          Map2Cross(DerefObject),
          Dup,
          Swap(2),
          Swap(1),
          Reduce(BuiltInReduction(Reduction(Vector(), "max", 0x2001))),
          Swap(1),
          Swap(2),
          Reduce(BuiltInReduction(Reduction(Vector(), "min", 0x2004))),
          Map2Cross(Add),
          Map2Cross(Sub)))
    }

    "emit wrapped object as right side of Let" in {
      testEmit("clicks := //clicks {foo: clicks}")(
        Vector(
          PushString("foo"),
          PushString("/clicks"),
          Morph1(BuiltInMorphism1(expandGlob)),
          AbsoluteLoad,
          Map2Cross(WrapObject)))
    }    
    
    "emit matched join of wrapped object for object with two fields having same provenance" in {
      testEmit("clicks := //clicks {foo: clicks, bar: clicks}")(
        Vector(
          PushString("foo"),
          PushString("/clicks"),
          Morph1(BuiltInMorphism1(expandGlob)),
          AbsoluteLoad,
          Dup,
          Swap(2),
          Swap(1),
          Map2Cross(WrapObject),
          PushString("bar"),
          Swap(1),
          Swap(2),
          Map2Cross(WrapObject),
          Map2Match(JoinObject)))
    }

    "emit empty array" in {
      testEmit("[]")(Vector(PushArray))
    }

    "emit wrap array for array with single element having constant string value" in {
      testEmit("[\"foo\"]")(
        Vector(
          PushString("foo"),
          Map1(WrapArray)))
    }

    "emit wrap array for array with single element having null value" in {
      testEmit("[null]")(
        Vector(
          PushNull,
          Map1(WrapArray)))
    }

    "emit join of wrapped arrays for array with two elements having constant values" in {
      testEmit("[\"foo\", true]")(
        Vector(
          PushString("foo"),
          Map1(WrapArray),
          PushTrue,
          Map1(WrapArray),
          Map2Cross(JoinArray)))
    }

    "emit join of wrapped arrays for array with four elements having either value provenance or static provenance" in {
      val input = """
        | a := new 4
        | [a, 5, a, 5]
        | """.stripMargin

      testEmit(input)(
        Vector(
          PushNum("5"),
          Map1(WrapArray),
          PushNum("5"),
          Map1(WrapArray),
          Map2Cross(JoinArray),
          PushNum("4"),
          Map1(New),
          Dup,
          Swap(2),
          Swap(1),
          Map1(WrapArray),
          Swap(1),
          Swap(2),
          Map1(WrapArray),
          Map2Match(JoinArray),
          Map2Cross(JoinArray),
          PushNum("1"),
          Map2Cross(ArraySwap),
          PushNum("3"),
          Map2Cross(ArraySwap),
          PushNum("2"),
          Map2Cross(ArraySwap)))
    }

    "emit join of wrapped arrays for array with four elements having values from two static provenances" in {
      testEmit("foo := //foo bar := //bar foo ~ bar [foo.a, bar.a, foo.b, bar.b]")(
        Vector(
          PushString("/bar"),
          Morph1(BuiltInMorphism1(expandGlob)),
          AbsoluteLoad,
          Dup,
          PushString("a"),
          Map2Cross(DerefObject),
          Map1(WrapArray),
          Swap(1),
          PushString("b"),
          Map2Cross(DerefObject),
          Map1(WrapArray),
          Map2Match(JoinArray),
          PushString("/foo"),
          Morph1(BuiltInMorphism1(expandGlob)),
          AbsoluteLoad,
          Dup,
          Swap(2),
          Swap(1),
          PushString("a"),
          Map2Cross(DerefObject),
          Map1(WrapArray),
          Swap(1),
          Swap(2),
          PushString("b"),
          Map2Cross(DerefObject),
          Map1(WrapArray),
          Map2Match(JoinArray),
          Map2Cross(JoinArray),
          PushNum("1"),
          Map2Cross(ArraySwap),
          PushNum("3"),
          Map2Cross(ArraySwap),
          PushNum("2"),
          Map2Cross(ArraySwap)))
    }

    "emit descent for object load" in {
      testEmit("clicks := //clicks clicks.foo")(
        Vector(
          PushString("/clicks"),
          Morph1(BuiltInMorphism1(expandGlob)),
          AbsoluteLoad,
          PushString("foo"),
          Map2Cross(DerefObject)))
    }

    "emit meta descent for object load" in {
      testEmit("clicks := //clicks clicks@foo")(
        Vector(
          PushString("/clicks"),
          Morph1(BuiltInMorphism1(expandGlob)),
          AbsoluteLoad,
          PushString("foo"),
          Map2Cross(DerefMetadata)))
    }

    "emit descent for array load" in {
      testEmit("clicks := //clicks clicks[1]")(
        Vector(
          PushString("/clicks"),
          Morph1(BuiltInMorphism1(expandGlob)),
          AbsoluteLoad,
          PushNum("1"),
          Map2Cross(DerefArray)))
    }

    "emit load of literal load" in {
      testEmit("""load("foo")""")(
        Vector(
          PushString("foo"),
          AbsoluteLoad)
      )
    }

    "emit filter cross for where loads from value provenance" in {
      testEmit("""1 where true""")(
        Vector(
          PushNum("1"),
          PushTrue,
          FilterCross))
    }

    "emit filter cross for where loads from value provenance" in {
      testEmit("""//clicks where (//clicks).foo = null""")(
        Vector(
          PushString("/clicks"),
          Morph1(BuiltInMorphism1(expandGlob)),
          AbsoluteLoad,
          PushString("/clicks"),
          Morph1(BuiltInMorphism1(expandGlob)),
          AbsoluteLoad,
          PushString("foo"),
          Map2Cross(DerefObject),
          PushNull,
          Map2Cross(Eq),
          FilterMatch))
    }

    "emit descent for array load with non-constant indices" in {
      testEmit("clicks := //clicks clicks[clicks]")(
        Vector(
          PushString("/clicks"),
          Morph1(BuiltInMorphism1(expandGlob)),
          AbsoluteLoad,
          Dup,
          Swap(1),
          Map2Match(DerefArray)))
    }

    "emit filter match for where loads from same provenance" in {
      testEmit("""foo := load("foo") foo where foo""")(
        Vector(
          PushString("foo"),
          AbsoluteLoad,
          Dup,
          Swap(1),
          FilterMatch))
    }

    "emit filter match for loads from same provenance when performing equality filter" in {
      testEmit("foo := //foo foo where foo.id = 2")(
        Vector(
          PushString("/foo"),
          Morph1(BuiltInMorphism1(expandGlob)),
          AbsoluteLoad,
          Dup,
          Swap(1),
          PushString("id"),
          Map2Cross(DerefObject),
          PushNum("2"),
          Map2Cross(Eq),
          FilterMatch))
    }

    "use dup bytecode to duplicate the same load" in {
      testEmit("""clicks := load("foo") clicks + clicks""")(
        Vector(
          PushString("foo"),
          AbsoluteLoad,
          Dup,
          Swap(1),
          Map2Match(Add)))
    }

    "use dup bytecode non-locally" in {
      testEmit("""clicks := load("foo") two := 2 * clicks two + clicks""")(
        Vector(
          PushNum("2"),
          PushString("foo"),
          AbsoluteLoad,
          Dup,
          Swap(2),
          Swap(1),
          Map2Cross(Mul),
          Swap(1),
          Map2Match(Add)))
    }

    "emit morphism1" in {
      // std::random::foobar(12) is not valid Quirrel and results in a compiler error
      val rand = Morphism1(Vector("std", "random"), "foobar", 0x0006)

      forall(libMorphism1 - rand) { f =>
        testEmit("""%s(4224)""".format(f.fqn))(
          Vector(
            PushNum("4224"),
            Morph1(BuiltInMorphism1(f))))
      }
    }

    "emit morphism2" in {
      forall(libMorphism2) { f =>
        testEmit("""%s(4224, 17)""".format(f.fqn))(
          Vector(
            PushNum("4224"),
            PushNum("17"),
            Morph2(BuiltInMorphism2(f))))
      }
    } 

    "emit count reduction" in {
      testEmit("count(1)")(
        Vector(
          PushNum("1"),
          Reduce(BuiltInReduction(Reduction(Vector(), "count", 0x2000)))))
    }

    "emit arbitrary reduction" in {
      forall(libReduction) { f => 
        testEmit("""%s(4224)""".format(f.fqn))(
          Vector(
            PushNum("4224"),
            Reduce(BuiltInReduction(f))))
      }
    }    

    "emit unary non-reduction with object deref" in {
      forall(lib1) { f => 
        testEmit("""%s((//foobar).baz)""".format(f.fqn))(
          Vector(
            PushString("/foobar"),
            Morph1(BuiltInMorphism1(expandGlob)),
            AbsoluteLoad,
            PushString("baz"),
            Map2Cross(DerefObject),
            Map1(BuiltInFunction1Op(f))))
      }
    }    
    
    "emit unary non-reduction" in {
      forall(lib1) { f => 
        testEmit("""%s("2012-02-29T00:44:52.599+08:00")""".format(f.fqn))(
          Vector(
            PushString("2012-02-29T00:44:52.599+08:00"),
            Map1(BuiltInFunction1Op(f))))
      }
    }    

    "emit binary non-reduction" in {
      forall(lib2) { f =>
        testEmit("""%s((//foo).time, (//foo).timeZone)""".format(f.fqn))(
          Vector(
            PushString("/foo"),
            Morph1(BuiltInMorphism1(expandGlob)),
            AbsoluteLoad,
            PushString("time"),
            Map2Cross(DerefObject),
            PushString("/foo"),
            Morph1(BuiltInMorphism1(expandGlob)),
            AbsoluteLoad,
            PushString("timeZone"),
            Map2Cross(DerefObject),
            Map2Match(BuiltInFunction2Op(f))))
      }
    }
 
    "emit body of fully applied characteristic function" in {
      testEmit("clicks := //clicks clicksFor(userId) := clicks where clicks.userId = userId clicksFor(\"foo\")")(
        Vector(
          PushString("/clicks"),
          Morph1(BuiltInMorphism1(expandGlob)),
          AbsoluteLoad,
          Dup,
          Swap(1),
          PushString("userId"),
          Map2Cross(DerefObject),
          PushString("foo"),
          Map2Cross(Eq),
          FilterMatch))
    }
    
    "emit body of a fully applied characteristic function with two variables" in {
      testEmit("""
        | fun(a, b) := 
        |   //campaigns where (//campaigns).ageRange = a & (//campaigns).gender = b
        | fun([25,36],
          "female")""".stripMargin)(
        Vector(
          PushString("/campaigns"),
          Morph1(BuiltInMorphism1(expandGlob)),
          AbsoluteLoad,
          PushString("/campaigns"),
          Morph1(BuiltInMorphism1(expandGlob)),
          AbsoluteLoad,
          PushString("ageRange"),
          Map2Cross(DerefObject),
          PushNum("25"),
          Map1(WrapArray),
          PushNum("36"),
          Map1(WrapArray),
          Map2Cross(JoinArray),
          Map2Cross(Eq),
          PushString("/campaigns"),
          Morph1(BuiltInMorphism1(expandGlob)),
          AbsoluteLoad,
          PushString("gender"),
          Map2Cross(DerefObject),
          PushString("female"),
          Map2Cross(Eq),
          Map2Match(And),
          FilterMatch))
    }

    "emit match for first-level union provenance" in {
      testEmit("a := //a b := //b a ~ b (b.x - a.x) * (a.y - b.y)")(
        Vector(
          PushString("/b"),
          Morph1(BuiltInMorphism1(expandGlob)),
          AbsoluteLoad,
          Dup,
          PushString("x"),
          Map2Cross(DerefObject),
          PushString("/a"),
          Morph1(BuiltInMorphism1(expandGlob)),
          AbsoluteLoad,
          Dup,
          Swap(2),
          Swap(1),
          PushString("x"),
          Map2Cross(DerefObject),
          Map2Cross(Sub),
          Swap(1),
          PushString("y"),
          Map2Cross(DerefObject),
          Swap(1),
          Swap(2),
          PushString("y"),
          Map2Cross(DerefObject),
          Map2Cross(Sub),
          Map2Match(Mul)),
        Vector(
          PushString("/b"),
          Morph1(BuiltInMorphism1(expandGlob)),
          AbsoluteLoad,
          Dup,
          Dup,
          Dup,
          Swap(1),
          Dup,
          Map2Match(Eq),
          FilterMatch,
          PushString("x"),
          Map2Cross(DerefObject),
          PushString("/a"),
          Morph1(BuiltInMorphism1(expandGlob)),
          AbsoluteLoad,
          Dup,
          Swap(2),
          Swap(1),
          Dup,
          Swap(2),
          Swap(1),
          Dup,
          Swap(2),
          Swap(1),
          Swap(1),
          Swap(2),
          Dup,
          Map2Match(Eq),
          FilterMatch,
          PushString("x"),
          Map2Cross(DerefObject),
          Map2Cross(Sub),
          Swap(1),
          Swap(1),
          Swap(2),
          Dup,
          Map2Match(Eq),
          FilterMatch,
          PushString("y"),
          Map2Cross(DerefObject),
          Swap(1),
          Swap(2),
          Swap(1),
          Swap(2),
          Swap(3),
          Dup,
          Map2Match(Eq),
          FilterMatch,
          PushString("y"),
          Map2Cross(DerefObject),
          Map2Cross(Sub),
          Map2Match(Mul)))
    }

    "emit split and merge for trivial cf example" in {
      testEmit("clicks := //clicks onDay := solve 'day clicks where clicks.day = 'day onDay")(
        Vector(
          PushString("/clicks"),
          Morph1(BuiltInMorphism1(expandGlob)),
          AbsoluteLoad,
          Dup,
          PushString("day"),
          Map2Cross(DerefObject),
          KeyPart(1),
          Swap(1),
          Group(0),
          Split,
          PushGroup(0),
          Merge))
    }

    "emit split and merge for solve with constraint" in {
      testEmit("""
        | foo := //foo
        | bar := //bar
        |
        | solve 'a = bar.a
        |   count(foo where foo.a = 'a)
        | """)(Vector(
          PushString("/foo"),
          Morph1(BuiltInMorphism1(expandGlob)),
          AbsoluteLoad,
          Dup,
          PushString("a"),
          Map2Cross(DerefObject),
          KeyPart(1),
          Swap(1),
          Group(0),
          PushString("/bar"),
          Morph1(BuiltInMorphism1(expandGlob)),
          AbsoluteLoad,
          Dup,
          Swap(2),
          Swap(1),
          PushString("a"),
          Map2Cross(DerefObject),
          KeyPart(1),
          Swap(1),
          Swap(2),
          PushString("a"),
          Map2Cross(DerefObject),
          Group(2),
          MergeBuckets(true),
          Split,
          PushGroup(0),
          Reduce(BuiltInReduction(Reduction(Vector(), "count", 0x2000))),
          Merge))
    }
    
    "emit merge_buckets & for trivial cf example with conjunction" in {
      testEmit("clicks := //clicks onDay := solve 'day clicks where clicks.day = 'day & clicks.din = 'day onDay")(
        Vector(
          PushString("/clicks"),
          Morph1(BuiltInMorphism1(expandGlob)),
          AbsoluteLoad,
          Dup,
          Dup,
          PushString("day"),
          Map2Cross(DerefObject),
          KeyPart(1),
          Swap(1),
          PushString("din"),
          Map2Cross(DerefObject),
          KeyPart(1),
          MergeBuckets(true),
          Swap(1),
          Group(0),
          Split,
          PushGroup(0),
          Merge))
    }

    "emit merge_buckets | for trivial cf example with disjunction" in {
      testEmit("clicks := //clicks onDay := solve 'day clicks where clicks.day = 'day | clicks.din = 'day onDay")(
        Vector(
          PushString("/clicks"),
          Morph1(BuiltInMorphism1(expandGlob)),
          AbsoluteLoad,
          Dup,
          Dup,
          PushString("day"),
          Map2Cross(DerefObject),
          KeyPart(1),
          Swap(1),
          PushString("din"),
          Map2Cross(DerefObject),
          KeyPart(1),
          MergeBuckets(false),
          Swap(1),
          Group(0),
          Split,
          PushGroup(0),
          Merge))
    }
    
    "emit split and merge for cf example with paired tic variables in critical condition" in {
      testEmit("""
        | clicks := //clicks
        | solve 'a, 'b
        |   clicks' := clicks where clicks.time = 'a & clicks.pageId = 'b
        |   clicks'
        | """)(
        Vector(
          PushString("/clicks"),
          Morph1(BuiltInMorphism1(expandGlob)),
          AbsoluteLoad,
          Dup,
          Dup,
          PushString("time"),
          Map2Cross(DerefObject),
          KeyPart(1),
          Swap(1),
          PushString("pageId"),
          Map2Cross(DerefObject),
          KeyPart(2),
          MergeBuckets(true),
          Swap(1),
          Group(0),
          Split,
          PushGroup(0),
          Merge))
    }
    
    "emit split and merge for cf example with consecutively-constrained paired tic variables on a single set" in {
      testEmit("""
        | organizations := //organizations
        | 
        | hist := solve 'revenue, 'campaign 
        |   organizations' := organizations where organizations.revenue = 'revenue
        |   organizations'' := organizations' where organizations'.campaign = 'campaign
        |   
        |   organizations''
        |   
        | hist""".stripMargin)(Vector())
    }.pendingUntilFixed
    
    "emit split and merge for cf example with single, multiply constrained tic variable" in {
      testEmit("""
        | clicks := //clicks
        | foo := solve 'a
        |   bar := clicks where clicks.a = 'a
        |   baz := clicks where clicks.b = 'a
        |
        |   bar.a + baz.b
        |
        | foo""".stripMargin)(
        Vector(
          PushString("/clicks"),
          Morph1(BuiltInMorphism1(expandGlob)),
          AbsoluteLoad,
          Dup,
          Dup,
          Dup,
          PushString("a"),
          Map2Cross(DerefObject),
          KeyPart(1),
          Swap(1),
          Group(0),
          Swap(1),
          PushString("b"),
          Map2Cross(DerefObject),
          KeyPart(1),
          Swap(1),
          Swap(2),
          Group(2),
          MergeBuckets(true),
          Split,
          PushGroup(0),
          PushString("a"),
          Map2Cross(DerefObject),
          PushGroup(2),
          PushString("b"),
          Map2Cross(DerefObject),
          Map2Match(Add),
          Merge))
    }
    
    "emit split and merge for cf example with independent tic variables on same set" in {
      testEmit("""
        | clicks := //clicks
        | 
        | foo := solve 'a, 'b
        |   bar := clicks where clicks.a = 'a
        |   baz := clicks where clicks.b = 'b
        |
        |   bar.a + baz.b
        |
        | foo""".stripMargin)(
        Vector(
          PushString("/clicks"),
          Morph1(BuiltInMorphism1(expandGlob)),
          AbsoluteLoad,
          Dup,
          Dup,
          Dup,
          PushString("a"),
          Map2Cross(DerefObject),
          KeyPart(1),
          Swap(1),
          Group(0),
          Swap(1),
          PushString("b"),
          Map2Cross(DerefObject),
          KeyPart(3),
          Swap(1),
          Swap(2),
          Group(2),
          MergeBuckets(true),
          Split,
          PushGroup(0),
          PushString("a"),
          Map2Cross(DerefObject),
          PushGroup(2),
          PushString("b"),
          Map2Cross(DerefObject),
          Map2Match(Add),
          Merge))
    }
    
    "emit split and merge for cf example with independent tic variables on different sets" in {
      testEmit("""
        | clicks := //clicks
        | imps := //impressions
        | 
        | foo := solve 'a, 'b
        |   bar := clicks where clicks.a = 'a
        |   baz := imps where imps.b = 'b
        |
        |   bar ~ baz
        |     bar.a + baz.b
        |
        | foo""".stripMargin)(
        Vector(
          PushString("/clicks"),
          Morph1(BuiltInMorphism1(expandGlob)),
          AbsoluteLoad,
          Dup,
          PushString("a"),
          Map2Cross(DerefObject),
          KeyPart(1),
          Swap(1),
          Group(0),
          PushString("/impressions"),
          Morph1(BuiltInMorphism1(expandGlob)),
          AbsoluteLoad,
          Dup,
          Swap(2),
          Swap(1),
          PushString("b"),
          Map2Cross(DerefObject),
          KeyPart(3),
          Swap(1),
          Swap(2),
          Group(2),
          MergeBuckets(true),
          Split,
          PushGroup(0),
          PushString("a"),
          Map2Cross(DerefObject),
          PushGroup(2),
          PushString("b"),
          Map2Cross(DerefObject),
          Map2Cross(Add),
          Merge))
    }
    
    "emit split and merge for cf example with extra sets" in {
      testEmit("""
        | clicks := //clicks
        | foo := solve 'a clicks where clicks = 'a & clicks.b = 42
        | foo""".stripMargin)(
        Vector(
          PushString("/clicks"),
          Morph1(BuiltInMorphism1(expandGlob)),
          AbsoluteLoad,
          Dup,
          Dup,
          KeyPart(1),
          Swap(1),
          PushString("b"),
          Map2Cross(DerefObject),
          PushNum("42"),
          Map2Cross(Eq),
          Extra,
          MergeBuckets(true),
          Swap(1),
          Group(0),
          Split,
          PushGroup(0),
          Merge))
    }

    "emit split and merge for rr cf example" in {
      testEmit("""
        | clicks := //clicks
        | 
        | totalPairs(sessionId) :=
        |   solve 'time
        |     clicks where clicks.externalSessionId = 'time & clicks.datetime = sessionId
        |   
        | totalPairs("fubar")""".stripMargin)(Vector())
    }.pendingUntilFixed     // TODO this *really* should be working
    
    "emit split and merge for ctr example" in {
      testEmit("""
        | clicks := //clicks
        | imps := //impressions
        | solve 'day
        |   count(clicks where clicks.day = 'day) / count(imps where imps.day = 'day)
        | """.stripMargin)(
        Vector(
          PushString("/clicks"),
          Morph1(BuiltInMorphism1(expandGlob)),
          AbsoluteLoad,
          Dup,
          PushString("day"),
          Map2Cross(DerefObject),
          KeyPart(1),
          Swap(1),
          Group(0),
          PushString("/impressions"),
          Morph1(BuiltInMorphism1(expandGlob)),
          AbsoluteLoad,
          Dup,
          Swap(2),
          Swap(1),
          PushString("day"),
          Map2Cross(DerefObject),
          KeyPart(1),
          Swap(1),
          Swap(2),
          Group(2),
          MergeBuckets(true),
          Split,
          PushGroup(0),
          Reduce(BuiltInReduction(Reduction(Vector(), "count", 0x2000))),
          PushGroup(2),
          Reduce(BuiltInReduction(Reduction(Vector(), "count", 0x2000))),
          Map2Cross(Div),
          Merge))
    }
    
    "emit dup for merge results" in {
      val input = """
        | clicks := //clicks
        | f := solve 'c count(clicks where clicks = 'c)
        | f.a + f.b""".stripMargin

      testEmit(input)(
        Vector(
          PushString("/clicks"),
          Morph1(BuiltInMorphism1(expandGlob)),
          AbsoluteLoad,
          Dup,
          KeyPart(1),
          Swap(1),
          Group(0),
          Split,
          PushGroup(0),
          Reduce(BuiltInReduction(Reduction(Vector(), "count", 0x2000))),
          Merge,
          Dup,
          PushString("a"),
          Map2Cross(DerefObject),
          Swap(1),
          PushString("b"),
          Map2Cross(DerefObject),
          Map2Match(Add)))
    }

    "emit code for an unquantified characteristic function" in {
      val input = """
        | campaigns := //campaigns
        | nums := distinct(campaigns.cpm where campaigns.cpm < 10)
        | sums := solve 'n
        |   m := max(nums where nums < 'n)
        |   (nums where nums = 'n) + m 
        | sums""".stripMargin

      testEmit(input)(Vector(
        PushString("/campaigns"),
        Morph1(BuiltInMorphism1(expandGlob)),
        AbsoluteLoad,
        Dup,
        PushString("cpm"),
        Map2Cross(DerefObject),
        Swap(1),
        PushString("cpm"),
        Map2Cross(DerefObject),
        PushNum("10"),
        Map2Cross(Lt),
        FilterMatch,
        Distinct,
        Dup,
        Dup,
        Dup,
        KeyPart(1),
        Swap(1),
        Group(0),
        Split,
        PushGroup(0),
        Swap(1),
        Swap(1),
        Swap(2),
        PushKey(1),
        Map2Cross(Lt),
        FilterMatch,
        Reduce(BuiltInReduction(Reduction(Vector(), "max", 0x2001))),
        Map2Cross(Add),
        Merge))
    }

    "determine a histogram of a composite key of revenue and campaign" >> {
      testEmit("""
        | campaigns := //campaigns
        | organizations := //organizations
        | 
        | solve 'revenue = organizations.revenue, 'campaign = organizations.campaign
        |   campaigns' := campaigns where campaigns.campaign = 'campaign
        |   { revenue: 'revenue, num: count(campaigns') }
        | """.stripMargin)(
        Vector(
          PushString("/campaigns"),
          Morph1(BuiltInMorphism1(expandGlob)),
          AbsoluteLoad,
          Dup,
          PushString("campaign"),
          Map2Cross(DerefObject),
          KeyPart(1),
          Swap(1),
          Group(0),
          PushString("/organizations"),
          Morph1(BuiltInMorphism1(expandGlob)),
          AbsoluteLoad,
          Dup,
          Swap(2),
          Swap(1),
          Dup,
          Swap(2),
          Swap(1),
          Dup,
          Swap(2),
          Swap(1),
          PushString("revenue"),
          Map2Cross(DerefObject),
          KeyPart(3),
          Swap(1),
          Swap(2),
          PushString("revenue"),
          Map2Cross(DerefObject),
          Group(2),
          Swap(1),
          Swap(2),
          PushString("campaign"),
          Map2Cross(DerefObject),
          KeyPart(1),
          Swap(1),
          Swap(2),
          Swap(3),
          PushString("campaign"),
          Map2Cross(DerefObject),
          Group(4),
          MergeBuckets(true),
          MergeBuckets(true),
          Split,
          PushString("revenue"),
          PushKey(3),
          Map2Cross(WrapObject),
          PushString("num"),
          PushGroup(0),
          Reduce(BuiltInReduction(Reduction(Vector(), "count", 0x2000))),
          Map2Cross(WrapObject),
          Map2Cross(JoinObject),
          Merge))
    }

    "emit code in case when an error is supressed" in {
      val input = """
        | clicks := //clicks
        | views := //views
        | solve 'b
        |   k := clicks.time where clicks.time = 'b
        |   j := views.time where views.time > 'b
        |   k ~ j
        |     { kay: k, jay: j }
        | """.stripMargin

      testEmit(input)(
        Vector(
          PushString("/clicks"),
          Morph1(BuiltInMorphism1(expandGlob)),
          AbsoluteLoad,
          Dup,
          PushString("time"),
          Map2Cross(DerefObject),
          KeyPart(1),
          Swap(1),
          PushString("time"),
          Map2Cross(DerefObject),
          Group(0),
          Split,
          PushString("kay"),
          PushGroup(0),
          Map2Cross(WrapObject),
          PushString("jay"),
          PushString("/views"),
          Morph1(BuiltInMorphism1(expandGlob)),
          AbsoluteLoad,
          Dup,
          Swap(3),
          Swap(2),
          Swap(1),
          PushString("time"),
          Map2Cross(DerefObject),
          Swap(1),
          Swap(2),
          Swap(3),
          PushString("time"),
          Map2Cross(DerefObject),
          PushKey(1),
          Map2Cross(Gt),
          FilterMatch,
          Map2Cross(WrapObject),
          Map2Cross(JoinObject),
          Merge))
    }
    
    "emit code for examples" in {
      "deviant-durations.qrl" >> {
        testEmit("""
          | interactions := //interactions
          | 
          | solve 'userId
          |   userInteractions := interactions where interactions.userId = 'userId
          |   
          |   m := mean(userInteractions.duration)
          |   sd := stdDev(userInteractions.duration)
          | 
          |   {
          |     userId: 'userId,
          |     interaction: userInteractions where userInteractions.duration > m + (sd * 3)
          |   }
          """.stripMargin)(
          Vector(
            PushString("/interactions"),
            Morph1(BuiltInMorphism1(expandGlob)),
            AbsoluteLoad,
            Dup,
            PushString("userId"),
            Map2Cross(DerefObject),
            KeyPart(1),
            Swap(1),
            Group(0),
            Split,
            PushString("userId"),
            PushKey(1),
            Map2Cross(WrapObject),
            PushString("interaction"),
            PushGroup(0),
            Dup,
            Swap(3),
            Swap(2),
            Swap(1),
            Dup,
            Swap(3),
            Swap(2),
            Swap(1),
            Dup,
            Swap(3),
            Swap(2),
            Swap(1),
            Swap(1),
            Swap(2),
            Swap(3),
            PushString("duration"),
            Map2Cross(DerefObject),
            Swap(1),
            Swap(2),
            Swap(3),
            Swap(4),
            PushString("duration"),
            Map2Cross(DerefObject),
            Reduce(BuiltInReduction(Reduction(Vector(), "mean", 0x2013))),
            Swap(1),
            Swap(2),
            Swap(3),
            Swap(4),
            Swap(5),
            PushString("duration"),
            Map2Cross(DerefObject),
            Reduce(BuiltInReduction(Reduction(Vector(), "stdDev", 0x2007))),
            PushNum("3"),
            Map2Cross(Mul),
            Map2Cross(Add),
            Map2Cross(Gt),
            FilterMatch,
            Map2Cross(WrapObject),
            Map2Cross(JoinObject),
            Merge))
      }
      
      "first-conversion.qrl" >> {
        testEmit("""
          | solve 'userId
          |   conversions' := //conversions
          |   impressions' := //impressions
          | 
          |   conversions := conversions' where conversions'.userId = 'userId
          |   impressions := impressions' where impressions'.userId = 'userId
          | 
          |   solve 'time
          |     impressionTimes := impressions.time where impressions.time = 'time
          |     conversionTimes :=
          |       conversions.time where conversions.time = min(conversions.time where conversions.time > 'time)
          |     
          |     conversionTimes ~ impressionTimes
          |       { impression: impressions, nextConversion: conversions }
          """.stripMargin)(
            Vector(
              PushString("/conversions"),
              Morph1(BuiltInMorphism1(expandGlob)),
              AbsoluteLoad,
              Dup,
              PushString("userId"),
              Map2Cross(DerefObject),
              KeyPart(1),
              Swap(1),
              Group(0),
              PushString("/impressions"),
              Morph1(BuiltInMorphism1(expandGlob)),
              AbsoluteLoad,
              Dup,
              Swap(2),
              Swap(1),
              PushString("userId"),
              Map2Cross(DerefObject),
              KeyPart(1),
              Swap(1),
              Swap(2),
              Group(2),
              MergeBuckets(true),
              Split,
              PushGroup(2),
              Dup,
              Dup,
              PushString("time"),
              Map2Cross(DerefObject),
              KeyPart(4),
              Swap(1),
              PushString("time"),
              Map2Cross(DerefObject),
              Group(3),
              Split,
              PushString("nextConversion"),
              PushGroup(0),
              Dup,
              Swap(2),
              Swap(1),
              Dup,
              Swap(2),
              Swap(1),
              Dup,
              Swap(2),
              Swap(1),
              Dup,
              Swap(2),
              Swap(1),
              Swap(1),
              Swap(2),
              PushString("time"),
              Map2Cross(DerefObject),
              Swap(1),
              Swap(2),
              Swap(3),
              PushString("time"),
              Map2Cross(DerefObject),
              Swap(1),
              Swap(2),
              Swap(3),
              Swap(4),
              PushString("time"),
              Map2Cross(DerefObject),
              Swap(1),
              Swap(2),
              Swap(3),
              Swap(4),
              Swap(5),
              PushString("time"),
              Map2Cross(DerefObject),
              PushKey(4),
              Map2Cross(Gt),
              FilterMatch,
              Reduce(BuiltInReduction(Reduction(Vector(), "min", 0x2004))),
              Map2Cross(Eq),
              FilterMatch,
              Dup,
              Map2Match(Eq),
              FilterMatch,
              Map2Cross(WrapObject),
              PushString("impression"),
              Swap(1),
              Swap(2),
              PushGroup(3),
              Dup,
              Map2Match(Eq),
              FilterMatch,
              Map2Cross(WrapObject),
              Map2Cross(JoinObject),
              Merge,
              Merge))
      }

      "histogram.qrl" >> {
        testEmit("""
          | clicks := //clicks
          | 
          | histogram := solve 'value
          |   { cnt: count(clicks where clicks = 'value), value: 'value }
          |   
          | histogram
          """.stripMargin)(
          Vector(
            PushString("/clicks"),
            Morph1(BuiltInMorphism1(expandGlob)),
            AbsoluteLoad,
            Dup,
            KeyPart(1),
            Swap(1),
            Group(0),
            Split,
            PushString("cnt"),
            PushGroup(0),
            Reduce(BuiltInReduction(Reduction(Vector(), "count", 0x2000))),
            Map2Cross(WrapObject),
            PushString("value"),
            PushKey(1),
            Map2Cross(WrapObject),
            Map2Cross(JoinObject),
            Merge))
      }
    }

    // Regression test for #PLATFORM-503 (Pivotal #39652091)
    "not emit dups inside functions (might have let bindings with formals)" in {
      val input = """
        |
        | f(x) :=
        |   medals := //summer_games/london_medals
        |   medals' := medals where medals.Country = x
        |   medals'' := new medals'
        |
        |   medals'' ~ medals'
        |     {a: medals'.Country, b: medals''.Country} where medals'.Total = medals''.Total
        |
        | f("India") union f("Canada")
        """.stripMargin

      testEmit(input)(
        Vector(
          PushString("a"),
          PushString("/summer_games/london_medals"),
          Morph1(BuiltInMorphism1(expandGlob)),
          AbsoluteLoad,
          Dup,
          Swap(2),
          Swap(1),
          Swap(1),
          Swap(2),
          PushString("Country"),
          Map2Cross(DerefObject),
          PushString("India"),
          Map2Cross(Eq),
          FilterMatch,
          Dup,
          Swap(2),
          Swap(1),
          Dup,
          Swap(2),
          Swap(1),
          PushString("Country"),
          Map2Cross(DerefObject),
          Map2Cross(WrapObject),
          PushString("b"),
          Swap(1),
          Swap(2),
          Map1(New),
          Dup,
          Swap(4),
          Swap(3),
          Swap(2),
          Swap(1),
          PushString("Country"),
          Map2Cross(DerefObject),
          Map2Cross(WrapObject),
          Map2Cross(JoinObject),
          Swap(1),
          PushString("Total"),
          Map2Cross(DerefObject),
          Swap(1),
          Swap(2),
          PushString("Total"),
          Map2Cross(DerefObject),
          Map2Cross(Eq),
          FilterMatch,
          PushString("a"),
          PushString("/summer_games/london_medals"),
          Morph1(BuiltInMorphism1(expandGlob)),
          AbsoluteLoad,
          Dup,
          Swap(3),
          Swap(2),
          Swap(1),
          Swap(1),
          Swap(2),
          Swap(3),
          PushString("Country"),
          Map2Cross(DerefObject),
          PushString("Canada"),
          Map2Cross(Eq),
          FilterMatch,
          Dup,
          Swap(3),
          Swap(2),
          Swap(1),
          Dup,
          Swap(3),
          Swap(2),
          Swap(1),
          PushString("Country"),
          Map2Cross(DerefObject),
          Map2Cross(WrapObject),
          PushString("b"),
          Swap(1),
          Swap(2),
          Swap(3),
          Map1(New),
          Dup,
          Swap(5),
          Swap(4),
          Swap(3),
          Swap(2),
          Swap(1),
          PushString("Country"),
          Map2Cross(DerefObject),
          Map2Cross(WrapObject),
          Map2Cross(JoinObject),
          Swap(1),
          Swap(2),
          PushString("Total"),
          Map2Cross(DerefObject),
          Swap(1),
          Swap(2),
          Swap(3),
          PushString("Total"),
          Map2Cross(DerefObject),
          Map2Cross(Eq),
          FilterMatch,
          IUnion))
    }

    // Regression test for #PLATFORM-652
    "if/then/else compiles into match instead of cross" in {
      val input = """
        | conversions := //conversions
        | conversions with
        |   {female: if conversions.customer.gender = "female" then 1 else 0}
        """.stripMargin

      testEmit(input)(
        Vector(
          PushString("/conversions"),
          Morph1(BuiltInMorphism1(expandGlob)),
          AbsoluteLoad,
          Dup,
          Dup,
          PushString("female"),
          PushNum("1"),
          Swap(1),
          Swap(2),
          Swap(3),
          PushString("customer"),
          Map2Cross(DerefObject),
          PushString("gender"),
          Map2Cross(DerefObject),
          PushString("female"),
          Map2Cross(Eq),
          FilterCross,
          PushNum("0"),
          Swap(1),
          Swap(2),
          Swap(3),
          Swap(4),
          PushString("customer"),
          Map2Cross(DerefObject),
          PushString("gender"),
          Map2Cross(DerefObject),
          PushString("female"),
          Map2Cross(Eq),
          Map1(Comp),
          FilterCross,
          IUnion,
          Map2Cross(WrapObject),
          Map2Match(JoinObject)))
    }
    
    // regression test for PLATFORM-909
    "produce valid bytecode for double-constrained group set should produce" in {
      val input = """
        | foo := //foo
        | 
        | solve 'a
        |   foo' := foo where foo = 'a
        |   count(foo' where foo' = 'a)
        | """.stripMargin
        
      testEmit(input)(Vector(
        PushString("/foo"),
        Morph1(BuiltInMorphism1(expandGlob)),
        AbsoluteLoad,
        Dup,
        KeyPart(1),
        Swap(1),
        Group(0),
        Split,
        PushGroup(0),
        Dup,
        Swap(1),
        PushKey(1),
        Map2Cross(Eq),
        FilterMatch,
        Reduce(BuiltInReduction(Reduction(Vector(), "count", 0x2000))),
        Merge))
    }
    
    "emit constraints defined within an inner parametric function" in {
      val input = """
        | foo := //foo
        |
        | solve 'a
        |   f(x) := foo where foo.a = 'a & foo.b = x
        |   f(42)
        | """.stripMargin
        
      emit(compileSingle(input)) must not(throwA[Throwable])
    }
    
    "not explode on consecutive solves with name-bound constraints" in {
      val input = """
        | train := //train
        | 
        | cumProb := solve 'rank
        |   train where train = 'rank
        | 
        | buckets := solve 'rank
        |   minimum := cumProb where cumProb = 'rank
        |   minimum
        | 
        | buckets
        | """.stripMargin
        
      emit(compileSingle(input)) must not(throwA[Throwable])
    }
    
    "not explode on cond in solve" in {
      val input = """
        | foo := //foo
        | solve 'a
        |   foo where (if foo.a then foo.b else foo.c) = 'a""".stripMargin
        
      testEmit(input)(
        Vector(
          PushString("/foo"),
          Morph1(BuiltInMorphism1(expandGlob)),
          AbsoluteLoad,
          Dup,
          Dup,
          Dup,
          Dup,
          PushString("b"),
          Map2Cross(DerefObject),
          Swap(1),
          PushString("a"),
          Map2Cross(DerefObject),
          FilterMatch,
          Swap(1),
          PushString("c"),
          Map2Cross(DerefObject),
          Swap(1),
          Swap(2),
          PushString("a"),
          Map2Cross(DerefObject),
          Map1(Comp),
          FilterMatch,
          IUnion,
          KeyPart(1),
          Swap(1),
          Group(0),
          Split,
          PushGroup(0),
          Merge))
    }
    
    "not explode on an assert inside a solve" in {
      val input = """
        | data := //clicks
        |  
        | solve 'minexp
        |   assert true
        |   data where data = 'minexp
        | """.stripMargin
        
      emit(compileSingle(input)) must not(throwA[Throwable])
    }
    
    "correctly emit a matched binary operation on the results of a flatten" in {
      val input = """
        | foo := flatten([{ a: 1, b: 2 }, { a: 3, b: 4 }])
        | foo where foo.a = 1
        | """.stripMargin
        
      testEmit(input)(Vector(
        PushString("a"),
        PushNum("1"),
        Map2Cross(WrapObject),
        PushString("b"),
        PushNum("2"),
        Map2Cross(WrapObject),
        Map2Cross(JoinObject),
        Map1(WrapArray),
        PushString("a"),
        PushNum("3"),
        Map2Cross(WrapObject),
        PushString("b"),
        PushNum("4"),
        Map2Cross(WrapObject),
        Map2Cross(JoinObject),
        Map1(WrapArray),
        Map2Cross(JoinArray),
        Morph1(BuiltInMorphism1(flatten)),
        Dup,
        Swap(1),
        PushString("a"),
        Map2Cross(DerefObject),
        PushNum("1"),
        Map2Cross(Eq),
        FilterMatch))
    }

    "emit match in join of if-then-else inside user defined function" in {
      val input = """
        | clicks := //clicks2
        |
        | foo(data) :=
        |   bar := if (data.product.price = 7.99) then mean(data.product.price) else data.product.price
        |   [bar, data.customer.state]
        |
        | foo(clicks)
      """.stripMargin

      testEmit(input)(Vector(
        PushString("/clicks2"),
        Morph1(BuiltInMorphism1(expandGlob)),
        AbsoluteLoad,
        Dup,
        Dup,
        Dup,
        Dup,
        PushString("product"),
        Map2Cross(DerefObject),
        PushString("price"),
        Map2Cross(DerefObject),
        Reduce(BuiltInReduction(Reduction(Vector(), "mean", 0x2013))),
        Swap(1),
        PushString("product"),
        Map2Cross(DerefObject),
        PushString("price"),
        Map2Cross(DerefObject),
        PushNum("7.99"),
        Map2Cross(Eq),
        FilterCross,
        Swap(1),
        PushString("product"),
        Map2Cross(DerefObject),
        PushString("price"),
        Map2Cross(DerefObject),
        Swap(1),
        Swap(2),
        PushString("product"),
        Map2Cross(DerefObject),
        PushString("price"),
        Map2Cross(DerefObject),
        PushNum("7.99"),
        Map2Cross(Eq),
        Map1(Comp),
        FilterMatch,
        IUnion,
        Map1(WrapArray),
        Swap(1),
        PushString("customer"),
        Map2Cross(DerefObject),
        PushString("state"),
        Map2Cross(DerefObject),
        Map1(WrapArray),
        Map2Match(JoinArray)))
    }
  }
  
  /* val exampleDir = new File("quirrel/examples")
  
  if (exampleDir.exists) {
    "specification examples" >> {
      val pending = Set("relative-durations.qrl")
      
      for (file <- exampleDir.listFiles if file.getName endsWith ".qrl") {
        if (pending contains file.getName) {
          file.getName >> {
            val result = compileSingle(LineStream(Source.fromFile(file)))
            result.errors must beEmpty
            emit(result) must not(beEmpty)
          }.pendingUntilFixed
        } else {
          file.getName >> {
            val result = compileSingle(LineStream(Source.fromFile(file)))
            result.errors must beEmpty
            emit(result) must not(beEmpty)
          }
        }
      }
    }
  } else {
    "specification examples" >> skipped
  } */
}
