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

import com.precog.bytecode.Instructions
import com.precog.bytecode.RandomLibrary

import org.scalacheck.Prop
import org.specs2.ScalaCheck
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
    with ScalaCheck
    with StubPhases
    with Compiler
    with Emitter
    with RawErrors 
    with RandomLibrary {

  import instructions._

  def compileEmit(input: String) = {
    val tree = compile(input.stripMargin)
    tree.errors must beEmpty
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
          LoadLocal,
          PushString("bar"),
          LoadLocal,
          IUnion))
    }    
    
    "emit instruction for two intersected loads" in {
      testEmit("""load("foo") intersect load("bar")""")(
        Vector(
          PushString("foo"),
          LoadLocal,
          PushString("bar"),
          LoadLocal,
          IIntersect))
    }    

    "emit instruction for two set differenced loads" in {
      testEmit("""load("foo") difference load("bar")""")(
        Vector(
          PushString("foo"),
          LoadLocal,
          PushString("bar"),
          LoadLocal,
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
          LoadLocal,
          PushNum("2"),
          Map2Cross(Mul)))
    }

    "emit line information for cross for division of load in static provenance with load in value provenance" in {
      testEmitLine("load(\"foo\") * 2")(
        Vector(
          Line(1,"load(\"foo\") * 2"),
          PushString("foo"),
          LoadLocal,
          PushNum("2"),
          Map2Cross(Mul)))
    }

    "emit cross for division of load in static provenance with load in value provenance" in {
      testEmit("2 * load(\"foo\")")(
        Vector(
          PushNum("2"),
          PushString("foo"),
          LoadLocal,
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

    "emit wrapped object as right side of Let" in {
      testEmit("clicks := //clicks {foo: clicks}")(
        Vector(
          PushString("foo"),
          PushString("/clicks"),
          LoadLocal,
          Map2Cross(WrapObject)))
    }    
    
    "emit matched join of wrapped object for object with two fields having same provenance" in {
      testEmit("clicks := //clicks {foo: clicks, bar: clicks}")(
        Vector(
          PushString("foo"),
          PushString("/clicks"),
          LoadLocal,
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

    "emit join of wrapped arrays for array with four elements having values from two static provenances" in {
      testEmit("foo := //foo bar := //bar foo ~ bar [foo.a, bar.a, foo.b, bar.b]")(
        Vector(
          PushString("/foo"),
          LoadLocal,
          Dup,
          PushString("a"),
          Map2Cross(DerefObject),
          Map1(WrapArray),
          Swap(1),
          PushString("b"),
          Map2Cross(DerefObject),
          Map1(WrapArray),
          Map2Match(JoinArray),
          PushString("/bar"),
          LoadLocal,
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
          Map2Cross(ArraySwap)))
    }

    "emit descent for object load" in {
      testEmit("clicks := //clicks clicks.foo")(
        Vector(
          PushString("/clicks"),
          LoadLocal,
          PushString("foo"),
          Map2Cross(DerefObject)))
    }

    "emit meta descent for object load" in {
      testEmit("clicks := //clicks clicks@foo")(
        Vector(
          PushString("/clicks"),
          LoadLocal,
          PushString("foo"),
          Map2Cross(DerefMetadata)))
    }

    "emit descent for array load" in {
      testEmit("clicks := //clicks clicks[1]")(
        Vector(
          PushString("/clicks"),
          LoadLocal,
          PushNum("1"),
          Map2Cross(DerefArray)))
    }

    "emit load of literal load" in {
      testEmit("""load("foo")""")(
        Vector(
          PushString("foo"),
          LoadLocal)
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
      testEmit("""//clicks where //clicks.foo = null""")(
        Vector(
          PushString("/clicks"),
          LoadLocal,
          PushString("/clicks"),
          LoadLocal,
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
          LoadLocal,
          Dup,
          Swap(1),
          Map2Match(DerefArray)))
    }

    "emit filter match for where loads from same provenance" in {
      testEmit("""foo := load("foo") foo where foo""")(
        Vector(
          PushString("foo"),
          LoadLocal,
          Dup,
          Swap(1),
          FilterMatch))
    }

    "emit filter match for loads from same provenance when performing equality filter" in {
      testEmit("foo := //foo foo where foo.id = 2")(
        Vector(
          PushString("/foo"),
          LoadLocal,
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
          LoadLocal,
          Dup,
          Swap(1),
          Map2Match(Add)))
    }

    "use dup bytecode non-locally" in {
      testEmit("""clicks := load("foo") two := 2 * clicks two + clicks""")(
        Vector(
          PushNum("2"),
          PushString("foo"),
          LoadLocal,
          Dup,
          Swap(2),
          Swap(1),
          Map2Cross(Mul),
          Swap(1),
          Map2Match(Add)))
    }

    "emit morphism1" in {
      forall(libMorphism1) { f =>
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
        testEmit("""%s(//foobar.baz)""".format(f.fqn))(
          Vector(
            PushString("/foobar"),
            LoadLocal,
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
        testEmit("""%s(//foo.time, //foo.timeZone)""".format(f.fqn))(
          Vector(
            PushString("/foo"),
            LoadLocal,
            PushString("time"),
            Map2Cross(DerefObject),
            PushString("/foo"),
            LoadLocal,
            PushString("timeZone"),
            Map2Cross(DerefObject),
            Map2Match(BuiltInFunction2Op(f))))
      }
    }
 
    "emit body of fully applied characteristic function" in {
      testEmit("clicks := //clicks clicksFor(userId) := clicks where clicks.userId = userId clicksFor(\"foo\")")(
        Vector(
          PushString("/clicks"),
          LoadLocal,
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
        |   //campaigns where //campaigns.ageRange = a & //campaigns.gender = b
        | fun([25,36],
          "female")""".stripMargin)(
        Vector(
          PushString("/campaigns"),
          LoadLocal,
          PushString("/campaigns"),
          LoadLocal,
          PushString("ageRange"),
          Map2Cross(DerefObject),
          PushNum("25"),
          Map1(WrapArray),
          PushNum("36"),
          Map1(WrapArray),
          Map2Cross(JoinArray),
          Map2Cross(Eq),
          PushString("/campaigns"),
          LoadLocal,
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
          LoadLocal,
          Dup,
          PushString("x"),
          Map2Cross(DerefObject),
          PushString("/a"),
          LoadLocal,
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
          LoadLocal,
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
          LoadLocal,
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
          LoadLocal,
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
          PushString("/bar"),
          LoadLocal,
          Dup,
          PushString("a"),
          Map2Cross(DerefObject),
          KeyPart(1),
          Swap(1),
          PushString("a"),
          Map2Cross(DerefObject),
          Group(0),
          PushString("/foo"),
          LoadLocal,
          Dup,
          Swap(2),
          Swap(1),
          PushString("a"),
          Map2Cross(DerefObject),
          KeyPart(1),
          Swap(1),
          Swap(2),
          Group(2),
          MergeBuckets(true),
          Split,
          PushGroup(2),
          Reduce(BuiltInReduction(Reduction(Vector(), "count", 0x2000))),
          Merge))
    }

    "emit merge_buckets & for trivial cf example with conjunction" in {
      testEmit("clicks := //clicks onDay := solve 'day clicks where clicks.day = 'day & clicks.din = 'day onDay")(
        Vector(
          PushString("/clicks"),
          LoadLocal,
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
          LoadLocal,
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
          LoadLocal,
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
          LoadLocal,
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
          LoadLocal,
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
          LoadLocal,
          Dup,
          PushString("a"),
          Map2Cross(DerefObject),
          KeyPart(1),
          Swap(1),
          Group(0),
          PushString("/impressions"),
          LoadLocal,
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
          LoadLocal,
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
          LoadLocal,
          Dup,
          PushString("day"),
          Map2Cross(DerefObject),
          KeyPart(1),
          Swap(1),
          Group(0),
          PushString("/impressions"),
          LoadLocal,
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
          LoadLocal,
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
        LoadLocal,
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
          PushString("/organizations"),
          LoadLocal,
          Dup,
          Dup,
          Dup,
          PushString("revenue"),
          Map2Cross(DerefObject),
          KeyPart(1),
          Swap(1),
          PushString("revenue"),
          Map2Cross(DerefObject),
          Group(0),
          Swap(1),
          PushString("campaign"),
          Map2Cross(DerefObject),
          KeyPart(3),
          Swap(1),
          Swap(2),
          PushString("campaign"),
          Map2Cross(DerefObject),
          Group(2),
          MergeBuckets(true),
          PushString("/campaigns"),
          LoadLocal,
          Dup,
          Swap(2),
          Swap(1),
          PushString("campaign"),
          Map2Cross(DerefObject),
          KeyPart(3),
          Swap(1),
          Swap(2),
          Group(4),
          MergeBuckets(true),
          Split,
          PushString("revenue"),
          PushKey(1),
          Map2Cross(WrapObject),
          PushString("num"),
          PushGroup(4),
          Reduce(BuiltInReduction(Reduction(Vector(), "count", 0x002000))),
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
          LoadLocal,
          Dup,
          PushString("time"),
          Map2Cross(DerefObject),
          KeyPart(1),
          Swap(1),
          PushString("time"),
          Map2Cross(DerefObject),
          Group(0),
          Split,
          PushString("jay"),
          PushString("/views"),
          LoadLocal,
          Dup,
          Swap(2),
          Swap(1),
          PushString("time"),
          Map2Cross(DerefObject),
          Swap(1),
          Swap(2),
          PushString("time"),
          Map2Cross(DerefObject),
          PushKey(1),
          Map2Cross(Gt),
          FilterMatch,
          Map2Cross(WrapObject),
          PushString("kay"),
          PushGroup(0),
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
            LoadLocal,
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
            LoadLocal,
            Dup,
            PushString("userId"),
            Map2Cross(DerefObject),
            KeyPart(1),
            Swap(1),
            Group(0),
            PushString("/impressions"),
            LoadLocal,
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
            PushString("impression"),
            Swap(1),
            PushGroup(3),
            Dup,
            Map2Match(Eq),
            FilterMatch,
            Map2Cross(WrapObject),
            PushString("nextConversion"),
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
            Dup,
            Swap(3),
            Swap(2),
            Swap(1),
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
            Swap(1),
            Swap(2),
            Swap(3),
            Swap(4),
            Swap(5),
            Swap(6),
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
            LoadLocal,
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
  }
  
  val exampleDir = new File("quirrel/examples")
  
  if (exampleDir.exists) {
    "specification examples" >> {
      val pending = Set("interaction-totals.qrl", "relative-durations.qrl")
      
      for (file <- exampleDir.listFiles if file.getName endsWith ".qrl") {
        if (pending contains file.getName) {
          file.getName >> {
            val result = compile(LineStream(Source.fromFile(file)))
            result.errors must beEmpty
            emit(result) must not(beEmpty)
          }.pendingUntilFixed
        } else {
          file.getName >> {
            val result = compile(LineStream(Source.fromFile(file)))
            result.errors must beEmpty
            emit(result) must not(beEmpty)
          }
        }
      }
    }
  } else {
    "specification examples" >> skipped
  }
}
