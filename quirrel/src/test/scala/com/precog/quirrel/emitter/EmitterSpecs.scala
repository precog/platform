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
            FilterCross(0,
          None)))
      }

      "which are null and string" >> {
        testEmit("""null where "foo" """)(
          Vector(
            PushNull,
            PushString("foo"),
            FilterCross(0,
          None)))
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
          LoadLocal(Het),
          PushString("bar"),
          LoadLocal(Het),
          IUnion))
    }    
    
    "emit instruction for two intersected loads" in {
      testEmit("""load("foo") intersect load("bar")""")(
        Vector(
          PushString("foo"),
          LoadLocal(Het),
          PushString("bar"),
          LoadLocal(Het),
          IIntersect))
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
          LoadLocal(Het),
          PushNum("2"),
          Map2Cross(Mul)))
    }

    "emit line information for cross for division of load in static provenance with load in value provenance" in {
      testEmitLine("load(\"foo\") * 2")(
        Vector(
          Line(1,"load(\"foo\") * 2"),
          PushString("foo"),
          LoadLocal(Het),
          PushNum("2"),
          Map2Cross(Mul)))
    }

    "emit cross for division of load in static provenance with load in value provenance" in {
      testEmit("2 * load(\"foo\")")(
        Vector(
          PushNum("2"),
          PushString("foo"),
          LoadLocal(Het),
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

    "emit matched join of wrapped object for object with two fields having same provenance" in {
      testEmit("clicks := //clicks {foo: clicks, bar: clicks}")(
        Vector(
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
          Map2Match(JoinObject)))
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
          LoadLocal(Het),
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
          LoadLocal(Het),
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
          LoadLocal(Het),
          PushString("foo"),
          Map2Cross(DerefObject)))
    }

    "emit descent for array load" in {
      testEmit("clicks := //clicks clicks[1]")(
        Vector(
          PushString("/clicks"),
          LoadLocal(Het),
          PushNum("1"),
          Map2Cross(DerefArray)))
    }

    "emit load of literal load" in {
      testEmit("""load("foo")""")(
        Vector(
          PushString("foo"),
          LoadLocal(Het))
      )
    }

    "emit filter cross for where loads from value provenance" in {
      testEmit("""1 where true""")(
        Vector(
          PushNum("1"),
          PushTrue,
          FilterCross(0,
          None)))
    }

    "emit filter cross for where loads from value provenance" in {
      testEmit("""//clicks where //clicks.foo = null""")(
        Vector(
          PushString("/clicks"),
          LoadLocal(Het),
          PushString("/clicks"),
          LoadLocal(Het),
          PushString("foo"),
          Map2Cross(DerefObject),
          PushNull,
          Map2Cross(Eq),
          FilterMatch(0,
          None)))
    }

    "emit descent for array load with non-constant indices" in {
      testEmit("clicks := //clicks clicks[clicks]")(
        Vector(
          PushString("/clicks"),
          LoadLocal(Het),
          Dup,
          Swap(1),
          Map2Match(DerefArray)))
    }

    "emit filter match for where loads from same provenance" in {
      testEmit("""foo := load("foo") foo where foo""")(
        Vector(
          PushString("foo"),
          LoadLocal(Het),
          Dup,
          Swap(1),
          FilterMatch(0,
          None)))
    }

    "emit filter match for loads from same provenance when performing equality filter" in {
      testEmit("foo := //foo foo where foo.id = 2")(
        Vector(
          PushString("/foo"),
          LoadLocal(Het),
          Dup,
          Swap(1),
          PushString("id"),
          Map2Cross(DerefObject),
          PushNum("2"),
          Map2Cross(Eq),
          FilterMatch(0,
          None)))
    }

    "use dup bytecode to duplicate the same load" in {
      testEmit("""clicks := load("foo") clicks + clicks""")(
        Vector(
          PushString("foo"),
          LoadLocal(Het),
          Dup,
          Swap(1),
          Map2Match(Add)))
    }

    "use dup bytecode non-locally" in {
      testEmit("""clicks := load("foo") two := 2 * clicks two + clicks""")(
        Vector(
          PushNum("2"),
          PushString("foo"),
          LoadLocal(Het),
          Dup,
          Swap(2),
          Swap(1),
          Map2Cross(Mul),
          Swap(1),
          Map2Match(Add)))
    }

    "emit count reduction" in {
      testEmit("count(1)")(
        Vector(
          PushNum("1"),
          Reduce(Count)))
    }

    "emit geometricMean reduction" in {
      testEmit("geometricMean(1)")(
        Vector(
          PushNum("1"),
          Reduce(GeometricMean)))
    }

    "emit mean reduction" in {
      testEmit("mean(1)")(
        Vector(
          PushNum("1"),
          Reduce(Mean)))
    }

    "emit median reduction" in {
      testEmit("median(1)")(
        Vector(
          PushNum("1"),
          Reduce(Median)))
    }

    "emit mode reduction" in {
      testEmit("mode(1)")(
        Vector(
          PushNum("1"),
          Reduce(Mode)))
    }

    "emit max reduction" in {
      testEmit("max(1)")(
        Vector(
          PushNum("1"),
          Reduce(Max)))
    }

    "emit min reduction" in {
      testEmit("min(1)")(
        Vector(
          PushNum("1"),
          Reduce(Min)))
    }

    "emit stdDev reduction" in {
      testEmit("stdDev(1)")(
        Vector(
          PushNum("1"),
          Reduce(StdDev)))
    }

    "emit sum reduction" in {
      testEmit("sum(1)")(
        Vector(
          PushNum("1"),
          Reduce(Sum)))
    } 

    "emit sumSq reduction" in {
      testEmit("sumSq(1)")(
        Vector(
          PushNum("1"),
          Reduce(SumSq)))
    }

    "emit variance reduction" in {
      testEmit("variance(1)")(
        Vector(
          PushNum("1"),
          Reduce(Variance)))
    }

    "emit distinct set-reduction" in {
      testEmit("distinct(1)")(
        Vector(
          PushNum("1"),
          SetReduce(Distinct)))
    } 
    
    "emit unary non-reduction" in {
      val f = lib1.head
      testEmit("""%s::%s("2012-02-29T00:44:52.599+08:00")""".format(f.namespace.mkString("::"), f.name))(
        Vector(
          PushString("2012-02-29T00:44:52.599+08:00"),
          Map1(BuiltInFunction1Op(f))))
    }    

    "emit binary non-reduction" in {
      val f = lib2.head
      testEmit("""%s::%s(//foo.time, //foo.timeZone)""".format(f.namespace.mkString("::"), f.name))(
        Vector(
          PushString("/foo"),
          LoadLocal(Het),
          PushString("time"),
          Map2Cross(DerefObject),
          PushString("/foo"),
          LoadLocal(Het),
          PushString("timeZone"),
          Map2Cross(DerefObject),
          Map2Match(BuiltInFunction2Op(f))))
    }

    "emit body of fully applied characteristic function" in {
      testEmit("clicks := //clicks clicksFor('userId) := clicks where clicks.userId = 'userId clicksFor(\"foo\")")(
        Vector(
          PushString("/clicks"),
          LoadLocal(Het),
          Dup,
          Swap(1),
          PushString("userId"),
          Map2Cross(DerefObject),
          PushString("foo"),
          Map2Cross(Eq),
          FilterMatch(0,
          None)))
    }
    
    "emit body of a fully applied characteristic function with two variables" in {
      testEmit("""
        | fun('a, 'b) := 
        |   //campaigns where //campaigns.ageRange = 'a & //campaigns.gender = 'b
        | fun([25,36],
          "female")""".stripMargin)(
        Vector(
          PushString("/campaigns"),
          LoadLocal(Het),
          PushString("/campaigns"),
          LoadLocal(Het),
          PushString("ageRange"),
          Map2Cross(DerefObject),
          PushNum("25"),
          Map1(WrapArray),
          PushNum("36"),
          Map1(WrapArray),
          Map2Cross(JoinArray),
          Map2Cross(Eq),
          PushString("/campaigns"),
          LoadLocal(Het),
          PushString("gender"),
          Map2Cross(DerefObject),
          PushString("female"),
          Map2Cross(Eq),
          Map2Match(And),
          FilterMatch(0,
          None)))
    }

    "emit match for first-level union provenance" in {
      testEmit("a := //a b := //b a ~ b (b.x - a.x) * (a.y - b.y)")(
        Vector(
          PushString("/b"),
          LoadLocal(Het),
          Dup,
          PushString("x"),
          Map2Cross(DerefObject),
          PushString("/a"),
          LoadLocal(Het),
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
          LoadLocal(Het),
          Dup,
          Dup,
          Dup,
          Swap(1),
          Dup,
          Map2Match(Eq),
          FilterMatch(0,
          None),
          PushString("x"),
          Map2Cross(DerefObject),
          PushString("/a"),
          LoadLocal(Het),
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
          FilterMatch(0,
          None),
          PushString("x"),
          Map2Cross(DerefObject),
          Map2Cross(Sub),
          Swap(1),
          Swap(1),
          Swap(2),
          Dup,
          Map2Match(Eq),
          FilterMatch(0,
          None),
          PushString("y"),
          Map2Cross(DerefObject),
          Swap(1),
          Swap(2),
          Swap(1),
          Swap(2),
          Swap(3),
          Dup,
          Map2Match(Eq),
          FilterMatch(0,
          None),
          PushString("y"),
          Map2Cross(DerefObject),
          Map2Cross(Sub),
          Map2Match(Mul)))
    }

    "emit split and merge for trivial cf example" in {
      testEmit("clicks := //clicks onDay('day) := clicks where clicks.day = 'day onDay")(
        Vector(
          PushString("/clicks"),
          LoadLocal(Het),
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

    "emit merge_buckets & for trivial cf example with conjunction" in {
      testEmit("clicks := //clicks onDay('day) := clicks where clicks.day = 'day & clicks.din = 'day onDay")(
        Vector(
          PushString("/clicks"),
          LoadLocal(Het),
          Dup,
          Dup,
          PushString("day"),
          Map2Cross(DerefObject),
          KeyPart(1),
          Swap(1),
          PushString("din"),
          Map2Cross(DerefObject),
          KeyPart(2),
          MergeBuckets(true),
          Swap(1),
          Group(0),
          Split,
          PushGroup(0),
          Merge))
    }

    "emit merge_buckets | for trivial cf example with disjunction" in {
      testEmit("clicks := //clicks onDay('day) := clicks where clicks.day = 'day | clicks.din = 'day onDay")(
        Vector(
          PushString("/clicks"),
          LoadLocal(Het),
          Dup,
          Dup,
          PushString("day"),
          Map2Cross(DerefObject),
          KeyPart(1),
          Swap(1),
          PushString("din"),
          Map2Cross(DerefObject),
          KeyPart(2),
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
        | foo('a, 'b) :=
        |   clicks' := clicks where clicks.time = 'a & clicks.pageId = 'b
        |   clicks'
        | foo""")(
        Vector(
          PushString("/clicks"),
          LoadLocal(Het),
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
        | hist('revenue, 'campaign) :=
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
        | foo('a) :=
        |   bar := clicks where clicks.a = 'a
        |   baz := clicks where clicks.b = 'a
        |
        |   bar.a + baz.b
        |
        | foo""".stripMargin)(
        Vector(
          PushString("/clicks"),
          LoadLocal(Het),
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
    
    "emit split and merge for cf example with independent tic variables on same set" in {
      testEmit("""
        | clicks := //clicks
        | 
        | foo('a, 'b) :=
        |   bar := clicks where clicks.a = 'a
        |   baz := clicks where clicks.b = 'b
        |
        |   bar.a + baz.b
        |
        | foo""".stripMargin)(
        Vector(
          PushString("/clicks"),
          LoadLocal(Het),
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
        | foo('a, 'b) :=
        |   bar := clicks where clicks.a = 'a
        |   baz := imps where imps.b = 'b
        |
        |   bar ~ baz
        |     bar.a + baz.b
        |
        | foo""".stripMargin)(
        Vector(
          PushString("/clicks"),
          LoadLocal(Het),
          Dup,
          PushString("a"),
          Map2Cross(DerefObject),
          KeyPart(1),
          Swap(1),
          Group(0),
          PushString("/impressions"),
          LoadLocal(Het),
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
        | foo('a) := clicks where clicks = 'a & clicks.b = 42
        | foo""".stripMargin)(
        Vector(
          PushString("/clicks"),
          LoadLocal(Het),
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

    "emit split and merge for ctr example" in {
      testEmit("""
        | clicks := //clicks
        | imps := //impressions
        | ctr('day) :=
        |   count(clicks where clicks.day = 'day) / count(imps where imps.day = 'day)
        | ctr""".stripMargin)(
        Vector(
          PushString("/clicks"),
          LoadLocal(Het),
          Dup,
          PushString("day"),
          Map2Cross(DerefObject),
          KeyPart(1),
          Swap(1),
          Group(0),
          PushString("/impressions"),
          LoadLocal(Het),
          Dup,
          Swap(2),
          Swap(1),
          PushString("day"),
          Map2Cross(DerefObject),
          KeyPart(3),
          Swap(1),
          Swap(2),
          Group(2),
          MergeBuckets(false),
          Split,
          PushGroup(0),
          Reduce(Count),
          PushGroup(2),
          Reduce(Count),
          Map2Cross(Div),
          Merge))
    }
    
    "emit dup for merge results" in {
      val input = """
        | clicks := //clicks
        | f('c) := count(clicks where clicks = 'c)
        | f.a + f.b""".stripMargin
        
      testEmit(input)(
        Vector(
          PushString("/clicks"),
          LoadLocal(Het),
          Dup,
          KeyPart(1),
          Swap(1),
          Group(0),
          Split,
          PushGroup(0),
          Reduce(Count),
          Merge,
          Dup,
          PushString("a"),
          Map2Cross(DerefObject),
          Swap(1),
          PushString("b"),
          Map2Cross(DerefObject),
          Map2Match(Add)))
    }
    
    "emit code for examples" in {
      "deviant-durations.qrl" >> {
        testEmit("""
          | interactions := //interactions
          | 
          | big1z('userId) :=
          |   userInteractions := interactions where interactions.userId = 'userId
          |   
          |   m := mean(userInteractions.duration)
          |   sd := stdDev(userInteractions.duration)
          | 
          |   {
          |     userId: 'userId,
          |     interaction: userInteractions where userInteractions.duration > m + (sd * 3)
          |   }
          |   
          | big1z
          """)(
          Vector(
            PushString("/interactions"),
            LoadLocal(Het),
            Dup,
            Dup,
            Dup,
            PushString("userId"),
            Map2Cross(DerefObject),
            KeyPart(1),
            Swap(1),
            Group(0),
            Swap(1),
            PushString("userId"),
            Map2Cross(DerefObject),
            KeyPart(3),
            Swap(1),
            Swap(2),
            Group(2),
            MergeBuckets(false),
            Split,
            PushString("userId"),
            PushKey(3),
            Map2Cross(WrapObject),
            PushString("interaction"),
            PushGroup(2),
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
            Reduce(Mean),
            Swap(1),
            Swap(2),
            Swap(3),
            Swap(4),
            Swap(5),
            PushString("duration"),
            Map2Cross(DerefObject),
            Reduce(StdDev),
            PushNum("3"),
            Map2Cross(Mul),
            Map2Cross(Add),
            Map2Cross(Gt),
            FilterMatch(0, None),
            Map2Cross(WrapObject),
            Map2Cross(JoinObject),
            Merge))
      }
      
      "first-conversion.qrl" >> {
        testEmit("""
          | firstConversionAfterEachImpression('userId) :=
          |   conversions' := //conversions
          |   impressions' := //impressions
          | 
          |   conversions := conversions' where conversions'.userId = 'userId
          |   impressions := impressions' where impressions'.userId = 'userId
          | 
          |   greaterConversions('time) :=
          |     impressionTimes := impressions.time where impressions.time = 'time
          |     conversionTimes :=
          |       conversions.time where conversions.time = min(conversions where conversions.time > 'time)
          |     
          |     conversionTimes ~ impressionTimes
          |       { impression: impressions, nextConversion: conversions }
          | 
          |   greaterConversions
          | 
          | firstConversionAfterEachImpression
          """)(
          Vector(
            PushString("/conversions"),
            LoadLocal(Het),
            Dup,
            Dup,
            Dup,
            PushString("userId"),
            Map2Cross(DerefObject),
            KeyPart(1),
            Swap(1),
            Group(0),
            PushString("/impressions"),
            LoadLocal(Het),
            Dup,
            Swap(2),
            Swap(1),
            PushString("userId"),
            Map2Cross(DerefObject),
            KeyPart(3),
            Swap(1),
            Swap(2),
            Group(2),
            MergeBuckets(true),
            Swap(1),
            PushString("userId"),
            Map2Cross(DerefObject),
            KeyPart(5),
            Swap(1),
            Swap(2),
            Group(4),
            MergeBuckets(false),
            Split,
            PushGroup(2),
            Dup,
            Dup,
            PushString("time"),
            Map2Cross(DerefObject),
            KeyPart(7),
            Swap(1),
            PushString("time"),
            Map2Cross(DerefObject),
            Group(6),
            Split,
            PushString("impression"),
            Swap(1),
            PushGroup(6),
            Dup,
            Map2Match(Eq),
            FilterMatch(0, None),
            Map2Cross(WrapObject),
            PushString("nextConversion"),
            PushGroup(4),
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
            Swap(1),
            Swap(2),
            Swap(3),
            Swap(4),
            Swap(5),
            Swap(6),
            PushString("time"),
            Map2Cross(DerefObject),
            PushKey(7),
            Map2Cross(Gt),
            FilterMatch(0, None),
            Reduce(Min),
            Map2Cross(Eq),
            FilterMatch(0, None),
            Dup,
            Map2Match(Eq),
            FilterMatch(0, None),
            Map2Cross(WrapObject),
            Map2Cross(JoinObject),
            Merge,
            Merge))
      }

      "histogram.qrl" >> {
        testEmit("""
          | clicks := //clicks
          | 
          | histogram('value) :=
          |   { cnt: count(clicks where clicks = 'value), value: 'value }
          |   
          | histogram
          """.stripMargin)(
          Vector(
            PushString("/clicks"),
            LoadLocal(Het),
            Dup,
            KeyPart(1),
            Swap(1),
            Group(0),
            Split,
            PushString("cnt"),
            PushGroup(0),
            Reduce(Count),
            Map2Cross(WrapObject),
            PushString("value"),
            PushKey(1),
            Map2Cross(WrapObject),
            Map2Cross(JoinObject),
            Merge))
      }
      
      /*
      "interaction-totals.qrl" >> {
        val input = """
          | interactions := //interactions
          | 
          | hourOfDay('time) := 'time / 3600000           -- timezones,
          anyone?
          | dayOfWeek('time) := 'time / 604800000         -- not even slightly correct
          | 
          | total('hour,
          'day) :=
          |   dayAndHour := dayOfWeek(interactions.time) = 'day & hourOfDay(interactions.time) = 'hour
          |   sum(interactions where dayAndHour)
          |   
          | total
          """.stripMargin
        
        parse(input) must not(throwA[ParseException])
      }
      
      "relative-durations.qrl" >> {
        val input = """
          | interactions := //interactions
          | 
          | relativeDurations('userId,
          'value) :=
          |   userInteractions := interactions where interactions.userId = 'userId
          |   interactionDurations := (userInteractions where userInteractions = 'value).duration
          |   totalDurations := sum(userInteractions.duration)
          | 
          |   { userId: 'userId,
          ratio: interactionDurations / totalDurations }
          | 
          | relativeDurations
          """.stripMargin
        
        parse(input) must not(throwA[ParseException])
      }*/
    }
  }
  
  val exampleDir = new File("quirrel/examples")
  
  if (exampleDir.exists) {
    "specification examples" >> {
      val pending = Set("interaction-totals.qrl", "relative-durations.qrl", "sessionize.qrl")
      
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
