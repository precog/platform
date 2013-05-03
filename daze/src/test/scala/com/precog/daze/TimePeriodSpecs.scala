package com.precog.daze

import org.specs2.mutable._

import com.precog.yggdrasil._
import com.precog.common._
import scalaz._
import scalaz.std.list._

import com.precog.util.IdGen

import org.joda.time._
import org.joda.time.format._

trait TimePeriodSpecs[M[+_]] extends Specification
    with EvaluatorTestSupport[M]
    with LongIdMemoryDatasetConsumer[M] { self =>
      
  import Function._
  
  import dag._
  import instructions._
  import library._

  val testAPIKey = "testAPIKey"

  val line = Line(1, 1, "")

  def testEval(graph: DepGraph): Set[SEvent] = {
    consumeEval(testAPIKey, graph, Path.Root) match {
      case Success(results) => results
      case Failure(error) => throw error
    }
  }

  "Period parsing" should {
    "parse period with all fields present" in {
      val input = Operate(BuiltInFunction1Op(ParsePeriod),
        Const(CString("P3Y6M4DT12H30M5S"))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect { case (ids, period) if ids.size == 0 => period }
      result2 mustEqual(Set(SString("P3Y6M4DT12H30M5S")))
    }

    "parse period with not all fields present" in {
      val input = Operate(BuiltInFunction1Op(ParsePeriod),
        Const(CString("P6M4DT30M5S"))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect { case (ids, period) if ids.size == 0 => period }
      result2 mustEqual(Set(SString("P6M4DT30M5S")))
    }

    "fail to parse string not in proper format" in {
      val input = Operate(BuiltInFunction1Op(ParsePeriod),
        Const(CString("6M4DT30M5S"))(line))(line)

      val result = testEval(input)

      result must haveSize(0)
    }
  }

  def createObject(field: String, op1: Op1, value: String) = {
    Join(WrapObject, Cross(None),
      Const(CString(field))(line),
      Operate(BuiltInFunction1Op(op1),
        Const(CString(value))(line))(line))(line)
  }

  def joinObject(obj1: DepGraph, obj2: DepGraph, obj3: DepGraph) = {
    Join(JoinObject, Cross(None),
        Join(JoinObject, Cross(None),
          obj1,
          obj2)(line),
        obj3)(line)
  }

  "Range computing" should {
    "compute correct range given single value" in {
      val start = createObject("start", ParseDateTimeFuzzy, "1987-12-09T18:33:02.037Z")
      val end = createObject("end", ParseDateTimeFuzzy, "2005-08-09T12:00:00.000Z")
      val step = createObject("step", ParsePeriod, "P3Y6M4DT12H30M5S")

      val obj = joinObject(start, end, step)
      val input = Operate(BuiltInFunction1Op(TimeRange), obj)(line)

      val result = testEval(input)

      result mustEqual Set((
        Vector(),
        SArray(Vector(
          SString("1987-12-09T18:33:02.037Z"),
          SString("1991-06-14T07:03:07.037Z"),
          SString("1994-12-18T19:33:12.037Z"),
          SString("1998-06-23T08:03:17.037Z"),
          SString("2001-12-27T20:33:22.037Z"),
          SString("2005-07-02T09:03:27.037Z")))))
    }

    "compute correct range given end earlier than start" in {
      val start = createObject("start", ParseDateTimeFuzzy, "1987-12-09T18:33:02.037Z")
      val end = createObject("end", ParseDateTimeFuzzy, "1980-08-09T12:00:00.000Z")
      val step = createObject("step", ParsePeriod, "P3Y6M4DT12H30M5S")

      val obj = joinObject(start, end, step)

      val input = Operate(BuiltInFunction1Op(TimeRange), obj)(line)

      val result = testEval(input)

      result mustEqual Set((
        Vector(),
        SArray(Vector(
          SString("1987-12-09T18:33:02.037Z")))))
    }

    "compute correct range given different timezones" in {
      val start = createObject("start", ParseDateTimeFuzzy, "1987-12-09T18:33:02.037+01:00")
      val end = createObject("end", ParseDateTimeFuzzy, "1987-12-09T20:33:02.037+03:00")
      val step = createObject("step", ParsePeriod, "PT2H")

      val obj = joinObject(start, end, step)

      val input = Operate(BuiltInFunction1Op(TimeRange), obj)(line)

      val result = testEval(input)

      result mustEqual Set((
        Vector(),
        SArray(Vector(
          SString("1987-12-09T18:33:02.037+01:00")))))
    }

    "compute correct range given multiple values" in {
      val objects = dag.LoadLocal(Const(CString("/hom/timerange"))(line))(line)

      def deref(field: String) = {
        dag.Join(DerefObject, Cross(None),
          objects,
          Const(CString(field))(line))(line)
      }

      def createObject2(field: String, op1: Op1) = {
        Join(WrapObject, Cross(None),
          Const(CString(field))(line),
          Operate(BuiltInFunction1Op(op1),
            deref(field))(line))(line)
      }

      val start = createObject2("start", ParseDateTimeFuzzy)
      val end = createObject2("end", ParseDateTimeFuzzy)
      val step = createObject2("step", ParsePeriod)

      val obj = Join(JoinObject, IdentitySort,
        Join(JoinObject, IdentitySort,
          start,
          end)(line),
        step)(line)

      val range = Join(WrapObject, Cross(None),
        Const(CString("range"))(line),
        Operate(BuiltInFunction1Op(TimeRange), obj)(line))(line)

      val input = Join(JoinObject, IdentitySort,
        obj,
        range)(line)

      val result = testEval(input)

      result must haveSize(4)

      val expected: Map[SValue, SArray] = Map(
        SString("PT1H") -> SArray(Vector(
          SString("1991-06-14T07:03:07.037Z"),
          SString("1991-06-14T08:03:07.037Z"))),
        SString("P1Y") -> SArray(Vector(
          SString("1991-06-14T07:03:07.037Z"))),
        SString("PT5S") -> SArray(Vector(
          SString("1991-06-14T07:03:07.037Z"),
          SString("1991-06-14T07:03:12.037Z"))),
        SString("P2M") -> SArray(Vector(
          SString("1991-06-14T07:03:07.037Z"),
          SString("1991-08-14T07:03:07.037Z"),
          SString("1991-10-14T07:03:07.037Z"))))

      result must haveAllElementsLike {
        case (ids, obj) =>
          ids must haveSize(1)
          obj must beLike {
            case SObject(fields) =>
              fields.keys mustEqual Set("start", "end", "step", "range")
              expected(fields("step")) mustEqual fields("range")
          }
      }
    }

    "fail to compute correct range given only start field" in {
      val start = createObject("start", ParseDateTimeFuzzy, "1987-12-09T18:33:02.037Z")
      val input = Operate(BuiltInFunction1Op(TimeRange), start)(line)

      val result = testEval(input)

      result must beEmpty
    }

    "fail to compute correct range given malformed input" in {
      val input = Operate(BuiltInFunction1Op(TimeRange), Const(CString("foo"))(line))(line)

      val result = testEval(input)

      result must beEmpty
    }
  }
}

object TimePeriodSpecs extends TimePeriodSpecs[test.YId] with test.YIdInstances
