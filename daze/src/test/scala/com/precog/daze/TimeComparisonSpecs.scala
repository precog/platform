package com.precog.daze

import org.specs2.mutable._

import com.precog.common._
import com.precog.yggdrasil._
import com.precog.common.Path
import scalaz._
import scalaz.std.list._

import com.precog.util.IdGen

import org.joda.time._
import org.joda.time.format._

trait TimeComparisonSpecs[M[+_]] extends Specification
    with EvaluatorTestSupport[M]
    with LongIdMemoryDatasetConsumer[M] { self =>
      
  import Function._
  
  import dag._
  import instructions._
  import library._

  val testAPIKey = "testAPIKey"

  val line = Line(1, 1, "")

  def parseDateTime(time: String, fmt: String) = {
    Join(BuiltInFunction2Op(ParseDateTime), Cross(None),
      Const(CString(time))(line),
      Const(CString(fmt))(line))(line)
  }

  def parseDateTimeFuzzy(time: String) =
    Operate(BuiltInFunction1Op(ParseDateTimeFuzzy), Const(CString(time))(line))(line)

  def doNotParse(time: String) = Const(CString(time))(line)

  def basicComparison(input: DepGraph, expected: Boolean) = {
    val result = testEval(input)
    
    result must haveSize(1)
    
    result must haveAllElementsLike {
      case (ids, SBoolean(d)) =>
        ids must haveSize(0)
        d mustEqual(expected)
    }
  }

  def testEval(graph: DepGraph): Set[SEvent] = {
    consumeEval(testAPIKey, graph, Path.Root) match {
      case Success(results) => results
      case Failure(error) => throw error
    }
  }

  "comparison of two DateTimes of value provenance" should {
    "compute lt resulting in false" in {
      val input = Join(Lt, Cross(None),
        parseDateTime("Jun 3, 2020 3:12:33 AM", "MMM d, yyyy h:mm:ss a"),
        parseDateTime("Jul 8, 1999 3:19:33 PM", "MMM d, yyyy h:mm:ss a"))(line)

      basicComparison(input, false)
    }

    "compute lt resulting in true" in {
      val input = Join(Lt, Cross(None),
        parseDateTimeFuzzy("2010-06-03T04:12:33.323Z"),
        parseDateTimeFuzzy("2011-06-03T04:12:33.323Z"))(line)

      basicComparison(input, true)
    }

    "compute gt resulting in false" in {
      val input = Join(Gt, Cross(None),
        parseDateTime("Jun 3, 2020 3:12:33 AM", "MMM d, yyyy h:mm:ss a"),
        parseDateTime("Jun 3, 2020 3:12:33 AM", "MMM d, yyyy h:mm:ss a"))(line)

      basicComparison(input, false)
    }

    "compute gt resulting in true" in {
      val input = Join(Gt, Cross(None),
        parseDateTime("Jun 3, 2020 3:12:33 AM", "MMM d, yyyy h:mm:ss a"),
        parseDateTime("Jul 8, 1999 3:19:33 PM", "MMM d, yyyy h:mm:ss a"))(line)

      basicComparison(input, true)
    }

    "compute lteq resulting in false" in {
      val input = Join(LtEq, Cross(None),
        parseDateTime("Jun 3, 2020 3:12:33 AM", "MMM d, yyyy h:mm:ss a"),
        parseDateTime("Jul 8, 1999 3:19:33 PM", "MMM d, yyyy h:mm:ss a"))(line)

      basicComparison(input, false)
    }

    "compute lteq resulting in true" in {
      val input = Join(LtEq, Cross(None),
        parseDateTime("Jun 3, 2020 3:12:33 AM", "MMM d, yyyy h:mm:ss a"),
        parseDateTime("Jun 3, 2020 3:12:33 AM", "MMM d, yyyy h:mm:ss a"))(line)

      basicComparison(input, true)
    }

    "compute gteq resulting in false" in {
      val input = Join(GtEq, Cross(None),
        parseDateTime("Jul 8, 1999 3:19:33 PM", "MMM d, yyyy h:mm:ss a"),
        parseDateTime("Jun 3, 2020 3:12:33 AM", "MMM d, yyyy h:mm:ss a"))(line)

      basicComparison(input, false)
    }

    "compute gteq resulting in true" in {
      val input = Join(GtEq, Cross(None),
        parseDateTime("Jun 3, 2020 3:12:33 AM", "MMM d, yyyy h:mm:ss a"),
        parseDateTime("Jun 3, 2020 3:12:33 AM", "MMM d, yyyy h:mm:ss a"))(line)

      basicComparison(input, true)
    }

    "compute eq resulting in false" in {
      val input = Join(Eq, Cross(None),
        parseDateTime("Jul 8, 1999 3:19:33 PM", "MMM d, yyyy h:mm:ss a"),
        parseDateTime("Jun 3, 2020 3:12:33 AM", "MMM d, yyyy h:mm:ss a"))(line)

      basicComparison(input, false)
    }

    "compute eq resulting in true" in {
      val input = Join(Eq, Cross(None),
        parseDateTime("Jun 3, 2020 3:12:33 AM", "MMM d, yyyy h:mm:ss a"),
        parseDateTime("Jun 3, 2020 3:12:33 AM", "MMM d, yyyy h:mm:ss a"))(line)

      basicComparison(input, true)
    }

    "compute eq given equal times in different timezones" in {
      val input = Join(Eq, Cross(None),
        parseDateTimeFuzzy("2011-06-03T04:12:33.323+02:00"),
        parseDateTimeFuzzy("2011-06-03T02:12:33.323Z"))(line)

      basicComparison(input, true)
    }

    "compute eq given times `equivalent` except for timezones" in {
      val input = Join(Eq, Cross(None),
        parseDateTimeFuzzy("2011-06-03T04:12:33.323+02:00"),
        parseDateTimeFuzzy("2011-06-03T04:12:33.323Z"))(line)

      basicComparison(input, false)
    }

    "compute noteq resulting in false" in {
      val input = Join(NotEq, Cross(None),
        parseDateTime("Jun 3, 2020 3:12:33 AM", "MMM d, yyyy h:mm:ss a"),
        parseDateTime("Jun 3, 2020 3:12:33 AM", "MMM d, yyyy h:mm:ss a"))(line)

      basicComparison(input, false)
    }

    "compute noteq resulting in true" in {
      val input = Join(NotEq, Cross(None),
        parseDateTime("Jul 8, 1999 3:19:33 PM", "MMM d, yyyy h:mm:ss a"),
        parseDateTime("Jun 3, 2020 3:12:33 AM", "MMM d, yyyy h:mm:ss a"))(line)

      basicComparison(input, true)
    }
  }

  "comparision of two DateTimes input as CDate" should {
    "produce correct results using lt" in {
      DateTimeZone.setDefault(DateTimeZone.UTC)

      val input = Join(Lt, Cross(None),
        Const(CDate(new DateTime("2010-09-23T18:33:22.520")))(line),
        Const(CDate(new DateTime("2011-09-23T18:33:22.520")))(line))(line)

      basicComparison(input, true)
    }
  }

  def extremeComparison(input: DepGraph, expected: String) = {
    val result = testEval(input)
    
    result must haveSize(1)
    
    result must haveAllElementsLike {
      case (ids, SString(str)) =>
        ids must haveSize(0)
        str mustEqual(expected)
    }
  }

  "comparision of two DateTimes with minTime" should {
    val smallTime = "2010-09-23T18:33:22.520-10:00"
    val bigTime =   "2011-09-23T18:33:22.520-10:00"

    "produce correct results when lhs is smaller" in {
      val input = Join(BuiltInFunction2Op(MinTimeOf), Cross(None),
        parseDateTimeFuzzy(smallTime),
        parseDateTimeFuzzy(bigTime))(line)

      extremeComparison(input, smallTime)
    }

    "produce correct results when rhs is smaller" in {
      val input = Join(BuiltInFunction2Op(MinTimeOf), Cross(None),
        parseDateTimeFuzzy(bigTime),
        parseDateTimeFuzzy(smallTime))(line)

      extremeComparison(input, smallTime)
    }

    "produce correct results when times are equal" in {
      val input = Join(BuiltInFunction2Op(MinTimeOf), Cross(None),
        parseDateTimeFuzzy(bigTime),
        parseDateTimeFuzzy(bigTime))(line)

      extremeComparison(input, bigTime)
    }
  }

  "comparision of two DateTimes with maxTime" should {
    val smallTime = "2010-09-23T18:33:22.520-10:00"
    val bigTime =   "2011-09-23T18:33:22.520-10:00"

    "produce correct results when lhs is larger" in {
      val input = Join(BuiltInFunction2Op(MaxTimeOf), Cross(None),
        parseDateTimeFuzzy(bigTime),
        parseDateTimeFuzzy(smallTime))(line)

      extremeComparison(input, bigTime)
    }

    "produce correct results when rhs is larger" in {
      val input = Join(BuiltInFunction2Op(MaxTimeOf), Cross(None),
        parseDateTimeFuzzy(smallTime),
        parseDateTimeFuzzy(bigTime))(line)

      extremeComparison(input, bigTime)
    }

    "produce correct results when times are equal" in {
      val input = Join(BuiltInFunction2Op(MaxTimeOf), Cross(None),
        parseDateTimeFuzzy(bigTime),
        parseDateTimeFuzzy(bigTime))(line)

      extremeComparison(input, bigTime)
    }
  }

  "comparision of two DateTimes when timezones are different" should {
    val smallTime = "2011-09-23T18:33:22.520-07:00"
    val bigTime =   "2011-09-23T18:33:22.520-10:00"

    "for maxTime, both parsed" in {
      val input = Join(BuiltInFunction2Op(MaxTimeOf), Cross(None),
        parseDateTimeFuzzy(bigTime),
        parseDateTimeFuzzy(smallTime))(line)

      extremeComparison(input, bigTime)
    }

    "for maxTime, big parsed" in {
      val input = Join(BuiltInFunction2Op(MaxTimeOf), Cross(None),
        parseDateTimeFuzzy(smallTime),
        doNotParse(bigTime))(line)

      extremeComparison(input, bigTime)
    }

    "for minTime, small parsed" in {
      val input = Join(BuiltInFunction2Op(MinTimeOf), Cross(None),
        doNotParse(bigTime),
        parseDateTimeFuzzy(smallTime))(line)

      extremeComparison(input, smallTime)
    }

    "for minTime, neither parsed" in {
      val input = Join(BuiltInFunction2Op(MinTimeOf), Cross(None),
        doNotParse(smallTime),
        doNotParse(bigTime))(line)

      extremeComparison(input, smallTime)
    }
  }
}

object TimeComparisonSpecs extends TimeComparisonSpecs[test.YId] with test.YIdInstances
