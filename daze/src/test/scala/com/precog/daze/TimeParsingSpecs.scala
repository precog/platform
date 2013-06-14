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

trait TimeParsingSpecs[M[+_]] extends Specification
    with EvaluatorTestSupport[M]
    with LongIdMemoryDatasetConsumer[M] { self =>
      
  import Function._
  
  import dag._
  import instructions._
  import library._

  val line = Line(1, 1, "")

  def testEval(graph: DepGraph): Set[SEvent] = {
    consumeEval(graph, defaultEvaluationContext) match {
      case Success(results) => results
      case Failure(error) => throw error
    }
  }

  "parse a time string into an ISO801 string, given its format" should {
    "time zone not specified" in {
      val input = Join(BuiltInFunction2Op(ParseDateTime), Cross(None),
        Const(CString("Jun 3, 2020 3:12:33 AM"))(line),
        Const(CString("MMM d, yyyy h:mm:ss a"))(line))(line)
        
      val result = testEval(input) collect {
        case (ids, SString(d)) if ids.length == 0 => d.toString
      }

      result must haveSize(1)

      result must contain("2020-06-03T03:12:33.000Z")
    }

    "time zone specified" in {
      val input = Join(BuiltInFunction2Op(ParseDateTime), Cross(None),
        Const(CString("Jun 3, 2020 3:12:33 AM -08:00"))(line),
        Const(CString("MMM d, yyyy h:mm:ss a Z"))(line))(line)
        
      val result = testEval(input) collect {
        case (ids, SString(d)) if ids.length == 0 => d.toString
      }

      result must haveSize(1)

      result must contain("2020-06-03T03:12:33.000-08:00")
    }

    "malformed string" in {
      val input = Join(BuiltInFunction2Op(ParseDateTime), Cross(None),
        Const(CString("Jun 3, 2020 3:12:33 AM -08:00 asteroid"))(line),
        Const(CString("MMM d, yyyy h:mm:ss a Z"))(line))(line)
        
      val result = testEval(input) collect {
        case (ids, SString(d)) if ids.length == 0 => d.toString
      }
      
      result must beEmpty
    }

    "results used in another time function from homogeneous set" in {
      val input = dag.Operate(BuiltInFunction1Op(Date),
        Join(BuiltInFunction2Op(ParseDateTime), Cross(None),
          dag.AbsoluteLoad(Const(CString("/hom/timeString"))(line))(line),
          Const(CString("MMM dd yyyy k:mm:ss.SSS"))(line))(line))(line)
        
      val result = testEval(input) collect {
        case (ids, SString(d)) if ids.length == 1 => d.toString
      }

      result must haveSize(4)

      result must contain("2010-06-03", "2010-06-04", "2011-08-12", "2010-10-09")
    }

    "from heterogeneous set" in {
      val input = Join(BuiltInFunction2Op(ParseDateTime), Cross(None),
          dag.AbsoluteLoad(Const(CString("/het/timeString"))(line))(line),
          Const(CString("MMM dd yyyy k:mm:ss.SSS"))(line))(line)
        
      val result = testEval(input) collect {
        case (ids, SString(d)) if ids.length == 1 => d.toString
      }

      result must haveSize(4)

      result must contain("2010-06-03T04:12:33.323Z", "2010-06-04T13:31:49.002Z", "2011-08-12T22:42:33.310Z", "2010-10-09T09:27:31.953Z")
    }

    "ChangeTimeZone function with not fully formed string without tz" in {
      val input = Join(BuiltInFunction2Op(ChangeTimeZone), Cross(None),
          Const(CString("2010-06-04"))(line),
          Const(CString("-10:00"))(line))(line)
        
      val result = testEval(input) collect {
        case (ids, SString(d)) if ids.length == 0 => d.toString
      }

      result must haveSize(1)

      result must contain("2010-06-03T14:00:00.000-10:00")
    }

    "ChangeTimeZone function with not fully formed string with tz" in {
      val input = Join(BuiltInFunction2Op(ChangeTimeZone), Cross(None),
          Const(CString("2010-06-04T+05:00"))(line),
          Const(CString("-10:00"))(line))(line)
        
      val result = testEval(input) collect {
        case (ids, SString(d)) if ids.length == 0 => d.toString
      }

      result must haveSize(1)

      result must contain("2010-06-03T09:00:00.000-10:00")
    }

    "Plus function with not fully formed string without tz" in {
      val input = Join(BuiltInFunction2Op(MinutesPlus), Cross(None),
          Const(CString("2010-06-04T05:04:01"))(line),
          Const(CLong(10))(line))(line)
        
      val result = testEval(input) collect {
        case (ids, SString(d)) if ids.length == 0 => d.toString
      }

      result must haveSize(1)

      result must contain("2010-06-04T05:14:01.000Z")
    }

    "Plus function with not fully formed string with tz" in {
      val input = Join(BuiltInFunction2Op(MinutesPlus), Cross(None),
          Const(CString("2010-06-04T05:04:01.000+05:00"))(line),
          Const(CLong(10))(line))(line)
        
      val result = testEval(input) collect {
        case (ids, SString(d)) if ids.length == 0 => d.toString
      }

      result must haveSize(1)

      result must contain("2010-06-04T05:14:01.000+05:00")
    }

    "Plus function with space instead of T" in {
      val input = Join(BuiltInFunction2Op(MinutesPlus), Cross(None),
          Const(CString("2010-06-04 05:04:01"))(line),
          Const(CLong(10))(line))(line)
        
      val result = testEval(input) collect {
        case (ids, SString(d)) if ids.length == 0 => d.toString
      }

      result must haveSize(1)

      result must contain("2010-06-04T05:14:01.000Z")
    }
    "Between function with not fully formed string without tz" in {
      val input = Join(BuiltInFunction2Op(HoursBetween), Cross(None),
          Const(CString("2010-06-04T05:04:01"))(line),
          Const(CString("2010-06-04T07:04:01+00:00"))(line))(line)
        
      val result = testEval(input) collect {
        case (ids, SDecimal(d)) if ids.length == 0 => d.toInt
      }

      result must haveSize(1)

      result must contain(2)
    }

    "Between function with not fully formed string with tz" in {
      val input = Join(BuiltInFunction2Op(HoursBetween), Cross(None),
          Const(CString("2010-06-04T05:04:01+05:00"))(line),
          Const(CString("2010-06-04T05:04:01+01:00"))(line))(line)
        
      val result = testEval(input) collect {
        case (ids, SDecimal(d)) if ids.length == 0 => d.toInt
      }

      result must haveSize(1)

      result must contain(4)
    }

    "GetMillis function with not fully formed string without tz" in {
      val input = Operate(BuiltInFunction1Op(GetMillis),
          Const(CString("2010-06-04T05"))(line))(line)
        
      val result = testEval(input) collect {
        case (ids, SDecimal(d)) if ids.length == 0 => d.toLong
      }

      result must haveSize(1)

      result must contain(1275627600000L)
    }

    "GetMillis function with not fully formed string with tz" in {
      val input = Operate(BuiltInFunction1Op(GetMillis),
          Const(CString("2010-06-04T03-02:00"))(line))(line)
        
      val result = testEval(input) collect {
        case (ids, SDecimal(d)) if ids.length == 0 => d.toLong
      }

      result must haveSize(1)

      result must contain(1275627600000L)
    }

    "TimeZone function with not fully formed string without tz" in {
      val input = Operate(BuiltInFunction1Op(TimeZone),
          Const(CString("2010-06-04T05"))(line))(line)
        
      val result = testEval(input) collect {
        case (ids, SString(d)) if ids.length == 0 => d.toString
      }

      result must haveSize(1)

      result must contain("+00:00")
    }

    "TimeZone function with not fully formed string with tz" in {
      val input = Operate(BuiltInFunction1Op(TimeZone),
          Const(CString("2010-06-04T03-02:00"))(line))(line)
        
      val result = testEval(input) collect {
        case (ids, SString(d)) if ids.length == 0 => d.toString
      }

      result must haveSize(1)

      result must contain("-02:00")
    }

    "Season function with not fully formed string without tz" in {
      val input = Operate(BuiltInFunction1Op(Season),
          Const(CString("2010-01-04"))(line))(line)
        
      val result = testEval(input) collect {
        case (ids, SString(d)) if ids.length == 0 => d.toString
      }

      result must haveSize(1)

      result must contain("winter")
    }

    "Season function with not fully formed string with tz" in {
      val input = Operate(BuiltInFunction1Op(Season),
          Const(CString("2010-01-04T-02:00"))(line))(line)
        
      val result = testEval(input) collect {
        case (ids, SString(d)) if ids.length == 0 => d.toString
      }

      result must haveSize(1)

      result must contain("winter")
    }

    "TimeFraction function with not fully formed string without tz" in {
      val input = Operate(BuiltInFunction1Op(HourOfDay),
          Const(CString("2010-01-04"))(line))(line)
        
      val result = testEval(input) collect {
        case (ids, SDecimal(d)) if ids.length == 0 => d.toInt
      }

      result must haveSize(1)

      result must contain(0)
    }

    "TimeFraction function with not fully formed string with tz" in {
      val input = Operate(BuiltInFunction1Op(HourOfDay),
        Const(CString("2010-01-04T03-02:00"))(line))(line)
        
      val result = testEval(input) collect {
        case (ids, SDecimal(d)) if ids.length == 0 => d.toInt
      }

      result must haveSize(1)

      result must contain(3)
    }
  }

  "\"flexible\" parsing" should {
    def testParseFuzzy(s: String, r: String) {
      val input = Operate(BuiltInFunction1Op(ParseDateTimeFuzzy), Const(CString(s))(line))(line)
      val result = testEval(input) collect {
        case (ids, SString(d)) if ids.length == 0 => d.toString
      }
      result must haveSize(1)
      result must contain(r)
    }

    "correctly handle a bunch of casually formatted dates" in {
      testParseFuzzy("1907/12/3 16:12:34", "1907-12-03T16:12:34.000Z")
      testParseFuzzy("1987/12/3 4pm", "1987-12-03T16:00:00.000Z")
      testParseFuzzy("1987/12/3 4:00 pm", "1987-12-03T16:00:00.000Z")
      testParseFuzzy("1987/12/3 4 PM", "1987-12-03T16:00:00.000Z")
      testParseFuzzy("1987/12/3 at 4 PM", "1987-12-03T16:00:00.000Z")
      testParseFuzzy("1987-12-3 16:00", "1987-12-03T16:00:00.000Z")
      testParseFuzzy("1987/12/3", "1987-12-03T12:00:00.000Z")
      testParseFuzzy("12/3/1987", "1987-12-03T12:00:00.000Z")

      // make sure we can handle ISO8601
      testParseFuzzy("2011-09-08T13:13:13", "2011-09-08T13:13:13.000Z")
      testParseFuzzy("2011-09-08 13:13:13", "2011-09-08T13:13:13.000Z")

      // yikes!!
      testParseFuzzy("12/3/87", "1987-12-03T12:00:00.000Z")
      testParseFuzzy("87/12/3", "1987-12-03T12:00:00.000Z")
      testParseFuzzy("12/9/3", "2003-12-09T12:00:00.000Z")
      testParseFuzzy("12/9/03", "2003-12-09T12:00:00.000Z")
    }
  }
}

object TimeParsingSpecs extends TimeParsingSpecs[test.YId] with test.YIdInstances
