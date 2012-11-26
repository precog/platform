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
package com.precog.daze

import org.specs2.mutable._

import com.precog.yggdrasil._
import com.precog.common.Path
import scalaz._
import scalaz.std.list._

import com.precog.util.IdGen

import org.joda.time._
import org.joda.time.format._

trait TimeLibSpec[M[+_]] extends Specification
    with EvaluatorTestSupport[M]
    with TimeLib[M] 
    with MemoryDatasetConsumer[M] { self =>
      
  import Function._
  
  import dag._
  import instructions._

  val testAPIKey = "testAPIKey"

  def testEval(graph: DepGraph): Set[SEvent] = withContext { ctx =>
    consumeEval(testAPIKey, graph, ctx,Path.Root) match {
      case Success(results) => results
      case Failure(error) => throw error
    }
  }

  "parse a time string into an ISO801 string, given its format" should {
    "time zone not specified" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(ParseDateTime), CrossLeftSort,
        Root(line, CString("Jun 3, 2020 3:12:33 AM")),
        Root(line, CString("MMM d, yyyy h:mm:ss a")))
        
      val result = testEval(input) collect {
        case (ids, SString(d)) if ids.length == 0 => d.toString
      }

      result must haveSize(1)

      result must contain("2020-06-03T03:12:33.000Z")
    }

    "time zone specified" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(ParseDateTime), CrossLeftSort,
        Root(line, CString("Jun 3, 2020 3:12:33 AM -08:00")),
        Root(line, CString("MMM d, yyyy h:mm:ss a Z")))
        
      val result = testEval(input) collect {
        case (ids, SString(d)) if ids.length == 0 => d.toString
      }

      result must haveSize(1)

      result must contain("2020-06-03T03:12:33.000-08:00")
    }

    "malformed string" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(ParseDateTime), CrossLeftSort,
        Root(line, CString("Jun 3, 2020 3:12:33 AM -08:00 asteroid")),
        Root(line, CString("MMM d, yyyy h:mm:ss a Z")))
        
      val result = testEval(input) collect {
        case (ids, SString(d)) if ids.length == 0 => d.toString
      }
      
      result must beEmpty
    }

    "results used in another time function from homogeneous set" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(Date),
        Join(line, BuiltInFunction2Op(ParseDateTime), CrossLeftSort,
          dag.LoadLocal(line, Root(line, CString("/hom/timeString"))),
          Root(line, CString("MMM dd yyyy k:mm:ss.SSS"))))
        
      val result = testEval(input) collect {
        case (ids, SString(d)) if ids.length == 1 => d.toString
      }

      result must haveSize(4)

      result must contain("2010-06-03", "2010-06-04", "2011-08-12", "2010-10-09")
    }

    "from heterogeneous set" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(ParseDateTime), CrossLeftSort,
          dag.LoadLocal(line, Root(line, CString("/het/timeString"))),
          Root(line, CString("MMM dd yyyy k:mm:ss.SSS")))
        
      val result = testEval(input) collect {
        case (ids, SString(d)) if ids.length == 1 => d.toString
      }

      result must haveSize(4)

      result must contain("2010-06-03T04:12:33.323Z", "2010-06-04T13:31:49.002Z", "2011-08-12T22:42:33.310Z", "2010-10-09T09:27:31.953Z")
    }

    "ChangeTimeZone function with not fully formed string without tz" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(ChangeTimeZone), CrossLeftSort,
          Root(line, CString("2010-06-04")),
          Root(line, CString("-10:00")))
        
      val result = testEval(input) collect {
        case (ids, SString(d)) if ids.length == 0 => d.toString
      }

      result must haveSize(1)

      result must contain("2010-06-03T14:00:00.000-10:00")
    }

    "ChangeTimeZone function with not fully formed string with tz" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(ChangeTimeZone), CrossLeftSort,
          Root(line, CString("2010-06-04T+05:00")),
          Root(line, CString("-10:00")))
        
      val result = testEval(input) collect {
        case (ids, SString(d)) if ids.length == 0 => d.toString
      }

      result must haveSize(1)

      result must contain("2010-06-03T09:00:00.000-10:00")
    }

    "Plus function with not fully formed string without tz" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(MinutesPlus), CrossLeftSort,
          Root(line, CString("2010-06-04T05:04:01")),
          Root(line, CLong(10)))
        
      val result = testEval(input) collect {
        case (ids, SString(d)) if ids.length == 0 => d.toString
      }

      result must haveSize(1)

      result must contain("2010-06-04T05:14:01.000Z")
    }

    "Plus function with not fully formed string with tz" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(MinutesPlus), CrossLeftSort,
          Root(line, CString("2010-06-04T05:04:01.000+05:00")),
          Root(line, CLong(10)))
        
      val result = testEval(input) collect {
        case (ids, SString(d)) if ids.length == 0 => d.toString
      }

      result must haveSize(1)

      result must contain("2010-06-04T05:14:01.000+05:00")
    }

    "Between function with not fully formed string without tz" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(HoursBetween), CrossLeftSort,
          Root(line, CString("2010-06-04T05:04:01")),
          Root(line, CString("2010-06-04T07:04:01+00:00")))
        
      val result = testEval(input) collect {
        case (ids, SDecimal(d)) if ids.length == 0 => d.toInt
      }

      result must haveSize(1)

      result must contain(2)
    }

    "Between function with not fully formed string with tz" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(HoursBetween), CrossLeftSort,
          Root(line, CString("2010-06-04T05:04:01+05:00")),
          Root(line, CString("2010-06-04T05:04:01+01:00")))
        
      val result = testEval(input) collect {
        case (ids, SDecimal(d)) if ids.length == 0 => d.toInt
      }

      result must haveSize(1)

      result must contain(4)
    }

    "GetMillis function with not fully formed string without tz" in {
      val line = Line(0, "")

      val input = Operate(line, BuiltInFunction1Op(GetMillis),
          Root(line, CString("2010-06-04T05")))
        
      val result = testEval(input) collect {
        case (ids, SDecimal(d)) if ids.length == 0 => d.toLong
      }

      result must haveSize(1)

      result must contain(1275627600000L)
    }

    "GetMillis function with not fully formed string with tz" in {
      val line = Line(0, "")

      val input = Operate(line, BuiltInFunction1Op(GetMillis),
          Root(line, CString("2010-06-04T03-02:00")))
        
      val result = testEval(input) collect {
        case (ids, SDecimal(d)) if ids.length == 0 => d.toLong
      }

      result must haveSize(1)

      result must contain(1275627600000L)
    }

    "TimeZone function with not fully formed string without tz" in {
      val line = Line(0, "")

      val input = Operate(line, BuiltInFunction1Op(TimeZone),
          Root(line, CString("2010-06-04T05")))
        
      val result = testEval(input) collect {
        case (ids, SString(d)) if ids.length == 0 => d.toString
      }

      result must haveSize(1)

      result must contain("+00:00")
    }

    "TimeZone function with not fully formed string with tz" in {
      val line = Line(0, "")

      val input = Operate(line, BuiltInFunction1Op(TimeZone),
          Root(line, CString("2010-06-04T03-02:00")))
        
      val result = testEval(input) collect {
        case (ids, SString(d)) if ids.length == 0 => d.toString
      }

      result must haveSize(1)

      result must contain("-02:00")
    }

    "Season function with not fully formed string without tz" in {
      val line = Line(0, "")

      val input = Operate(line, BuiltInFunction1Op(Season),
          Root(line, CString("2010-01-04")))
        
      val result = testEval(input) collect {
        case (ids, SString(d)) if ids.length == 0 => d.toString
      }

      result must haveSize(1)

      result must contain("winter")
    }

    "Season function with not fully formed string with tz" in {
      val line = Line(0, "")

      val input = Operate(line, BuiltInFunction1Op(Season),
          Root(line, CString("2010-01-04T-02:00")))
        
      val result = testEval(input) collect {
        case (ids, SString(d)) if ids.length == 0 => d.toString
      }

      result must haveSize(1)

      result must contain("winter")
    }

    "TimeFraction function with not fully formed string without tz" in {
      val line = Line(0, "")

      val input = Operate(line, BuiltInFunction1Op(HourOfDay),
          Root(line, CString("2010-01-04")))
        
      val result = testEval(input) collect {
        case (ids, SDecimal(d)) if ids.length == 0 => d.toInt
      }

      result must haveSize(1)

      result must contain(0)
    }

    "TimeFraction function with not fully formed string with tz" in {
      val line = Line(0, "")

      val input = Operate(line, BuiltInFunction1Op(HourOfDay),
          Root(line, CString("2010-01-04T03-02:00")))
        
      val result = testEval(input) collect {
        case (ids, SDecimal(d)) if ids.length == 0 => d.toInt
      }

      result must haveSize(1)

      result must contain(3)
    }
  }
      
  "changing time zones (homogenous case)" should {
    "change to the correct time zone" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(ChangeTimeZone), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))),
        Root(line, CString("-10:00")))
        
      val result = testEval(input) collect {
        case (ids, SString(d)) if ids.length == 1 => d.toString
      }
      
      result must contain("2011-02-21T01:09:59.165-10:00", "2012-02-11T06:11:33.394-10:00", "2011-09-06T06:44:52.848-10:00", "2010-04-28T15:37:52.599-10:00", "2012-12-28T06:38:19.430-10:00").only
    }

    "not modify millisecond value" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(ChangeTimeZone), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))),
        Root(line, CString("-10:00")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(time)) if ids.length == 1 => 
          val newTime = ISODateTimeFormat.dateTimeParser().withOffsetParsed.parseDateTime(time)
          newTime.getMillis.toLong
      }

      result2 must contain(1272505072599L, 1298286599165L, 1315327492848L, 1328976693394L, 1356712699430L)
    }

    "work correctly for fractional zones" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(ChangeTimeZone), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))),
        Root(line, CString("-10:30")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d.toString
      }
      
      result2 must contain("2011-02-21T00:39:59.165-10:30", "2012-02-11T05:41:33.394-10:30", "2011-09-06T06:14:52.848-10:30", "2010-04-28T15:07:52.599-10:30", "2012-12-28T06:08:19.430-10:30")
    }
  }

  "changing time zones (heterogeneous case)" should {
    "change to the correct time zone" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(ChangeTimeZone), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/het/iso8601"))),
        Root(line, CString("-10:00")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d.toString
      }
      
      result2 must contain("2011-02-21T01:09:59.165-10:00", "2012-02-11T06:11:33.394-10:00", "2011-09-06T06:44:52.848-10:00", "2010-04-28T15:37:52.599-10:00", "2012-12-28T06:38:19.430-10:00")
    }

    "not modify millisecond value" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(ChangeTimeZone), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))),
        Root(line, CString("-10:00")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(time)) if ids.length == 1 => 
          val newTime = ISODateTimeFormat.dateTimeParser().withOffsetParsed.parseDateTime(time)
          newTime.getMillis.toLong
      }

      result2 must contain(1272505072599L, 1298286599165L, 1315327492848L, 1328976693394L, 1356712699430L)
    }

    "work correctly for fractional zones" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(ChangeTimeZone), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/het/iso8601"))),
        Root(line, CString("-10:30")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d.toString
      }
      
      result2 must contain("2011-02-21T00:39:59.165-10:30", "2012-02-11T05:41:33.394-10:30", "2011-09-06T06:14:52.848-10:30", "2010-04-28T15:07:52.599-10:30", "2012-12-28T06:08:19.430-10:30")
    }
  }

  "converting an ISO time string to a millis value (homogeneous case)" should {
    "return the correct millis value" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(GetMillis),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toLong
      }
      
      result2 must contain(1272505072599L, 1315327492848L, 1328976693394L, 1356712699430L, 1298286599165L)
    }
  }

  "converting an ISO time string to a millis value (heterogeneous case)" should {
    "return the correct millis value" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(GetMillis),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toLong
      }
      
      result2 must contain(1272505072599L, 1315327492848L, 1328976693394L, 1356712699430L, 1298286599165L)
    }  
  }

 "converting a millis value to an ISO time string (homogeneous case)" should {
    "return the correct time string" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(MillisToISO), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/hom/millisSinceEpoch"))),
        Root(line, CString("-10:00")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d.toString
      }
      
      result2 must contain("2012-02-28T06:44:52.420-10:00", "2012-02-18T06:44:52.780-10:00", "2012-02-21T08:28:42.774-10:00", "2012-02-25T08:01:27.710-10:00", "2012-02-18T06:44:52.854-10:00")      
    }

    "default to UTC if time zone is not specified" in todo

  }

  "converting a millis value to an ISO time string (heterogeneous set)" should {
    "return the correct time string" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(MillisToISO), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/het/millisSinceEpoch"))),
        Root(line, CString("-10:00")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d.toString
      }
      
      result2 must contain("2012-02-28T06:44:52.420-10:00", "2012-02-18T06:44:52.780-10:00", "2012-02-21T08:28:42.774-10:00", "2012-02-25T08:01:27.710-10:00", "2012-02-18T06:44:52.854-10:00")      
    }

    "default to UTC if time zone is not specified" in todo

  }

  "time plus functions (homogeneous case)" should {
    "compute incrememtation of positive number of years" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(YearsPlus), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))),
        Root(line, CLong(5)))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(s)) if ids.length == 1 => s
      }
      
      result2 must contain(
        "2015-04-29T09:37:52.599+08:00", 
        "2016-02-21T20:09:59.165+09:00",
        "2016-09-06T06:44:52.848-10:00",
        "2017-02-11T09:11:33.394-07:00",
        "2017-12-28T22:38:19.430+06:00")
    }
    "compute incrememtation of negative number of years" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(YearsPlus), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))),
        Root(line, CLong(-5)))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(s)) if ids.length == 1 => s
      }
      
      result2 must contain(
        "2005-04-29T09:37:52.599+08:00", 
        "2006-02-21T20:09:59.165+09:00",
        "2006-09-06T06:44:52.848-10:00",
        "2007-02-11T09:11:33.394-07:00",
        "2007-12-28T22:38:19.430+06:00")
    }
    "compute incrememtation of zero of years" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(YearsPlus), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))),
        Root(line, CLong(0)))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(s)) if ids.length == 1 => s
      }
      
      result2 must contain(
        "2010-04-29T09:37:52.599+08:00", 
        "2011-02-21T20:09:59.165+09:00",
        "2011-09-06T06:44:52.848-10:00",
        "2012-02-11T09:11:33.394-07:00",
        "2012-12-28T22:38:19.430+06:00")
    }

    "compute incrememtation of months" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(MonthsPlus), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))),
        Root(line, CLong(5)))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(s)) if ids.length == 1 => s
      }
      
      result2 must contain(
        "2010-09-29T09:37:52.599+08:00", 
        "2011-07-21T20:09:59.165+09:00",
        "2012-02-06T06:44:52.848-10:00",
        "2012-07-11T09:11:33.394-07:00",
        "2013-05-28T22:38:19.430+06:00")
    }

    "compute incrememtation of weeks" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(WeeksPlus), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))),
        Root(line, CLong(5)))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(s)) if ids.length == 1 => s
      }
      
      result2 must contain(
        "2011-10-11T06:44:52.848-10:00", 
        "2012-03-17T09:11:33.394-07:00", 
        "2011-03-28T20:09:59.165+09:00", 
        "2013-02-01T22:38:19.430+06:00",
        "2010-06-03T09:37:52.599+08:00")
    }
    "compute incrememtation of days" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(DaysPlus), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))),
        Root(line, CLong(5)))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(s)) if ids.length == 1 => s
      }
      
      result2 must contain(
        "2010-05-04T09:37:52.599+08:00", 
        "2011-02-26T20:09:59.165+09:00",
        "2011-09-11T06:44:52.848-10:00",
        "2012-02-16T09:11:33.394-07:00",
        "2013-01-02T22:38:19.430+06:00")
    }
    "compute incrememtation of hours" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(HoursPlus), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))),
        Root(line, CLong(5)))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(s)) if ids.length == 1 => s
      }
      
      result2 must contain(
        "2010-04-29T14:37:52.599+08:00",
        "2011-02-22T01:09:59.165+09:00",
        "2011-09-06T11:44:52.848-10:00",
        "2012-02-11T14:11:33.394-07:00",
        "2012-12-29T03:38:19.430+06:00")
    }
    "compute incrememtation of minutes" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(MinutesPlus), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))),
        Root(line, CLong(5)))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(s)) if ids.length == 1 => s
      }
      
      result2 must contain(
        "2010-04-29T09:42:52.599+08:00", 
        "2011-02-21T20:14:59.165+09:00",
        "2011-09-06T06:49:52.848-10:00",
        "2012-02-11T09:16:33.394-07:00",
        "2012-12-28T22:43:19.430+06:00")
    }
    "compute incrememtation of seconds" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(SecondsPlus), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))),
        Root(line, CLong(5)))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(s)) if ids.length == 1 => s
      }
      
      result2 must contain(
        "2010-04-29T09:37:57.599+08:00", 
        "2011-02-21T20:10:04.165+09:00",
        "2011-09-06T06:44:57.848-10:00",
        "2012-02-11T09:11:38.394-07:00",
        "2012-12-28T22:38:24.430+06:00")
    }
    "compute incrememtation of ms" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(MillisPlus), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))),
        Root(line, CLong(5)))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(s)) if ids.length == 1 => s
      }
      
      result2 must contain(
        "2010-04-29T09:37:52.604+08:00", 
        "2011-02-21T20:09:59.170+09:00",
        "2011-09-06T06:44:52.853-10:00",
        "2012-02-11T09:11:33.399-07:00",
        "2012-12-28T22:38:19.435+06:00")
    }
  }

  "time plus functions (heterogeneous case)" should {
    "compute incrememtation of years" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(YearsPlus), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/het/iso8601"))),
        Root(line, CLong(5)))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(s)) if ids.length == 1 => s
      }
      
      result2 must contain(
        "2015-04-29T09:37:52.599+08:00", 
        "2016-02-21T20:09:59.165+09:00",
        "2016-09-06T06:44:52.848-10:00",
        "2017-02-11T09:11:33.394-07:00",
        "2017-12-28T22:38:19.430+06:00")
    }

    "compute incrememtation of months" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(MonthsPlus), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/het/iso8601"))),
        Root(line, CLong(5)))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(s)) if ids.length == 1 => s
      }
      
      result2 must contain(
        "2010-09-29T09:37:52.599+08:00", 
        "2011-07-21T20:09:59.165+09:00",
        "2012-02-06T06:44:52.848-10:00",
        "2012-07-11T09:11:33.394-07:00",
        "2013-05-28T22:38:19.430+06:00")
    }

    "compute incrememtation of weeks" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(WeeksPlus), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/het/iso8601"))),
        Root(line, CLong(5)))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(s)) if ids.length == 1 => s
      }
      
      result2 must contain(
        "2011-10-11T06:44:52.848-10:00", 
        "2012-03-17T09:11:33.394-07:00", 
        "2011-03-28T20:09:59.165+09:00", 
        "2013-02-01T22:38:19.430+06:00",
        "2010-06-03T09:37:52.599+08:00")
    }
    "compute incrememtation of days" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(DaysPlus), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/het/iso8601"))),
        Root(line, CLong(5)))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(s)) if ids.length == 1 => s
      }
      
      result2 must contain(
        "2010-05-04T09:37:52.599+08:00", 
        "2011-02-26T20:09:59.165+09:00",
        "2011-09-11T06:44:52.848-10:00",
        "2012-02-16T09:11:33.394-07:00",
        "2013-01-02T22:38:19.430+06:00")
    }
    "compute incrememtation of hours" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(HoursPlus), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/het/iso8601"))),
        Root(line, CLong(5)))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(s)) if ids.length == 1 => s
      }
      
      result2 must contain(
        "2010-04-29T14:37:52.599+08:00",
        "2011-02-22T01:09:59.165+09:00",
        "2011-09-06T11:44:52.848-10:00",
        "2012-02-11T14:11:33.394-07:00",
        "2012-12-29T03:38:19.430+06:00")
    }
    "compute incrememtation of minutes" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(MinutesPlus), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/het/iso8601"))),
        Root(line, CLong(5)))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(s)) if ids.length == 1 => s
      }
      
      result2 must contain(
        "2010-04-29T09:42:52.599+08:00", 
        "2011-02-21T20:14:59.165+09:00",
        "2011-09-06T06:49:52.848-10:00",
        "2012-02-11T09:16:33.394-07:00",
        "2012-12-28T22:43:19.430+06:00")
    }
    "compute incrememtation of seconds" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(SecondsPlus), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/het/iso8601"))),
        Root(line, CLong(5)))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(s)) if ids.length == 1 => s
      }
      
      result2 must contain(
        "2010-04-29T09:37:57.599+08:00", 
        "2011-02-21T20:10:04.165+09:00",
        "2011-09-06T06:44:57.848-10:00",
        "2012-02-11T09:11:38.394-07:00",
        "2012-12-28T22:38:24.430+06:00")
    }
    "compute incrememtation of ms" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(MillisPlus), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/het/iso8601"))),
        Root(line, CLong(5)))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(s)) if ids.length == 1 => s
      }
      
      result2 must contain(
        "2010-04-29T09:37:52.604+08:00", 
        "2011-02-21T20:09:59.170+09:00",
        "2011-09-06T06:44:52.853-10:00",
        "2012-02-11T09:11:33.399-07:00",
        "2012-12-28T22:38:19.435+06:00")
    }
  }

  "time difference functions (homogeneous case)" should {
    "compute difference of years" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(YearsBetween), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))),
        Root(line, CString("2010-09-23T18:33:22.520-10:00")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(-2, -1, 0)
    }
    "compute difference of months" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(MonthsBetween), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))),
        Root(line, CString("2010-09-23T18:33:22.520-10:00")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(-16, -4, -27, 4, -11)
    }
    "compute difference of weeks" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(WeeksBetween), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))),
        Root(line, CString("2010-09-23T18:33:22.520-10:00")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(-49, -118, -72, -21, 21)
    }
    "compute difference of days" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(DaysBetween), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))),
        Root(line, CString("2010-09-23T18:33:22.520-10:00")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(-505, -347, 148, -826, -150)
    }
    "compute difference of hours" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(HoursBetween), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))),
        Root(line, CString("2010-09-23T18:33:22.520-10:00")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toLong
      }
      
      result2 must contain(-12131, -3606, -19836, -8340, 3554)
    }
    "compute difference of minutes" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(MinutesBetween), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))),
        Root(line, CString("2010-09-23T18:33:22.520-10:00")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toLong
      }
      
      result2 must contain(-727898, 213295, -216396, -500411, -1190164)
    }
    "compute difference of seconds" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(SecondsBetween), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))),
        Root(line, CString("2010-09-23T18:33:22.520-10:00")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toLong
      }
      
      result2 must contain(-30024690, -43673890, -12983796, -71409896, 12797729)
    }
    "compute difference of ms" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(MillisBetween), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))),
        Root(line, CString("2010-09-23T18:33:22.520-10:00")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toLong
      }
      
      result2 must contain(12797729921L, -12983796645L, -30024690328L, -43673890874L, -71409896910L)
    }
  }

  "time difference functions (heterogeneous case)" should {
    "compute difference of years" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(YearsBetween), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/het/iso8601"))),
        Root(line, CString("2010-09-23T18:33:22.520-10:00")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(-2, -1, 0)
    }
    "compute difference of months" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(MonthsBetween), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/het/iso8601"))),
        Root(line, CString("2010-09-23T18:33:22.520-10:00")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(-16, -4, -27, 4, -11)
    }
    "compute difference of weeks" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(WeeksBetween), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/het/iso8601"))),
        Root(line, CString("2010-09-23T18:33:22.520-10:00")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(-49, -118, -72, -21, 21)
    }
    "compute difference of days" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(DaysBetween), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/het/iso8601"))),
        Root(line, CString("2010-09-23T18:33:22.520-10:00")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(-505, -347, 148, -826, -150)
    }
    "compute difference of hours" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(HoursBetween), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/het/iso8601"))),
        Root(line, CString("2010-09-23T18:33:22.520-10:00")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toLong
      }
      
      result2 must contain(-12131, -3606, -19836, -8340, 3554)
    }
    "compute difference of minutes" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(MinutesBetween), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/het/iso8601"))),
        Root(line, CString("2010-09-23T18:33:22.520-10:00")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toLong
      }
      
      result2 must contain(-727898, 213295, -216396, -500411, -1190164)
    }
    "compute difference of seconds" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(SecondsBetween), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/het/iso8601"))),
        Root(line, CString("2010-09-23T18:33:22.520-10:00")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toLong
      }
      
      result2 must contain(-30024690, -43673890, -12983796, -71409896, 12797729)
    }
    "compute difference of ms" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(MillisBetween), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/het/iso8601"))),
        Root(line, CString("2010-09-23T18:33:22.520-10:00")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toLong
      }
      
      result2 must contain(12797729921L, -12983796645L, -30024690328L, -43673890874L, -71409896910L)
    }
  }


  "time extraction functions (homogeneous case)" should {
    "extract time zone" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(TimeZone),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d.toString
      }
      
      result2 must contain("+08:00", "+09:00", "-10:00", "-07:00", "+06:00")
    }     
  
    "compute season" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(Season),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d.toString
      }
      
      result2 must contain("spring", "winter", "summer")
    }

    "compute year" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(Year),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(2010, 2011, 2012)
    }

    "compute quarter" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(QuarterOfYear),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(1, 2, 3, 4)
    }

    "compute month of year" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(MonthOfYear),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(4, 2, 9, 12)
    }
    
    "compute week of year" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(WeekOfYear),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(17, 8, 36, 6, 52)
    }
    "compute week of month" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(WeekOfMonth),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(2, 5, 4)
    }
    "compute day of year" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(DayOfYear),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(52, 119, 42, 249, 363)
    }
    "compute day of month" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(DayOfMonth),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
        
      result2 must contain(21, 29, 11, 6, 28)
    }
    "compute day of week" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(DayOfWeek),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(1, 2, 6, 5, 4)
    }
    "compute hour of day" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(HourOfDay),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(20, 6, 9)
    }
    "compute minute of hour" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(MinuteOfHour),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(9, 44, 11, 37, 38)
    }
    "compute second of minute" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(SecondOfMinute),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(19, 59, 52, 33)
    }
    "compute millis of second" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(MillisOfSecond),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(430, 165, 848, 394, 599)
    }
  }

  "time extraction functions (heterogeneous case)" should {
    "extract time zone" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(TimeZone),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d.toString
      }
      
      result2 must contain("+08:00", "+09:00", "-10:00", "-07:00", "+06:00")
    }     
  
    "compute season" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(Season),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d.toString
      }
      
      result2 must contain("spring", "winter", "summer")
    }

    "compute year" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(Year),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(2010, 2011, 2012)
    }

    "compute quarter" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(QuarterOfYear),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(1, 2, 3, 4)
    }

    "compute month of year" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(MonthOfYear),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(4, 2, 9, 12)
    }
    
    "compute week of year" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(WeekOfYear),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(17, 8, 36, 6, 52)
    }
    "compute week of month" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(WeekOfMonth),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(2, 5, 4)
    }
    "compute day of year" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(DayOfYear),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(52, 119, 42, 249, 363)
    }
    "compute day of month" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(DayOfMonth),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
        
      result2 must contain(21, 29, 11, 6, 28)
    }
    "compute day of week" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(DayOfWeek),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(1, 2, 6, 5, 4)
    }
    "compute hour of day" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(HourOfDay),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(20, 6, 9)
    }
    "compute minute of hour" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(MinuteOfHour),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(9, 44, 11, 37, 38)
    }
    "compute second of minute" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(SecondOfMinute),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(19, 59, 52, 33)
    }
    "compute millis of second" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(MillisOfSecond),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(430, 165, 848, 394, 599)
    }
  }

  "time truncation functions (homogeneous case)" should {
    "determine date" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(Date),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("2010-04-29","2011-02-21","2011-09-06","2012-02-11","2012-12-28")
    }
    "determine year and month" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(YearMonth),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("2010-04","2011-02","2011-09","2012-02","2012-12")
    }
    "determine year and day of year" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(YearDayOfYear),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("2012-363", "2011-249", "2012-042", "2010-119", "2011-052")
    }
    "determine month and day" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(MonthDay),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("04-29","02-21","09-06","02-11","12-28")
    }
    "determine date and hour" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(DateHour),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("2010-04-29T09","2011-02-21T20","2011-09-06T06","2012-02-11T09","2012-12-28T22")
    }
    "determine date, hour, and minute" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(DateHourMinute),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("2010-04-29T09:37","2011-02-21T20:09","2011-09-06T06:44","2012-02-11T09:11","2012-12-28T22:38")
    }
    "determine date, hour, minute, and second" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(DateHourMinuteSecond),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("2010-04-29T09:37:52","2011-02-21T20:09:59","2011-09-06T06:44:52","2012-02-11T09:11:33","2012-12-28T22:38:19")
    }
    "determine date, hour, minute, second, and ms" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(DateHourMinuteSecondMillis),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("2010-04-29T09:37:52.599","2011-02-21T20:09:59.165","2011-09-06T06:44:52.848","2012-02-11T09:11:33.394","2012-12-28T22:38:19.430")
    }
    "determine time with timezone" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(TimeWithZone),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("09:37:52.599+08:00","20:09:59.165+09:00","06:44:52.848-10:00","09:11:33.394-07:00","22:38:19.430+06:00")
    }
    "determine time without timezone" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(TimeWithoutZone),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("09:37:52.599","20:09:59.165","06:44:52.848","09:11:33.394","22:38:19.430")
    }
    "determine hour and minute" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(HourMinute),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("09:37","20:09","06:44","09:11","22:38")
    }
    "determine hour, minute, and second" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(HourMinuteSecond),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("09:37:52","20:09:59","06:44:52","09:11:33","22:38:19")
    }
  }

  "time truncation functions (heterogeneous case)" should {
    "determine date" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(Date),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("2010-04-29","2011-02-21","2011-09-06","2012-02-11","2012-12-28")
    }
    "determine year and month" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(YearMonth),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("2010-04","2011-02","2011-09","2012-02","2012-12")
    }
    "determine year and day of year" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(YearDayOfYear),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("2012-363", "2011-249", "2012-042", "2010-119", "2011-052")
    }
    "determine month and day" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(MonthDay),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("04-29","02-21","09-06","02-11","12-28")
    }
    "determine date and hour" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(DateHour),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("2010-04-29T09","2011-02-21T20","2011-09-06T06","2012-02-11T09","2012-12-28T22")
    }
    "determine date, hour, and minute" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(DateHourMinute),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("2010-04-29T09:37","2011-02-21T20:09","2011-09-06T06:44","2012-02-11T09:11","2012-12-28T22:38")
    }
    "determine date, hour, minute, and second" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(DateHourMinuteSecond),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("2010-04-29T09:37:52","2011-02-21T20:09:59","2011-09-06T06:44:52","2012-02-11T09:11:33","2012-12-28T22:38:19")
    }
    "determine date, hour, minute, second, and ms" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(DateHourMinuteSecondMillis),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("2010-04-29T09:37:52.599","2011-02-21T20:09:59.165","2011-09-06T06:44:52.848","2012-02-11T09:11:33.394","2012-12-28T22:38:19.430")
    }
    "determine time with timezone" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(TimeWithZone),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("09:37:52.599+08:00","20:09:59.165+09:00","06:44:52.848-10:00","09:11:33.394-07:00","22:38:19.430+06:00")
    }
    "determine time without timezone" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(TimeWithoutZone),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("09:37:52.599","20:09:59.165","06:44:52.848","09:11:33.394","22:38:19.430")
    }
    "determine hour and minute" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(HourMinute),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("09:37","20:09","06:44","09:11","22:38")
    }
    "determine hour, minute, and second" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(HourMinuteSecond),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("09:37:52","20:09:59","06:44:52","09:11:33","22:38:19")
    }
  }

  "changing time zones (homogenous case)" should {
    "change to the correct time zone" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(ChangeTimeZone), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))),
        Root(line, CString("-10:00")))

      val result = testEval(input) collect {
        case (ids, SString(d)) if ids.length == 1 => d.toString
      }

      result must contain("2011-02-21T01:09:59.165-10:00", "2012-02-11T06:11:33.394-10:00", "2011-09-06T06:44:52.848-10:00", "2010-04-28T15:37:52.599-10:00", "2012-12-28T06:38:19.430-10:00").only
    }

    "not modify millisecond value" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(ChangeTimeZone), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))),
        Root(line, CString("-10:00")))

      val result = testEval(input)

      result must haveSize(5)

      val result2 = result collect {
        case (ids, SString(time)) if ids.length == 1 =>
          val newTime = ISODateTimeFormat.dateTimeParser().withOffsetParsed.parseDateTime(time)
          newTime.getMillis.toLong
      }

      result2 must contain(1272505072599L, 1298286599165L, 1315327492848L, 1328976693394L, 1356712699430L)
    }

    "work correctly for fractional zones" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(ChangeTimeZone), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))),
        Root(line, CString("-10:30")))

      val result = testEval(input)

      result must haveSize(5)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d.toString
      }

      result2 must contain("2011-02-21T00:39:59.165-10:30", "2012-02-11T05:41:33.394-10:30", "2011-09-06T06:14:52.848-10:30", "2010-04-28T15:07:52.599-10:30", "2012-12-28T06:08:19.430-10:30")
    }
  }

  "changing time zones (heterogeneous case)" should {
    "change to the correct time zone" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(ChangeTimeZone), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/het/iso8601"))),
        Root(line, CString("-10:00")))

      val result = testEval(input)

      result must haveSize(5)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d.toString
      }

      result2 must contain("2011-02-21T01:09:59.165-10:00", "2012-02-11T06:11:33.394-10:00", "2011-09-06T06:44:52.848-10:00", "2010-04-28T15:37:52.599-10:00", "2012-12-28T06:38:19.430-10:00")
    }

    "not modify millisecond value" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(ChangeTimeZone), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))),
        Root(line, CString("-10:00")))

      val result = testEval(input)

      result must haveSize(5)

      val result2 = result collect {
        case (ids, SString(time)) if ids.length == 1 =>
          val newTime = ISODateTimeFormat.dateTimeParser().withOffsetParsed.parseDateTime(time)
          newTime.getMillis.toLong
      }

      result2 must contain(1272505072599L, 1298286599165L, 1315327492848L, 1328976693394L, 1356712699430L)
    }

    "work correctly for fractional zones" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(ChangeTimeZone), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/het/iso8601"))),
        Root(line, CString("-10:30")))

      val result = testEval(input)

      result must haveSize(5)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d.toString
      }

      result2 must contain("2011-02-21T00:39:59.165-10:30", "2012-02-11T05:41:33.394-10:30", "2011-09-06T06:14:52.848-10:30", "2010-04-28T15:07:52.599-10:30", "2012-12-28T06:08:19.430-10:30")
    }
  }

  "converting an ISO time string to a millis value (homogeneous case)" should {
    "return the correct millis value" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(GetMillis),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601"))))

      val result = testEval(input)

      result must haveSize(5)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toLong
      }

      result2 must contain(1272505072599L, 1315327492848L, 1328976693394L, 1356712699430L, 1298286599165L)
    }
  }

  "converting an ISO time string to a millis value (heterogeneous case)" should {
    "return the correct millis value" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(GetMillis),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601"))))

      val result = testEval(input)

      result must haveSize(5)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toLong
      }

      result2 must contain(1272505072599L, 1315327492848L, 1328976693394L, 1356712699430L, 1298286599165L)
    }
  }

 "converting a millis value to an ISO time string (homogeneous case)" should {
    "return the correct time string" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(MillisToISO), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/hom/millisSinceEpoch"))),
        Root(line, CString("-10:00")))

      val result = testEval(input)

      result must haveSize(5)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d.toString
      }

      result2 must contain("2012-02-28T06:44:52.420-10:00", "2012-02-18T06:44:52.780-10:00", "2012-02-21T08:28:42.774-10:00", "2012-02-25T08:01:27.710-10:00", "2012-02-18T06:44:52.854-10:00")
    }

    "default to UTC if time zone is not specified" in todo

  }

  "converting a millis value to an ISO time string (heterogeneous set)" should {
    "return the correct time string" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(MillisToISO), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/het/millisSinceEpoch"))),
        Root(line, CString("-10:00")))

      val result = testEval(input)

      result must haveSize(5)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d.toString
      }

      result2 must contain("2012-02-28T06:44:52.420-10:00", "2012-02-18T06:44:52.780-10:00", "2012-02-21T08:28:42.774-10:00", "2012-02-25T08:01:27.710-10:00", "2012-02-18T06:44:52.854-10:00")
    }

    "default to UTC if time zone is not specified" in todo

  }

  "time plus functions (homogeneous case across slices)" should {
    "compute incrememtation of positive number of years" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(YearsPlus), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601AcrossSlices"))),
        Root(line, CLong(5)))

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SString(s)) if ids.length == 1 => s
      }

      result2 must contain(
        "2017-03-04T12:19:00.040Z",
        "2014-08-04T04:52:17.443Z",
        "2014-05-18T11:33:38.358+11:00",
        "2014-10-29T02:43:41.657+04:00",
        "2012-02-04T10:58:14.041-01:00",
        "2014-07-17T10:30:16.115+07:00",
        "2014-05-02T01:14:41.555-10:00",
        "2017-07-30T13:18:40.252-03:00",
        "2016-02-15T13:49:53.937+07:00",
        "2016-10-27T01:11:04.423-04:00",
        "2013-01-10T18:36:48.745-03:00",
        "2017-10-11T00:36:31.692-02:00",
        "2016-08-11T19:29:55.119+05:00",
        "2015-02-09T02:20:17.040-05:00",
        "2017-12-28T22:38:19.430+06:00",
        "2016-03-06T13:56:56.877-02:00",
        "2012-03-24T04:49:22.259-09:00",
        "2017-03-14T03:48:21.874Z",
        "2013-05-23T17:31:37.488Z",
        "2016-02-10T14:53:34.278-01:00",
        "2013-03-06T21:02:28.910-11:00",
        "2017-08-15T21:05:04.684Z")
    }
    "compute incrememtation of negative number of years" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(YearsPlus), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601AcrossSlices"))),
        Root(line, CLong(-5)))

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SString(s)) if ids.length == 1 => s
      }

      result2 must contain(
        "2006-03-06T13:56:56.877-02:00",
        "2007-08-15T21:05:04.684Z",
        "2004-05-18T11:33:38.358+11:00",
        "2002-02-04T10:58:14.041-01:00",
        "2007-03-14T03:48:21.874Z",
        "2006-02-10T14:53:34.278-01:00",
        "2004-07-17T10:30:16.115+07:00",
        "2007-03-04T12:19:00.040Z",
        "2006-02-15T13:49:53.937+07:00",
        "2004-05-02T01:14:41.555-10:00",
        "2003-01-10T18:36:48.745-03:00",
        "2003-05-23T17:31:37.488Z",
        "2007-10-11T00:36:31.692-02:00",
        "2007-12-28T22:38:19.430+06:00",
        "2004-08-04T04:52:17.443Z",
        "2006-08-11T19:29:55.119+05:00",
        "2007-07-30T13:18:40.252-03:00",
        "2006-10-27T01:11:04.423-04:00",
        "2004-10-29T02:43:41.657+04:00",
        "2005-02-09T02:20:17.040-05:00",
        "2003-03-06T21:02:28.910-11:00",
        "2002-03-24T04:49:22.259-09:00")
    }
    "compute incrememtation of zero of years" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(YearsPlus), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601AcrossSlices"))),
        Root(line, CLong(0)))

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SString(s)) if ids.length == 1 => s
      }

      result2 must contain(
        "2008-03-06T21:02:28.910-11:00",
        "2008-05-23T17:31:37.488Z",
        "2009-07-17T10:30:16.115+07:00",
        "2011-03-06T13:56:56.877-02:00",
        "2012-12-28T22:38:19.430+06:00",
        "2008-01-10T18:36:48.745-03:00",
        "2012-08-15T21:05:04.684Z",
        "2011-08-11T19:29:55.119+05:00",
        "2007-02-04T10:58:14.041-01:00",
        "2012-10-11T00:36:31.692-02:00",
        "2009-05-02T01:14:41.555-10:00",
        "2011-02-10T14:53:34.278-01:00",
        "2009-10-29T02:43:41.657+04:00",
        "2010-02-09T02:20:17.040-05:00",
        "2009-05-18T11:33:38.358+11:00",
        "2012-07-30T13:18:40.252-03:00",
        "2012-03-14T03:48:21.874Z",
        "2009-08-04T04:52:17.443Z",
        "2011-02-15T13:49:53.937+07:00",
        "2007-03-24T04:49:22.259-09:00",
        "2012-03-04T12:19:00.040Z",
        "2011-10-27T01:11:04.423-04:00")
    }

    "compute incrememtation of months" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(MonthsPlus), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601AcrossSlices"))),
        Root(line, CLong(5)))

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SString(s)) if ids.length == 1 => s
      }

      result2 must contain(
        "2010-03-29T02:43:41.657+04:00",
        "2012-12-30T13:18:40.252-03:00",
        "2012-08-04T12:19:00.040Z",
        "2007-07-04T10:58:14.041-01:00",
        "2009-12-17T10:30:16.115+07:00",
        "2013-03-11T00:36:31.692-02:00",
        "2012-03-27T01:11:04.423-04:00",
        "2008-06-10T18:36:48.745-03:00",
        "2012-08-14T03:48:21.874Z",
        "2009-10-18T11:33:38.358+11:00",
        "2008-08-06T21:02:28.910-11:00",
        "2010-07-09T02:20:17.040-05:00",
        "2013-05-28T22:38:19.430+06:00",
        "2008-10-23T17:31:37.488Z",
        "2011-07-15T13:49:53.937+07:00",
        "2010-01-04T04:52:17.443Z",
        "2011-08-06T13:56:56.877-02:00",
        "2013-01-15T21:05:04.684Z",
        "2011-07-10T14:53:34.278-01:00",
        "2009-10-02T01:14:41.555-10:00",
        "2012-01-11T19:29:55.119+05:00",
        "2007-08-24T04:49:22.259-09:00")
    }

    "compute incrememtation of weeks" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(WeeksPlus), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601AcrossSlices"))),
        Root(line, CLong(5)))

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SString(s)) if ids.length == 1 => s
      }

      result2 must contain(
        "2011-09-15T19:29:55.119+05:00",
        "2012-09-03T13:18:40.252-03:00",
        "2009-09-08T04:52:17.443Z",
        "2008-04-10T21:02:28.910-11:00",
        "2009-06-22T11:33:38.358+11:00",
        "2010-03-16T02:20:17.040-05:00",
        "2012-04-18T03:48:21.874Z",
        "2007-03-11T10:58:14.041-01:00",
        "2012-04-08T12:19:00.040Z",
        "2013-02-01T22:38:19.430+06:00",
        "2008-02-14T18:36:48.745-03:00",
        "2008-06-27T17:31:37.488Z",
        "2007-04-28T04:49:22.259-09:00",
        "2009-08-21T10:30:16.115+07:00",
        "2009-12-03T02:43:41.657+04:00",
        "2009-06-06T01:14:41.555-10:00",
        "2012-11-15T00:36:31.692-02:00",
        "2011-04-10T13:56:56.877-02:00",
        "2011-03-22T13:49:53.937+07:00",
        "2011-03-17T14:53:34.278-01:00",
        "2011-12-01T01:11:04.423-04:00",
        "2012-09-19T21:05:04.684Z")
    }
    "compute incrememtation of days" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(DaysPlus), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601AcrossSlices"))),
        Root(line, CLong(5)))

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SString(s)) if ids.length == 1 => s
      }

      result2 must contain(
        "2010-02-14T02:20:17.040-05:00",
        "2007-03-29T04:49:22.259-09:00",
        "2012-08-20T21:05:04.684Z",
        "2011-11-01T01:11:04.423-04:00",
        "2012-10-16T00:36:31.692-02:00",
        "2013-01-02T22:38:19.430+06:00",
        "2009-07-22T10:30:16.115+07:00",
        "2008-05-28T17:31:37.488Z",
        "2008-01-15T18:36:48.745-03:00",
        "2009-11-03T02:43:41.657+04:00",
        "2011-08-16T19:29:55.119+05:00",
        "2012-03-09T12:19:00.040Z",
        "2009-08-09T04:52:17.443Z",
        "2012-03-19T03:48:21.874Z",
        "2011-03-11T13:56:56.877-02:00",
        "2011-02-20T13:49:53.937+07:00",
        "2007-02-09T10:58:14.041-01:00",
        "2008-03-11T21:02:28.910-11:00",
        "2012-08-04T13:18:40.252-03:00",
        "2009-05-23T11:33:38.358+11:00",
        "2009-05-07T01:14:41.555-10:00",
        "2011-02-15T14:53:34.278-01:00")
    }
    "compute incrememtation of hours" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(HoursPlus), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601AcrossSlices"))),
        Root(line, CLong(5)))

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SString(s)) if ids.length == 1 => s
      }

      result2 must contain(
        "2009-08-04T09:52:17.443Z",
        "2012-12-29T03:38:19.430+06:00",
        "2009-07-17T15:30:16.115+07:00",
        "2007-03-24T09:49:22.259-09:00",
        "2012-07-30T18:18:40.252-03:00",
        "2012-08-16T02:05:04.684Z",
        "2012-03-14T08:48:21.874Z",
        "2009-10-29T07:43:41.657+04:00",
        "2009-05-02T06:14:41.555-10:00",
        "2011-10-27T06:11:04.423-04:00",
        "2008-05-23T22:31:37.488Z",
        "2007-02-04T15:58:14.041-01:00",
        "2011-02-15T18:49:53.937+07:00",
        "2011-02-10T19:53:34.278-01:00",
        "2008-03-07T02:02:28.910-11:00",
        "2011-03-06T18:56:56.877-02:00",
        "2012-03-04T17:19:00.040Z",
        "2012-10-11T05:36:31.692-02:00",
        "2010-02-09T07:20:17.040-05:00",
        "2011-08-12T00:29:55.119+05:00",
        "2008-01-10T23:36:48.745-03:00",
        "2009-05-18T16:33:38.358+11:00")
    }
    "compute incrememtation of minutes" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(MinutesPlus), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601AcrossSlices"))),
        Root(line, CLong(5)))

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SString(s)) if ids.length == 1 => s
      }

      result2 must contain(
        "2012-03-04T12:24:00.040Z",
        "2008-03-06T21:07:28.910-11:00",
        "2012-03-14T03:53:21.874Z",
        "2011-10-27T01:16:04.423-04:00",
        "2011-08-11T19:34:55.119+05:00",
        "2009-10-29T02:48:41.657+04:00",
        "2012-08-15T21:10:04.684Z",
        "2007-03-24T04:54:22.259-09:00",
        "2012-12-28T22:43:19.430+06:00",
        "2009-05-02T01:19:41.555-10:00",
        "2007-02-04T11:03:14.041-01:00",
        "2009-08-04T04:57:17.443Z",
        "2012-10-11T00:41:31.692-02:00",
        "2011-02-10T14:58:34.278-01:00",
        "2011-03-06T14:01:56.877-02:00",
        "2012-07-30T13:23:40.252-03:00",
        "2009-07-17T10:35:16.115+07:00",
        "2008-05-23T17:36:37.488Z",
        "2010-02-09T02:25:17.040-05:00",
        "2011-02-15T13:54:53.937+07:00",
        "2008-01-10T18:41:48.745-03:00",
        "2009-05-18T11:38:38.358+11:00")
    }
    "compute incrememtation of seconds" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(SecondsPlus), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601AcrossSlices"))),
        Root(line, CLong(5)))

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SString(s)) if ids.length == 1 => s
      }

      result2 must contain(
        "2009-08-04T04:52:22.443Z",
        "2008-05-23T17:31:42.488Z",
        "2007-03-24T04:49:27.259-09:00",
        "2012-12-28T22:38:24.430+06:00",
        "2009-05-18T11:33:43.358+11:00",
        "2011-02-10T14:53:39.278-01:00",
        "2012-10-11T00:36:36.692-02:00",
        "2012-03-14T03:48:26.874Z",
        "2009-05-02T01:14:46.555-10:00",
        "2011-03-06T13:57:01.877-02:00",
        "2012-08-15T21:05:09.684Z",
        "2010-02-09T02:20:22.040-05:00",
        "2011-08-11T19:30:00.119+05:00",
        "2012-03-04T12:19:05.040Z",
        "2009-10-29T02:43:46.657+04:00",
        "2011-10-27T01:11:09.423-04:00",
        "2009-07-17T10:30:21.115+07:00",
        "2008-01-10T18:36:53.745-03:00",
        "2007-02-04T10:58:19.041-01:00",
        "2008-03-06T21:02:33.910-11:00",
        "2011-02-15T13:49:58.937+07:00",
        "2012-07-30T13:18:45.252-03:00")
    }
    "compute incrememtation of ms" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(MillisPlus), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601AcrossSlices"))),
        Root(line, CLong(5)))

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SString(s)) if ids.length == 1 => s
      }

      result2 must contain(
        "2009-05-02T01:14:41.560-10:00",
        "2010-02-09T02:20:17.045-05:00",
        "2012-08-15T21:05:04.689Z",
        "2008-03-06T21:02:28.915-11:00",
        "2009-10-29T02:43:41.662+04:00",
        "2011-08-11T19:29:55.124+05:00",
        "2011-02-10T14:53:34.283-01:00",
        "2008-01-10T18:36:48.750-03:00",
        "2009-05-18T11:33:38.363+11:00",
        "2012-07-30T13:18:40.257-03:00",
        "2011-03-06T13:56:56.882-02:00",
        "2009-07-17T10:30:16.120+07:00",
        "2011-10-27T01:11:04.428-04:00",
        "2012-10-11T00:36:31.697-02:00",
        "2007-02-04T10:58:14.046-01:00",
        "2009-08-04T04:52:17.448Z",
        "2012-03-04T12:19:00.045Z",
        "2012-03-14T03:48:21.879Z",
        "2012-12-28T22:38:19.435+06:00",
        "2008-05-23T17:31:37.493Z",
        "2007-03-24T04:49:22.264-09:00",
        "2011-02-15T13:49:53.942+07:00")
    }
  }

  "time plus functions (heterogeneous case across slices)" should {
    "compute incrememtation of years" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(YearsPlus), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/het/iso8601AcrossSlices"))),
        Root(line, CLong(5)))

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SString(s)) if ids.length == 1 => s
      }

      result2 must contain(
        "2013-10-24T11:44:19.844+03:00",
        "2017-05-05T08:58:10.171+10:00",
        "2015-11-21T23:50:10.932+06:00",
        "2015-10-25T01:51:16.248+04:00",
        "2012-07-14T03:49:30.311-07:00",
        "2016-06-25T00:18:50.873-11:00",
        "2013-05-27T16:27:24.858Z",
        "2013-07-02T18:53:43.506-04:00",
        "2014-08-17T05:54:08.513+02:00",
        "2016-10-13T15:47:40.629+08:00")
    }

    "compute incrememtation of months" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(MonthsPlus), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/het/iso8601AcrossSlices"))),
        Root(line, CLong(5)))

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SString(s)) if ids.length == 1 => s
      }

      result2 must contain(
        "2011-04-21T23:50:10.932+06:00",
        "2012-10-05T08:58:10.171+10:00",
        "2012-03-13T15:47:40.629+08:00",
        "2010-01-17T05:54:08.513+02:00",
        "2009-03-24T11:44:19.844+03:00",
        "2008-12-02T18:53:43.506-04:00",
        "2011-03-25T01:51:16.248+04:00",
        "2008-10-27T16:27:24.858Z",
        "2007-12-14T03:49:30.311-07:00",
        "2011-11-25T00:18:50.873-11:00")
    }

    "compute incrememtation of weeks" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(WeeksPlus), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/het/iso8601AcrossSlices"))),
        Root(line, CLong(5)))

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SString(s)) if ids.length == 1 => s
      }

      result2 must contain(
        "2009-09-21T05:54:08.513+02:00",
        "2011-11-17T15:47:40.629+08:00",
        "2008-08-06T18:53:43.506-04:00",
        "2011-07-30T00:18:50.873-11:00",
        "2012-06-09T08:58:10.171+10:00",
        "2010-12-26T23:50:10.932+06:00",
        "2010-11-29T01:51:16.248+04:00",
        "2008-11-28T11:44:19.844+03:00",
        "2007-08-18T03:49:30.311-07:00",
        "2008-07-01T16:27:24.858Z")
    }
    "compute incrememtation of days" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(DaysPlus), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/het/iso8601AcrossSlices"))),
        Root(line, CLong(5)))

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SString(s)) if ids.length == 1 => s
      }

      result2 must contain(
        "2008-06-01T16:27:24.858Z",
        "2007-07-19T03:49:30.311-07:00",
        "2010-10-30T01:51:16.248+04:00",
        "2009-08-22T05:54:08.513+02:00",
        "2008-10-29T11:44:19.844+03:00",
        "2010-11-26T23:50:10.932+06:00",
        "2011-06-30T00:18:50.873-11:00",
        "2011-10-18T15:47:40.629+08:00",
        "2012-05-10T08:58:10.171+10:00",
        "2008-07-07T18:53:43.506-04:00")
    }
    "compute incrememtation of hours" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(HoursPlus), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/het/iso8601AcrossSlices"))),
        Root(line, CLong(5)))

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SString(s)) if ids.length == 1 => s
      }

      result2 must contain(
        "2012-05-05T13:58:10.171+10:00",
        "2009-08-17T10:54:08.513+02:00",
        "2011-10-13T20:47:40.629+08:00",
        "2008-05-27T21:27:24.858Z",
        "2008-10-24T16:44:19.844+03:00",
        "2007-07-14T08:49:30.311-07:00",
        "2011-06-25T05:18:50.873-11:00",
        "2010-11-22T04:50:10.932+06:00",
        "2008-07-02T23:53:43.506-04:00",
        "2010-10-25T06:51:16.248+04:00")
    }
    "compute incrememtation of minutes" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(MinutesPlus), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/het/iso8601AcrossSlices"))),
        Root(line, CLong(5)))

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SString(s)) if ids.length == 1 => s
      }

      result2 must contain(
        "2010-11-21T23:55:10.932+06:00",
        "2011-10-13T15:52:40.629+08:00",
        "2008-10-24T11:49:19.844+03:00",
        "2010-10-25T01:56:16.248+04:00",
        "2012-05-05T09:03:10.171+10:00",
        "2008-07-02T18:58:43.506-04:00",
        "2011-06-25T00:23:50.873-11:00",
        "2008-05-27T16:32:24.858Z",
        "2007-07-14T03:54:30.311-07:00",
        "2009-08-17T05:59:08.513+02:00")
    }
    "compute incrememtation of seconds" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(SecondsPlus), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/het/iso8601AcrossSlices"))),
        Root(line, CLong(5)))

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SString(s)) if ids.length == 1 => s
      }

      result2 must contain(
        "2011-06-25T00:18:55.873-11:00",
        "2009-08-17T05:54:13.513+02:00",
        "2010-11-21T23:50:15.932+06:00",
        "2008-10-24T11:44:24.844+03:00",
        "2012-05-05T08:58:15.171+10:00",
        "2010-10-25T01:51:21.248+04:00",
        "2008-05-27T16:27:29.858Z",
        "2011-10-13T15:47:45.629+08:00",
        "2007-07-14T03:49:35.311-07:00",
        "2008-07-02T18:53:48.506-04:00")
    }
    "compute incrememtation of ms" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(MillisPlus), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/het/iso8601AcrossSlices"))),
        Root(line, CLong(5)))

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SString(s)) if ids.length == 1 => s
      }

      result2 must contain(
        "2008-07-02T18:53:43.511-04:00",
        "2011-06-25T00:18:50.878-11:00",
        "2007-07-14T03:49:30.316-07:00",
        "2010-11-21T23:50:10.937+06:00",
        "2008-05-27T16:27:24.863Z",
        "2010-10-25T01:51:16.253+04:00",
        "2008-10-24T11:44:19.849+03:00",
        "2011-10-13T15:47:40.634+08:00",
        "2012-05-05T08:58:10.176+10:00",
        "2009-08-17T05:54:08.518+02:00")
    }
  }

  "time difference functions (homogeneous case across slices)" should {
    "compute difference of years" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(YearsBetween), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601AcrossSlices"))),
        Root(line, CString("2010-09-23T18:33:22.520-10:00")))

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(0, 1, 2, 3, -1, -2)
    }
    "compute difference of months" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(MonthsBetween), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601AcrossSlices"))),
        Root(line, CString("2010-09-23T18:33:22.520-10:00")))

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(-27, 10, -22, 14, 28, -4, 13, 41, 32, -5, -10, 7, 16, 43, -13, 30, -24, -17)
    }
    "compute difference of weeks" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(WeeksBetween), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601AcrossSlices"))),
        Root(line, CString("2010-09-23T18:33:22.520-10:00")))

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(-98, -96, -118, -76, 189, 121, 132, 70, -75, -23, 141, -19, 32, 59, -20, -106, 182, 72, -56, 47, -45, 62)
    }
    "compute difference of days" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(DaysBetween), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601AcrossSlices"))),
        Root(line, CString("2010-09-23T18:33:22.520-10:00")))

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(1327, 930, -826, -536, -675, 1279, 853, -139, -691, -163, 509, 434, 494, 330, -747, 226, -527, -321, -398, 415, 987, -144)
    }
    "compute difference of hours" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(HoursBetween), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601AcrossSlices"))),
        Root(line, CString("2010-09-23T18:33:22.520-10:00")))

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toLong
      }

      result2 must contain(-3458, 31864, 11859, -12655, -9552, -3347, -16600, -12887, 12233, 22340, -16211, -17950, -19836, 10417, 23694, -3923, 20483, 9983, -7713, 30710, 7925, 5445)
    }
    "compute difference of minutes" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(MinutesBetween), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601AcrossSlices"))),
        Root(line, CString("2010-09-23T18:33:22.520-10:00")))

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toLong
      }

      result2 must contain(-573157, 1911875, 1228981, 599021, -1077003, 1421696, -1190164, 1340430, -235403, -773234, 1842644, -462836, 625023, -200840, -972705, 326713, 475549, 711599, -996031, 733998, -759345, -207496)
    }
    "compute difference of seconds" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(SecondsBetween), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601AcrossSlices"))),
        Root(line, CString("2010-09-23T18:33:22.520-10:00")))

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toLong
      }

      result2 must contain(-12050411, 42695984, 44039920, 114712508, -46394099, -64620189, -71409896, 28532980, 35941265, -27770192, -12449791, 80425853, 110558640, 19602785, 73738905, -34389461, -45560737, 37501386, -59761902, -58362317, -14124214, 85301793)
    }
    "compute difference of ms" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(MillisBetween), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601AcrossSlices"))),
        Root(line, CString("2010-09-23T18:33:22.520-10:00")))

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toLong
      }

      result2 must contain(114712508479L, -71409896910L, -45560737520L, -34389461903L, 35941265077L, 110558640261L, -64620189172L, 44039920965L, 28532980863L, 85301793775L, -46394099354L, -12449791417L, 19602785480L, 80425853610L, 37501386405L, 42695984162L, -59761902164L, -27770192599L, 73738905032L, -14124214357L, -12050411758L, -58362317732L)
    }
  }

  "time difference functions (heterogeneous case across slices)" should {
    "compute difference of years" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(YearsBetween), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/het/iso8601AcrossSlices"))),
        Root(line, CString("2010-09-23T18:33:22.520-10:00")))

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(0, 1, 2, 3, -1)
    }
    "compute difference of months" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(MonthsBetween), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/het/iso8601AcrossSlices"))),
        Root(line, CString("2010-09-23T18:33:22.520-10:00")))

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(-12, 38, -19, 13, 22, 27, -1, -9, 26)
    }
    "compute difference of weeks" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(WeeksBetween), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/het/iso8601AcrossSlices"))),
        Root(line, CString("2010-09-23T18:33:22.520-10:00")))

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(-54, -39, -8, 57, 121, 116, -4, 166, -84, 99)
    }
    "compute difference of days" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(DaysBetween), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/het/iso8601AcrossSlices"))),
        Root(line, CString("2010-09-23T18:33:22.520-10:00")))

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(-274, 849, -58, -588, 403, -384, 1167, -30, 699, 813)
    }
    "compute difference of hours" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(HoursBetween), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/het/iso8601AcrossSlices"))),
        Root(line, CString("2010-09-23T18:33:22.520-10:00")))

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toLong
      }

      result2 must contain(9672, -737, -14130, -9219, 20388, -1405, 19517, 16795, -6582, 28025)
    }
    "compute difference of minutes" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(MinutesBetween), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/het/iso8601AcrossSlices"))),
        Root(line, CString("2010-09-23T18:33:22.520-10:00")))

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toLong
      }

      result2 must contain(-394965, -847824, 1223285, -84316, 1007749, 580359, 1681543, 1171059, -44237, -553154)
    }
    "compute difference of seconds" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(SecondsBetween), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/het/iso8601AcrossSlices"))),
        Root(line, CString("2010-09-23T18:33:22.520-10:00")))

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toLong
      }

      result2 must contain(-2654273, 100892632, -5059008, -23697928, 73397157, 70263579, -50869487, 60464942, -33189258, 34821554)
    }
    "compute difference of ms" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(MillisBetween), CrossLeftSort,
        dag.LoadLocal(line, Root(line, CString("/het/iso8601AcrossSlices"))),
        Root(line, CString("2010-09-23T18:33:22.520-10:00")))

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toLong
      }

      result2 must contain(60464942676L, -50869487651L, 34821554007L, -2654273728L, 100892632209L, -5059008412L, -33189258109L, -23697928353L, 73397157662L, 70263579014L)
    }
  }

  "time extraction functions (homogeneous case across slices)" should {
    "extract time zone" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(TimeZone),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601AcrossSlices"))))

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d.toString
      }

      result2 must contain("-09:00", "-11:00", "-10:00", "+04:00", "-01:00", "+11:00", "-03:00", "+05:00", "-02:00", "-05:00", "+00:00", "+06:00", "-04:00", "+07:00")
    }

    "compute season" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(Season),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601AcrossSlices"))))

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d.toString
      }

      result2 must contain("spring", "winter", "summer")
    }

    "compute year" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(Year),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601AcrossSlices"))))

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(2010, 2007, 2011, 2012, 2009, 2008)
    }

    "compute quarter" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(QuarterOfYear),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601AcrossSlices"))))

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(1, 2, 3, 4)
    }

    "compute month of year" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(MonthOfYear),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601AcrossSlices"))))

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(5, 10, 1, 2, 12, 7, 3, 8)
    }

    "compute week of year" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(WeekOfYear),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601AcrossSlices"))))

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(5, 10, 52, 29, 6, 21, 33, 9, 41, 2, 32, 44, 12, 7, 18, 31, 11, 43)
    }
    "compute week of month" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(WeekOfMonth),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601AcrossSlices"))))

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(5, 1, 6, 2, 3, 4)
    }
    "compute day of year" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(DayOfYear),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601AcrossSlices"))))

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(138, 10, 46, 228, 216, 74, 302, 65, 285, 212, 41, 64, 144, 66, 198, 223, 35, 363, 40, 300, 122, 83)
    }
    "compute day of month" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(DayOfMonth),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601AcrossSlices"))))

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(10, 24, 14, 29, 6, 28, 9, 2, 17, 27, 18, 11, 23, 30, 4, 15)
    }
    "compute day of week" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(DayOfWeek),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601AcrossSlices"))))

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(5, 1, 6, 2, 7, 3, 4)
    }
    "compute hour of day" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(HourOfDay),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601AcrossSlices"))))

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(0, 10, 14, 1, 21, 13, 2, 17, 22, 12, 3, 18, 11, 19, 4)
    }
    "compute minute of hour" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(MinuteOfHour),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601AcrossSlices"))))

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(5, 56, 52, 14, 20, 29, 38, 33, 53, 2, 49, 48, 18, 31, 11, 43, 58, 36, 30, 19)
    }
    "compute second of minute" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(SecondOfMinute),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601AcrossSlices"))))

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(0, 56, 37, 14, 28, 38, 21, 53, 41, 34, 17, 22, 48, 16, 31, 40, 55, 19, 4)
    }
    "compute millis of second" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(MillisOfSecond),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601AcrossSlices"))))

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(555, 115, 443, 937, 877, 692, 910, 252, 874, 488, 41, 657, 430, 745, 423, 259, 278, 40, 119, 684, 358)
    }
  }

  "time extraction functions (heterogeneous case across slices)" should {
    "extract time zone" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(TimeZone),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601AcrossSlices"))))

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d.toString
      }

      result2 must contain("+08:00", "-07:00", "-11:00", "+04:00", "+10:00", "+03:00", "+02:00", "+00:00", "+06:00", "-04:00")
    }

    "compute season" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(Season),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601AcrossSlices"))))

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d.toString
      }

      result2 must contain("summer", "spring", "fall")
    }

    "compute year" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(Year),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601AcrossSlices"))))

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(2010, 2007, 2011, 2012, 2009, 2008)
    }

    "compute quarter" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(QuarterOfYear),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601AcrossSlices"))))

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(4, 2, 3)
    }

    "compute month of year" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(MonthOfYear),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601AcrossSlices"))))

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(5, 10, 6, 7, 11, 8)
    }

    "compute week of year" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(WeekOfYear),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601AcrossSlices"))))

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(25, 46, 28, 41, 34, 22, 27, 18, 43)
    }
    "compute week of month" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(WeekOfMonth),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601AcrossSlices"))))

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(3, 5, 1, 4)
    }
    "compute day of year" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(DayOfYear),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601AcrossSlices"))))

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(184, 325, 229, 298, 148, 176, 286, 126, 195)
    }
    "compute day of month" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(DayOfMonth),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601AcrossSlices"))))

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(5, 24, 25, 14, 21, 13, 2, 17, 27)
    }
    "compute day of week" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(DayOfWeek),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601AcrossSlices"))))

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(5, 1, 6, 2, 7, 3, 4)
    }
    "compute hour of day" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(HourOfDay),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601AcrossSlices"))))

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(0, 5, 1, 3, 18, 16, 11, 23, 8, 15)
    }
    "compute minute of hour" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(MinuteOfHour),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601AcrossSlices"))))

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(53, 44, 27, 54, 49, 18, 50, 58, 51, 47)
    }
    "compute second of minute" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(SecondOfMinute),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601AcrossSlices"))))

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(10, 24, 50, 16, 43, 40, 8, 30, 19)
    }
    "compute millis of second" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(MillisOfSecond),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601AcrossSlices"))))

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(629, 873, 248, 311, 513, 858, 932, 844, 171, 506)
    }
  }

  "time truncation functions (homogeneous case across slices)" should {
    "determine date" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(Date),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601AcrossSlices"))))

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }

      result2 must contain("2011-02-15", "2009-07-17", "2011-03-06", "2012-12-28", "2009-10-29", "2009-08-04", "2011-02-10", "2012-07-30", "2012-08-15", "2011-08-11", "2012-03-14", "2010-02-09", "2007-02-04", "2008-05-23", "2009-05-02", "2012-03-04", "2012-10-11", "2008-03-06", "2011-10-27", "2009-05-18", "2007-03-24", "2008-01-10")
    }
    "determine year and month" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(YearMonth),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601AcrossSlices"))))

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }

      result2 must contain("2010-02", "2012-08", "2007-02", "2012-12", "2008-03", "2009-08", "2012-07", "2012-03", "2007-03", "2011-03", "2009-07", "2011-10", "2011-02", "2008-05", "2012-10", "2011-08", "2008-01", "2009-05", "2009-10")
    }
    "determine year and day of year" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(YearDayOfYear),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601AcrossSlices"))))

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }

      result2 must contain("2008-144", "2011-223", "2009-302", "2012-363", "2010-040", "2009-122", "2008-010", "2011-065", "2012-074", "2007-083", "2012-285", "2012-228", "2012-212", "2011-300", "2009-138", "2008-066", "2012-064", "2009-198", "2011-041", "2011-046", "2007-035", "2009-216")
    }
    "determine month and day" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(MonthDay),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601AcrossSlices"))))

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }

      result2 must contain("03-14", "03-06", "05-18", "01-10", "10-29", "02-04", "02-15", "08-11", "07-30", "08-15", "10-27", "02-10", "05-23", "03-04", "12-28", "03-24", "02-09", "05-02", "10-11", "08-04", "07-17")
    }
    "determine date and hour" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(DateHour),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601AcrossSlices"))))

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }

      result2 must contain("2007-03-24T04", "2011-02-10T14", "2008-01-10T18", "2009-08-04T04", "2008-03-06T21", "2009-05-18T11", "2011-03-06T13", "2012-07-30T13", "2011-02-15T13", "2011-08-11T19", "2009-07-17T10", "2008-05-23T17", "2012-12-28T22", "2009-10-29T02", "2012-10-11T00", "2011-10-27T01", "2010-02-09T02", "2012-03-04T12", "2009-05-02T01", "2012-08-15T21", "2012-03-14T03", "2007-02-04T10")
    }
    "determine date, hour, and minute" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(DateHourMinute),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601AcrossSlices"))))

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }

      result2 must contain("2012-10-11T00:36", "2011-02-10T14:53", "2007-02-04T10:58", "2011-10-27T01:11", "2012-12-28T22:38", "2008-01-10T18:36", "2011-08-11T19:29", "2011-02-15T13:49", "2012-08-15T21:05", "2008-03-06T21:02", "2011-03-06T13:56", "2010-02-09T02:20", "2009-10-29T02:43", "2009-08-04T04:52", "2009-05-02T01:14", "2009-05-18T11:33", "2007-03-24T04:49", "2009-07-17T10:30", "2012-03-14T03:48", "2012-03-04T12:19", "2012-07-30T13:18", "2008-05-23T17:31")
    }
    "determine date, hour, minute, and second" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(DateHourMinuteSecond),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601AcrossSlices"))))

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }

      result2 must contain("2007-03-24T04:49:22", "2012-08-15T21:05:04", "2009-05-02T01:14:41", "2012-10-11T00:36:31", "2008-03-06T21:02:28", "2011-08-11T19:29:55", "2012-12-28T22:38:19", "2007-02-04T10:58:14", "2009-08-04T04:52:17", "2011-10-27T01:11:04", "2008-05-23T17:31:37", "2008-01-10T18:36:48", "2012-03-04T12:19:00", "2011-03-06T13:56:56", "2009-07-17T10:30:16", "2010-02-09T02:20:17", "2009-05-18T11:33:38", "2012-07-30T13:18:40", "2012-03-14T03:48:21", "2009-10-29T02:43:41", "2011-02-15T13:49:53", "2011-02-10T14:53:34")
    }
    "determine date, hour, minute, second, and ms" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(DateHourMinuteSecondMillis),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601AcrossSlices"))))

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }

      result2 must contain("2012-07-30T13:18:40.252", "2012-03-14T03:48:21.874", "2008-01-10T18:36:48.745", "2008-03-06T21:02:28.910", "2007-03-24T04:49:22.259", "2010-02-09T02:20:17.040", "2011-03-06T13:56:56.877", "2012-03-04T12:19:00.040", "2012-08-15T21:05:04.684", "2009-05-02T01:14:41.555", "2011-02-10T14:53:34.278", "2012-12-28T22:38:19.430", "2008-05-23T17:31:37.488", "2009-08-04T04:52:17.443", "2011-10-27T01:11:04.423", "2009-07-17T10:30:16.115", "2011-08-11T19:29:55.119", "2007-02-04T10:58:14.041", "2009-05-18T11:33:38.358", "2011-02-15T13:49:53.937", "2012-10-11T00:36:31.692", "2009-10-29T02:43:41.657")
    }
    "determine time with timezone" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(TimeWithZone),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601AcrossSlices"))))

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }

      result2 must contain("13:56:56.877-02:00", "21:02:28.910-11:00", "01:14:41.555-10:00", "22:38:19.430+06:00", "02:20:17.040-05:00", "12:19:00.040Z", "14:53:34.278-01:00", "13:49:53.937+07:00", "18:36:48.745-03:00", "04:49:22.259-09:00", "01:11:04.423-04:00", "13:18:40.252-03:00", "21:05:04.684Z", "04:52:17.443Z", "03:48:21.874Z", "11:33:38.358+11:00", "02:43:41.657+04:00", "00:36:31.692-02:00", "10:30:16.115+07:00", "17:31:37.488Z", "19:29:55.119+05:00", "10:58:14.041-01:00")
    }
    "determine time without timezone" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(TimeWithoutZone),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601AcrossSlices"))))

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }

      result2 must contain("02:43:41.657", "12:19:00.040", "13:56:56.877", "03:48:21.874", "04:49:22.259", "04:52:17.443", "00:36:31.692", "21:05:04.684", "02:20:17.040", "10:30:16.115", "17:31:37.488", "22:38:19.430", "13:49:53.937", "01:14:41.555", "13:18:40.252", "18:36:48.745", "10:58:14.041", "11:33:38.358", "14:53:34.278", "01:11:04.423", "21:02:28.910", "19:29:55.119")
    }
    "determine hour and minute" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(HourMinute),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601AcrossSlices"))))

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }

      result2 must contain("04:52", "12:19", "02:43", "21:05", "21:02", "22:38", "00:36", "13:18", "11:33", "03:48", "10:30", "19:29", "04:49", "01:11", "10:58", "14:53", "01:14", "18:36", "13:56", "17:31", "13:49", "02:20")
    }
    "determine hour, minute, and second" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(HourMinuteSecond),
        dag.LoadLocal(line, Root(line, CString("/hom/iso8601AcrossSlices"))))

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }

      result2 must contain("01:11:04", "10:58:14", "00:36:31", "10:30:16", "04:52:17", "13:56:56", "02:20:17", "12:19:00", "18:36:48", "17:31:37", "04:49:22", "11:33:38", "01:14:41", "02:43:41", "14:53:34", "22:38:19", "21:02:28", "21:05:04", "13:18:40", "19:29:55", "13:49:53", "03:48:21")
    }
  }

  "time truncation functions (heterogeneous case across slices)" should {
    "determine date" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(Date),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601AcrossSlices"))))

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }

      result2 must contain("2008-07-02", "2012-05-05", "2010-10-25", "2010-11-21", "2011-10-13", "2011-06-25", "2007-07-14", "2008-05-27", "2009-08-17", "2008-10-24")
    }
    "determine year and month" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(YearMonth),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601AcrossSlices"))))

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }

      result2 must contain("2010-11", "2008-07", "2008-10", "2009-08", "2011-06", "2011-10", "2008-05", "2007-07", "2010-10", "2012-05")
    }
    "determine year and day of year" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(YearDayOfYear),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601AcrossSlices"))))

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }

      result2 must contain("2008-148", "2008-184", "2007-195", "2009-229", "2011-286", "2010-298", "2008-298", "2012-126", "2010-325", "2011-176")
    }
    "determine month and day" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(MonthDay),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601AcrossSlices"))))

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }

      result2 must contain("10-25", "10-24", "10-13", "06-25", "05-27", "11-21", "07-14", "07-02", "05-05", "08-17")
    }
    "determine date and hour" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(DateHour),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601AcrossSlices"))))

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }

      result2 must contain("2008-05-27T16", "2012-05-05T08", "2008-07-02T18", "2010-11-21T23", "2009-08-17T05", "2011-10-13T15", "2010-10-25T01", "2011-06-25T00", "2008-10-24T11", "2007-07-14T03")
    }
    "determine date, hour, and minute" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(DateHourMinute),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601AcrossSlices"))))

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }

      result2 must contain("2007-07-14T03:49", "2010-10-25T01:51", "2011-06-25T00:18", "2008-07-02T18:53", "2008-05-27T16:27", "2012-05-05T08:58", "2010-11-21T23:50", "2009-08-17T05:54", "2011-10-13T15:47", "2008-10-24T11:44")
    }
    "determine date, hour, minute, and second" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(DateHourMinuteSecond),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601AcrossSlices"))))

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }

      result2 must contain("2008-10-24T11:44:19", "2008-07-02T18:53:43", "2007-07-14T03:49:30", "2010-10-25T01:51:16", "2008-05-27T16:27:24", "2011-06-25T00:18:50", "2012-05-05T08:58:10", "2009-08-17T05:54:08", "2010-11-21T23:50:10", "2011-10-13T15:47:40")
    }
    "determine date, hour, minute, second, and ms" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(DateHourMinuteSecondMillis),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601AcrossSlices"))))

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }

      result2 must contain("2010-11-21T23:50:10.932", "2008-07-02T18:53:43.506", "2008-10-24T11:44:19.844", "2007-07-14T03:49:30.311", "2008-05-27T16:27:24.858", "2010-10-25T01:51:16.248", "2011-10-13T15:47:40.629", "2012-05-05T08:58:10.171", "2011-06-25T00:18:50.873", "2009-08-17T05:54:08.513")
    }
    "determine time with timezone" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(TimeWithZone),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601AcrossSlices"))))

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }

      result2 must contain("23:50:10.932+06:00", "01:51:16.248+04:00", "15:47:40.629+08:00", "16:27:24.858Z", "03:49:30.311-07:00", "18:53:43.506-04:00", "08:58:10.171+10:00", "11:44:19.844+03:00", "00:18:50.873-11:00", "05:54:08.513+02:00")
    }
    "determine time without timezone" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(TimeWithoutZone),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601AcrossSlices"))))

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }

      result2 must contain("16:27:24.858", "23:50:10.932", "18:53:43.506", "11:44:19.844", "05:54:08.513", "15:47:40.629", "08:58:10.171", "03:49:30.311", "00:18:50.873", "01:51:16.248")
    }
    "determine hour and minute" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(HourMinute),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601AcrossSlices"))))

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }

      result2 must contain("16:27", "01:51", "18:53", "23:50", "05:54", "03:49", "00:18", "08:58", "11:44", "15:47")
    }
    "determine hour, minute, and second" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(HourMinuteSecond),
        dag.LoadLocal(line, Root(line, CString("/het/iso8601AcrossSlices"))))

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }

      result2 must contain("05:54:08", "01:51:16", "16:27:24", "11:44:19", "18:53:43", "08:58:10", "00:18:50", "23:50:10", "15:47:40", "03:49:30")
    }
  }
}

object TimeLibSpec extends TimeLibSpec[test.YId] with test.YIdInstances

// vim: set ts=4 sw=4 et:
