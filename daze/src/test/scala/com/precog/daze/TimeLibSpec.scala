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
import com.precog.yggdrasil.memoization._
import com.precog.common.Path
import scalaz._
import scalaz.effect._
import scalaz.iteratee._
import scalaz.std.list._
import Iteratee._

import com.precog.common.VectorCase
import com.precog.util.IdGen

import org.joda.time._
import org.joda.time.format._

trait TimeLibSpec[M[+_]] extends Specification
    with Evaluator[M]
    with TestConfigComponent[M] 
    with TimeLib[M] 
    with MemoryDatasetConsumer[M] { self =>
      
  import Function._
  
  import dag._
  import instructions._

  val testUID = "testUID"

  def testEval(graph: DepGraph): Set[SEvent] = withContext { ctx =>
    consumeEval(testUID, graph, ctx,Path.Root) match {
      case Success(results) => results
      case Failure(error) => throw error
    }
  }

  "all time functions" should {
    "validate input" in todo
    "return failing validations for bad input" in todo
  }

  "parse a time string into an ISO801 string, given its format" should {
    "time zone not specified" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(ParseDateTime), CrossLeftSort,
        Root(line, PushString("Jun 3, 2020 3:12:33 AM")),
        Root(line, PushString("MMM d, yyyy h:mm:ss a")))
        
      val result = testEval(input) collect {
        case (VectorCase(), SString(d)) => d.toString
      }

      result must haveSize(1)

      result must contain("2020-06-03T03:12:33.000Z")
    }

    "time zone specified" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(ParseDateTime), CrossLeftSort,
        Root(line, PushString("Jun 3, 2020 3:12:33 AM -08:00")),
        Root(line, PushString("MMM d, yyyy h:mm:ss a Z")))
        
      val result = testEval(input) collect {
        case (VectorCase(), SString(d)) => d.toString
      }

      result must haveSize(1)

      result must contain("2020-06-03T03:12:33.000-08:00")
    }

    "malformed string" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(ParseDateTime), CrossLeftSort,
        Root(line, PushString("Jun 3, 2020 3:12:33 AM -08:00 asteroid")),
        Root(line, PushString("MMM d, yyyy h:mm:ss a Z")))
        
      val result = testEval(input) collect {
        case (VectorCase(), SString(d)) => d.toString
      }
      
      result must beEmpty
    }

    "results used in another time function from homogeneous set" in {
      val line = Line(0, "")

      val input = dag.Operate(line, BuiltInFunction1Op(Date),
        Join(line, BuiltInFunction2Op(ParseDateTime), CrossLeftSort,
          dag.LoadLocal(line, Root(line, PushString("/hom/timeString"))),
          Root(line, PushString("MMM dd yyyy k:mm:ss.SSS"))))
        
      val result = testEval(input) collect {
        case (VectorCase(_), SString(d)) => d.toString
      }

      result must haveSize(4)

      result must contain("2010-06-03", "2010-06-04", "2011-08-12", "2010-10-09")
    }

    "from heterogeneous set" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(ParseDateTime), CrossLeftSort,
          dag.LoadLocal(line, Root(line, PushString("/het/timeString"))),
          Root(line, PushString("MMM dd yyyy k:mm:ss.SSS")))
        
      val result = testEval(input) collect {
        case (VectorCase(_), SString(d)) => d.toString
      }

      result must haveSize(4)

      result must contain("2010-06-03T04:12:33.323Z", "2010-06-04T13:31:49.002Z", "2011-08-12T22:42:33.310Z", "2010-10-09T09:27:31.953Z")
    }

    "ChangeTimeZone function with not fully formed string without tz" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(ChangeTimeZone), CrossLeftSort,
          Root(line, PushString("2010-06-04")),
          Root(line, PushString("-10:00")))
        
      val result = testEval(input) collect {
        case (VectorCase(), SString(d)) => d.toString
      }

      result must haveSize(1)

      result must contain("2010-06-03T14:00:00.000-10:00")
    }

    "ChangeTimeZone function with not fully formed string with tz" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(ChangeTimeZone), CrossLeftSort,
          Root(line, PushString("2010-06-04T+05:00")),
          Root(line, PushString("-10:00")))
        
      val result = testEval(input) collect {
        case (VectorCase(), SString(d)) => d.toString
      }

      result must haveSize(1)

      result must contain("2010-06-03T09:00:00.000-10:00")
    }

    "Plus function with not fully formed string without tz" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(MinutesPlus), CrossLeftSort,
          Root(line, PushString("2010-06-04T05:04:01")),
          Root(line, PushNum("10")))
        
      val result = testEval(input) collect {
        case (VectorCase(), SString(d)) => d.toString
      }

      result must haveSize(1)

      result must contain("2010-06-04T05:14:01.000Z")
    }

    "Plus function with not fully formed string with tz" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(MinutesPlus), CrossLeftSort,
          Root(line, PushString("2010-06-04T05:04:01.000+05:00")),
          Root(line, PushNum("10")))
        
      val result = testEval(input) collect {
        case (VectorCase(), SString(d)) => d.toString
      }

      result must haveSize(1)

      result must contain("2010-06-04T05:14:01.000+05:00")
    }

    "Between function with not fully formed string without tz" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(HoursBetween), CrossLeftSort,
          Root(line, PushString("2010-06-04T05:04:01")),
          Root(line, PushString("2010-06-04T07:04:01+00:00")))
        
      val result = testEval(input) collect {
        case (VectorCase(), SDecimal(d)) => d.toInt
      }

      result must haveSize(1)

      result must contain(2)
    }

    "Between function with not fully formed string with tz" in {
      val line = Line(0, "")

      val input = Join(line, BuiltInFunction2Op(HoursBetween), CrossLeftSort,
          Root(line, PushString("2010-06-04T05:04:01+05:00")),
          Root(line, PushString("2010-06-04T05:04:01+01:00")))
        
      val result = testEval(input) collect {
        case (VectorCase(), SDecimal(d)) => d.toInt
      }

      result must haveSize(1)

      result must contain(4)
    }

    "GetMillis function with not fully formed string without tz" in {
      val line = Line(0, "")

      val input = Operate(line, BuiltInFunction1Op(GetMillis),
          Root(line, PushString("2010-06-04T05")))
        
      val result = testEval(input) collect {
        case (VectorCase(), SDecimal(d)) => d.toLong
      }

      result must haveSize(1)

      result must contain(1275627600000L)
    }

    "GetMillis function with not fully formed string with tz" in {
      val line = Line(0, "")

      val input = Operate(line, BuiltInFunction1Op(GetMillis),
          Root(line, PushString("2010-06-04T03-02:00")))
        
      val result = testEval(input) collect {
        case (VectorCase(), SDecimal(d)) => d.toLong
      }

      result must haveSize(1)

      result must contain(1275627600000L)
    }

    "TimeZone function with not fully formed string without tz" in {
      val line = Line(0, "")

      val input = Operate(line, BuiltInFunction1Op(TimeZone),
          Root(line, PushString("2010-06-04T05")))
        
      val result = testEval(input) collect {
        case (VectorCase(), SString(d)) => d.toString
      }

      result must haveSize(1)

      result must contain("+00:00")
    }

    "TimeZone function with not fully formed string with tz" in {
      val line = Line(0, "")

      val input = Operate(line, BuiltInFunction1Op(TimeZone),
          Root(line, PushString("2010-06-04T03-02:00")))
        
      val result = testEval(input) collect {
        case (VectorCase(), SString(d)) => d.toString
      }

      result must haveSize(1)

      result must contain("-02:00")
    }

    "Season function with not fully formed string without tz" in {
      val line = Line(0, "")

      val input = Operate(line, BuiltInFunction1Op(Season),
          Root(line, PushString("2010-01-04")))
        
      val result = testEval(input) collect {
        case (VectorCase(), SString(d)) => d.toString
      }

      result must haveSize(1)

      result must contain("winter")
    }

    "Season function with not fully formed string with tz" in {
      val line = Line(0, "")

      val input = Operate(line, BuiltInFunction1Op(Season),
          Root(line, PushString("2010-01-04T-02:00")))
        
      val result = testEval(input) collect {
        case (VectorCase(), SString(d)) => d.toString
      }

      result must haveSize(1)

      result must contain("winter")
    }

    "TimeFraction function with not fully formed string without tz" in {
      val line = Line(0, "")

      val input = Operate(line, BuiltInFunction1Op(HourOfDay),
          Root(line, PushString("2010-01-04")))
        
      val result = testEval(input) collect {
        case (VectorCase(), SDecimal(d)) => d.toInt
      }

      result must haveSize(1)

      result must contain(0)
    }

    "TimeFraction function with not fully formed string with tz" in {
      val line = Line(0, "")

      val input = Operate(line, BuiltInFunction1Op(HourOfDay),
          Root(line, PushString("2010-01-04T03-02:00")))
        
      val result = testEval(input) collect {
        case (VectorCase(), SDecimal(d)) => d.toInt
      }

      result must haveSize(1)

      result must contain(3)
    }
  }
      
  "changing time zones (homogenous case)" should {
    "change to the correct time zone" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(ChangeTimeZone), CrossLeftSort,
        dag.LoadLocal(line, Root(line, PushString("/hom/iso8601"))),
        Root(line, PushString("-10:00")))
        
      val result = testEval(input) collect {
        case (VectorCase(_), SString(d)) => d.toString
      }
      
      result must contain("2011-02-21T01:09:59.165-10:00", "2012-02-11T06:11:33.394-10:00", "2011-09-06T06:44:52.848-10:00", "2010-04-28T15:37:52.599-10:00", "2012-12-28T06:38:19.430-10:00").only
    }

    "not modify millisecond value" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(ChangeTimeZone), CrossLeftSort,
        dag.LoadLocal(line, Root(line, PushString("/hom/iso8601"))),
        Root(line, PushString("-10:00")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SString(time)) => 
          val newTime = ISODateTimeFormat.dateTimeParser().withOffsetParsed.parseDateTime(time)
          newTime.getMillis.toLong
      }

      result2 must contain(1272505072599L, 1298286599165L, 1315327492848L, 1328976693394L, 1356712699430L)
    }

    "work correctly for fractional zones" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(ChangeTimeZone), CrossLeftSort,
        dag.LoadLocal(line, Root(line, PushString("/hom/iso8601"))),
        Root(line, PushString("-10:30")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SString(d)) => d.toString
      }
      
      result2 must contain("2011-02-21T00:39:59.165-10:30", "2012-02-11T05:41:33.394-10:30", "2011-09-06T06:14:52.848-10:30", "2010-04-28T15:07:52.599-10:30", "2012-12-28T06:08:19.430-10:30")
    }
  }

  "changing time zones (heterogeneous case)" should {
    "change to the correct time zone" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(ChangeTimeZone), CrossLeftSort,
        dag.LoadLocal(line, Root(line, PushString("/het/iso8601"))),
        Root(line, PushString("-10:00")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SString(d)) => d.toString
      }
      
      result2 must contain("2011-02-21T01:09:59.165-10:00", "2012-02-11T06:11:33.394-10:00", "2011-09-06T06:44:52.848-10:00", "2010-04-28T15:37:52.599-10:00", "2012-12-28T06:38:19.430-10:00")
    }

    "not modify millisecond value" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(ChangeTimeZone), CrossLeftSort,
        dag.LoadLocal(line, Root(line, PushString("/hom/iso8601"))),
        Root(line, PushString("-10:00")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SString(time)) => 
          val newTime = ISODateTimeFormat.dateTimeParser().withOffsetParsed.parseDateTime(time)
          newTime.getMillis.toLong
      }

      result2 must contain(1272505072599L, 1298286599165L, 1315327492848L, 1328976693394L, 1356712699430L)
    }

    "work correctly for fractional zones" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(ChangeTimeZone), CrossLeftSort,
        dag.LoadLocal(line, Root(line, PushString("/het/iso8601"))),
        Root(line, PushString("-10:30")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SString(d)) => d.toString
      }
      
      result2 must contain("2011-02-21T00:39:59.165-10:30", "2012-02-11T05:41:33.394-10:30", "2011-09-06T06:14:52.848-10:30", "2010-04-28T15:07:52.599-10:30", "2012-12-28T06:08:19.430-10:30")
    }
  }

  "converting an ISO time string to a millis value (homogeneous case)" should {
    "return the correct millis value" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(GetMillis),
        dag.LoadLocal(line, Root(line, PushString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toLong
      }
      
      result2 must contain(1272505072599L, 1315327492848L, 1328976693394L, 1356712699430L, 1298286599165L)
    }
  }

  "converting an ISO time string to a millis value (heterogeneous case)" should {
    "return the correct millis value" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(GetMillis),
        dag.LoadLocal(line, Root(line, PushString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toLong
      }
      
      result2 must contain(1272505072599L, 1315327492848L, 1328976693394L, 1356712699430L, 1298286599165L)
    }  
  }

 "converting a millis value to an ISO time string (homogeneous case)" should {
    "return the correct time string" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(MillisToISO), CrossLeftSort,
        dag.LoadLocal(line, Root(line, PushString("/hom/millisSinceEpoch"))),
        Root(line, PushString("-10:00")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SString(d)) => d.toString
      }
      
      result2 must contain("2012-02-28T06:44:52.420-10:00", "2012-02-18T06:44:52.780-10:00", "2012-02-21T08:28:42.774-10:00", "2012-02-25T08:01:27.710-10:00", "2012-02-18T06:44:52.854-10:00")      
    }

    "default to UTC if time zone is not specified" in todo

  }

  "converting a millis value to an ISO time string (heterogeneous set)" should {
    "return the correct time string" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(MillisToISO), CrossLeftSort,
        dag.LoadLocal(line, Root(line, PushString("/het/millisSinceEpoch"))),
        Root(line, PushString("-10:00")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SString(d)) => d.toString
      }
      
      result2 must contain("2012-02-28T06:44:52.420-10:00", "2012-02-18T06:44:52.780-10:00", "2012-02-21T08:28:42.774-10:00", "2012-02-25T08:01:27.710-10:00", "2012-02-18T06:44:52.854-10:00")      
    }

    "default to UTC if time zone is not specified" in todo

  }

  "time plus functions (homogeneous case)" should {
    "compute incrememtation of positive number of years" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(YearsPlus), CrossLeftSort,
        dag.LoadLocal(line, Root(line, PushString("/hom/iso8601"))),
        Root(line, PushNum("5")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SString(s)) => s
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
        dag.LoadLocal(line, Root(line, PushString("/hom/iso8601"))),
        Root(line, PushNum("-5")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SString(s)) => s
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
        dag.LoadLocal(line, Root(line, PushString("/hom/iso8601"))),
        Root(line, PushNum("0")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SString(s)) => s
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
        dag.LoadLocal(line, Root(line, PushString("/hom/iso8601"))),
        Root(line, PushNum("5")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SString(s)) => s
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
        dag.LoadLocal(line, Root(line, PushString("/hom/iso8601"))),
        Root(line, PushNum("5")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SString(s)) => s
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
        dag.LoadLocal(line, Root(line, PushString("/hom/iso8601"))),
        Root(line, PushNum("5")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SString(s)) => s
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
        dag.LoadLocal(line, Root(line, PushString("/hom/iso8601"))),
        Root(line, PushNum("5")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SString(s)) => s
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
        dag.LoadLocal(line, Root(line, PushString("/hom/iso8601"))),
        Root(line, PushNum("5")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SString(s)) => s
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
        dag.LoadLocal(line, Root(line, PushString("/hom/iso8601"))),
        Root(line, PushNum("5")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SString(s)) => s
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
        dag.LoadLocal(line, Root(line, PushString("/hom/iso8601"))),
        Root(line, PushNum("5")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SString(s)) => s
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
        dag.LoadLocal(line, Root(line, PushString("/het/iso8601"))),
        Root(line, PushNum("5")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SString(s)) => s
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
        dag.LoadLocal(line, Root(line, PushString("/het/iso8601"))),
        Root(line, PushNum("5")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SString(s)) => s
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
        dag.LoadLocal(line, Root(line, PushString("/het/iso8601"))),
        Root(line, PushNum("5")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SString(s)) => s
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
        dag.LoadLocal(line, Root(line, PushString("/het/iso8601"))),
        Root(line, PushNum("5")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SString(s)) => s
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
        dag.LoadLocal(line, Root(line, PushString("/het/iso8601"))),
        Root(line, PushNum("5")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SString(s)) => s
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
        dag.LoadLocal(line, Root(line, PushString("/het/iso8601"))),
        Root(line, PushNum("5")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SString(s)) => s
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
        dag.LoadLocal(line, Root(line, PushString("/het/iso8601"))),
        Root(line, PushNum("5")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SString(s)) => s
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
        dag.LoadLocal(line, Root(line, PushString("/het/iso8601"))),
        Root(line, PushNum("5")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SString(s)) => s
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
        dag.LoadLocal(line, Root(line, PushString("/hom/iso8601"))),
        Root(line, PushString("2010-09-23T18:33:22.520-10:00")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(-2, -1, 0)
    }
    "compute difference of months" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(MonthsBetween), CrossLeftSort,
        dag.LoadLocal(line, Root(line, PushString("/hom/iso8601"))),
        Root(line, PushString("2010-09-23T18:33:22.520-10:00")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(-16, -4, -27, 4, -11)
    }
    "compute difference of weeks" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(WeeksBetween), CrossLeftSort,
        dag.LoadLocal(line, Root(line, PushString("/hom/iso8601"))),
        Root(line, PushString("2010-09-23T18:33:22.520-10:00")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(-49, -118, -72, -21, 21)
    }
    "compute difference of days" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(DaysBetween), CrossLeftSort,
        dag.LoadLocal(line, Root(line, PushString("/hom/iso8601"))),
        Root(line, PushString("2010-09-23T18:33:22.520-10:00")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(-505, -347, 148, -826, -150)
    }
    "compute difference of hours" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(HoursBetween), CrossLeftSort,
        dag.LoadLocal(line, Root(line, PushString("/hom/iso8601"))),
        Root(line, PushString("2010-09-23T18:33:22.520-10:00")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toLong
      }
      
      result2 must contain(-12131, -3606, -19836, -8340, 3554)
    }
    "compute difference of minutes" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(MinutesBetween), CrossLeftSort,
        dag.LoadLocal(line, Root(line, PushString("/hom/iso8601"))),
        Root(line, PushString("2010-09-23T18:33:22.520-10:00")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toLong
      }
      
      result2 must contain(-727898, 213295, -216396, -500411, -1190164)
    }
    "compute difference of seconds" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(SecondsBetween), CrossLeftSort,
        dag.LoadLocal(line, Root(line, PushString("/hom/iso8601"))),
        Root(line, PushString("2010-09-23T18:33:22.520-10:00")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toLong
      }
      
      result2 must contain(-30024690, -43673890, -12983796, -71409896, 12797729)
    }
    "compute difference of ms" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(MillisBetween), CrossLeftSort,
        dag.LoadLocal(line, Root(line, PushString("/hom/iso8601"))),
        Root(line, PushString("2010-09-23T18:33:22.520-10:00")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toLong
      }
      
      result2 must contain(12797729921L, -12983796645L, -30024690328L, -43673890874L, -71409896910L)
    }
  }

  "time difference functions (heterogeneous case)" should {
    "compute difference of years" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(YearsBetween), CrossLeftSort,
        dag.LoadLocal(line, Root(line, PushString("/het/iso8601"))),
        Root(line, PushString("2010-09-23T18:33:22.520-10:00")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(-2, -1, 0)
    }
    "compute difference of months" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(MonthsBetween), CrossLeftSort,
        dag.LoadLocal(line, Root(line, PushString("/het/iso8601"))),
        Root(line, PushString("2010-09-23T18:33:22.520-10:00")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(-16, -4, -27, 4, -11)
    }
    "compute difference of weeks" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(WeeksBetween), CrossLeftSort,
        dag.LoadLocal(line, Root(line, PushString("/het/iso8601"))),
        Root(line, PushString("2010-09-23T18:33:22.520-10:00")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(-49, -118, -72, -21, 21)
    }
    "compute difference of days" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(DaysBetween), CrossLeftSort,
        dag.LoadLocal(line, Root(line, PushString("/het/iso8601"))),
        Root(line, PushString("2010-09-23T18:33:22.520-10:00")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(-505, -347, 148, -826, -150)
    }
    "compute difference of hours" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(HoursBetween), CrossLeftSort,
        dag.LoadLocal(line, Root(line, PushString("/het/iso8601"))),
        Root(line, PushString("2010-09-23T18:33:22.520-10:00")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toLong
      }
      
      result2 must contain(-12131, -3606, -19836, -8340, 3554)
    }
    "compute difference of minutes" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(MinutesBetween), CrossLeftSort,
        dag.LoadLocal(line, Root(line, PushString("/het/iso8601"))),
        Root(line, PushString("2010-09-23T18:33:22.520-10:00")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toLong
      }
      
      result2 must contain(-727898, 213295, -216396, -500411, -1190164)
    }
    "compute difference of seconds" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(SecondsBetween), CrossLeftSort,
        dag.LoadLocal(line, Root(line, PushString("/het/iso8601"))),
        Root(line, PushString("2010-09-23T18:33:22.520-10:00")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toLong
      }
      
      result2 must contain(-30024690, -43673890, -12983796, -71409896, 12797729)
    }
    "compute difference of ms" in {
      val line = Line(0, "")
      
      val input = Join(line, BuiltInFunction2Op(MillisBetween), CrossLeftSort,
        dag.LoadLocal(line, Root(line, PushString("/het/iso8601"))),
        Root(line, PushString("2010-09-23T18:33:22.520-10:00")))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toLong
      }
      
      result2 must contain(12797729921L, -12983796645L, -30024690328L, -43673890874L, -71409896910L)
    }
  }


  "time extraction functions (homogeneous case)" should {
    "extract time zone" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(TimeZone),
        dag.LoadLocal(line, Root(line, PushString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SString(d)) => d.toString
      }
      
      result2 must contain("+08:00", "+09:00", "-10:00", "-07:00", "+06:00")
    }     
  
    "compute season" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(Season),
        dag.LoadLocal(line, Root(line, PushString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SString(d)) => d.toString
      }
      
      result2 must contain("spring", "winter", "summer")
    }

    "compute year" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(Year),
        dag.LoadLocal(line, Root(line, PushString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(2010, 2011, 2012)
    }

    "compute quarter" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(QuarterOfYear),
        dag.LoadLocal(line, Root(line, PushString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(1, 2, 3, 4)
    }

    "compute month of year" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(MonthOfYear),
        dag.LoadLocal(line, Root(line, PushString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(4, 2, 9, 12)
    }
    
    "compute week of year" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(WeekOfYear),
        dag.LoadLocal(line, Root(line, PushString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(17, 8, 36, 6, 52)
    }
    "compute week of month" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(WeekOfMonth),
        dag.LoadLocal(line, Root(line, PushString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(2, 5, 4)
    }
    "compute day of year" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(DayOfYear),
        dag.LoadLocal(line, Root(line, PushString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(52, 119, 42, 249, 363)
    }
    "compute day of month" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(DayOfMonth),
        dag.LoadLocal(line, Root(line, PushString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }
        
      result2 must contain(21, 29, 11, 6, 28)
    }
    "compute day of week" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(DayOfWeek),
        dag.LoadLocal(line, Root(line, PushString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(1, 2, 6, 5, 4)
    }
    "compute hour of day" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(HourOfDay),
        dag.LoadLocal(line, Root(line, PushString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(20, 6, 9)
    }
    "compute minute of hour" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(MinuteOfHour),
        dag.LoadLocal(line, Root(line, PushString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(9, 44, 11, 37, 38)
    }
    "compute second of minute" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(SecondOfMinute),
        dag.LoadLocal(line, Root(line, PushString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(19, 59, 52, 33)
    }
    "compute millis of second" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(MillisOfSecond),
        dag.LoadLocal(line, Root(line, PushString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(430, 165, 848, 394, 599)
    }
  }

  "time extraction functions (heterogeneous case)" should {
    "extract time zone" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(TimeZone),
        dag.LoadLocal(line, Root(line, PushString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SString(d)) => d.toString
      }
      
      result2 must contain("+08:00", "+09:00", "-10:00", "-07:00", "+06:00")
    }     
  
    "compute season" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(Season),
        dag.LoadLocal(line, Root(line, PushString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SString(d)) => d.toString
      }
      
      result2 must contain("spring", "winter", "summer")
    }

    "compute year" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(Year),
        dag.LoadLocal(line, Root(line, PushString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(2010, 2011, 2012)
    }

    "compute quarter" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(QuarterOfYear),
        dag.LoadLocal(line, Root(line, PushString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(1, 2, 3, 4)
    }

    "compute month of year" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(MonthOfYear),
        dag.LoadLocal(line, Root(line, PushString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(4, 2, 9, 12)
    }
    
    "compute week of year" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(WeekOfYear),
        dag.LoadLocal(line, Root(line, PushString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(17, 8, 36, 6, 52)
    }
    "compute week of month" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(WeekOfMonth),
        dag.LoadLocal(line, Root(line, PushString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(2, 5, 4)
    }
    "compute day of year" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(DayOfYear),
        dag.LoadLocal(line, Root(line, PushString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(52, 119, 42, 249, 363)
    }
    "compute day of month" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(DayOfMonth),
        dag.LoadLocal(line, Root(line, PushString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }
        
      result2 must contain(21, 29, 11, 6, 28)
    }
    "compute day of week" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(DayOfWeek),
        dag.LoadLocal(line, Root(line, PushString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(1, 2, 6, 5, 4)
    }
    "compute hour of day" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(HourOfDay),
        dag.LoadLocal(line, Root(line, PushString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(20, 6, 9)
    }
    "compute minute of hour" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(MinuteOfHour),
        dag.LoadLocal(line, Root(line, PushString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(9, 44, 11, 37, 38)
    }
    "compute second of minute" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(SecondOfMinute),
        dag.LoadLocal(line, Root(line, PushString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(19, 59, 52, 33)
    }
    "compute millis of second" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(MillisOfSecond),
        dag.LoadLocal(line, Root(line, PushString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SDecimal(d)) => d.toInt
      }
      
      result2 must contain(430, 165, 848, 394, 599)
    }
  }

  "time truncation functions (homogeneous case)" should {
    "determine date" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(Date),
        dag.LoadLocal(line, Root(line, PushString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SString(d)) => d
      }
      
      result2 must contain("2010-04-29","2011-02-21","2011-09-06","2012-02-11","2012-12-28")
    }
    "determine year and month" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(YearMonth),
        dag.LoadLocal(line, Root(line, PushString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SString(d)) => d
      }
      
      result2 must contain("2010-04","2011-02","2011-09","2012-02","2012-12")
    }
    "determine year and day of year" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(YearDayOfYear),
        dag.LoadLocal(line, Root(line, PushString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SString(d)) => d
      }
      
      result2 must contain("2012-363", "2011-249", "2012-042", "2010-119", "2011-052")
    }
    "determine month and day" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(MonthDay),
        dag.LoadLocal(line, Root(line, PushString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SString(d)) => d
      }
      
      result2 must contain("04-29","02-21","09-06","02-11","12-28")
    }
    "determine date and hour" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(DateHour),
        dag.LoadLocal(line, Root(line, PushString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SString(d)) => d
      }
      
      result2 must contain("2010-04-29T09","2011-02-21T20","2011-09-06T06","2012-02-11T09","2012-12-28T22")
    }
    "determine date, hour, and minute" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(DateHourMinute),
        dag.LoadLocal(line, Root(line, PushString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SString(d)) => d
      }
      
      result2 must contain("2010-04-29T09:37","2011-02-21T20:09","2011-09-06T06:44","2012-02-11T09:11","2012-12-28T22:38")
    }
    "determine date, hour, minute, and second" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(DateHourMinuteSecond),
        dag.LoadLocal(line, Root(line, PushString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SString(d)) => d
      }
      
      result2 must contain("2010-04-29T09:37:52","2011-02-21T20:09:59","2011-09-06T06:44:52","2012-02-11T09:11:33","2012-12-28T22:38:19")
    }
    "determine date, hour, minute, second, and ms" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(DateHourMinuteSecondMillis),
        dag.LoadLocal(line, Root(line, PushString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SString(d)) => d
      }
      
      result2 must contain("2010-04-29T09:37:52.599","2011-02-21T20:09:59.165","2011-09-06T06:44:52.848","2012-02-11T09:11:33.394","2012-12-28T22:38:19.430")
    }
    "determine time with timezone" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(TimeWithZone),
        dag.LoadLocal(line, Root(line, PushString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SString(d)) => d
      }
      
      result2 must contain("09:37:52.599+08:00","20:09:59.165+09:00","06:44:52.848-10:00","09:11:33.394-07:00","22:38:19.430+06:00")
    }
    "determine time without timezone" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(TimeWithoutZone),
        dag.LoadLocal(line, Root(line, PushString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SString(d)) => d
      }
      
      result2 must contain("09:37:52.599","20:09:59.165","06:44:52.848","09:11:33.394","22:38:19.430")
    }
    "determine hour and minute" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(HourMinute),
        dag.LoadLocal(line, Root(line, PushString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SString(d)) => d
      }
      
      result2 must contain("09:37","20:09","06:44","09:11","22:38")
    }
    "determine hour, minute, and second" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(HourMinuteSecond),
        dag.LoadLocal(line, Root(line, PushString("/hom/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SString(d)) => d
      }
      
      result2 must contain("09:37:52","20:09:59","06:44:52","09:11:33","22:38:19")
    }
  }

  "time truncation functions (heterogeneous case)" should {
    "determine date" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(Date),
        dag.LoadLocal(line, Root(line, PushString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SString(d)) => d
      }
      
      result2 must contain("2010-04-29","2011-02-21","2011-09-06","2012-02-11","2012-12-28")
    }
    "determine year and month" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(YearMonth),
        dag.LoadLocal(line, Root(line, PushString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SString(d)) => d
      }
      
      result2 must contain("2010-04","2011-02","2011-09","2012-02","2012-12")
    }
    "determine year and day of year" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(YearDayOfYear),
        dag.LoadLocal(line, Root(line, PushString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SString(d)) => d
      }
      
      result2 must contain("2012-363", "2011-249", "2012-042", "2010-119", "2011-052")
    }
    "determine month and day" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(MonthDay),
        dag.LoadLocal(line, Root(line, PushString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SString(d)) => d
      }
      
      result2 must contain("04-29","02-21","09-06","02-11","12-28")
    }
    "determine date and hour" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(DateHour),
        dag.LoadLocal(line, Root(line, PushString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SString(d)) => d
      }
      
      result2 must contain("2010-04-29T09","2011-02-21T20","2011-09-06T06","2012-02-11T09","2012-12-28T22")
    }
    "determine date, hour, and minute" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(DateHourMinute),
        dag.LoadLocal(line, Root(line, PushString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SString(d)) => d
      }
      
      result2 must contain("2010-04-29T09:37","2011-02-21T20:09","2011-09-06T06:44","2012-02-11T09:11","2012-12-28T22:38")
    }
    "determine date, hour, minute, and second" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(DateHourMinuteSecond),
        dag.LoadLocal(line, Root(line, PushString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SString(d)) => d
      }
      
      result2 must contain("2010-04-29T09:37:52","2011-02-21T20:09:59","2011-09-06T06:44:52","2012-02-11T09:11:33","2012-12-28T22:38:19")
    }
    "determine date, hour, minute, second, and ms" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(DateHourMinuteSecondMillis),
        dag.LoadLocal(line, Root(line, PushString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SString(d)) => d
      }
      
      result2 must contain("2010-04-29T09:37:52.599","2011-02-21T20:09:59.165","2011-09-06T06:44:52.848","2012-02-11T09:11:33.394","2012-12-28T22:38:19.430")
    }
    "determine time with timezone" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(TimeWithZone),
        dag.LoadLocal(line, Root(line, PushString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SString(d)) => d
      }
      
      result2 must contain("09:37:52.599+08:00","20:09:59.165+09:00","06:44:52.848-10:00","09:11:33.394-07:00","22:38:19.430+06:00")
    }
    "determine time without timezone" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(TimeWithoutZone),
        dag.LoadLocal(line, Root(line, PushString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SString(d)) => d
      }
      
      result2 must contain("09:37:52.599","20:09:59.165","06:44:52.848","09:11:33.394","22:38:19.430")
    }
    "determine hour and minute" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(HourMinute),
        dag.LoadLocal(line, Root(line, PushString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SString(d)) => d
      }
      
      result2 must contain("09:37","20:09","06:44","09:11","22:38")
    }
    "determine hour, minute, and second" in {
      val line = Line(0, "")
      
      val input = dag.Operate(line, BuiltInFunction1Op(HourMinuteSecond),
        dag.LoadLocal(line, Root(line, PushString("/het/iso8601"))))
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (VectorCase(_), SString(d)) => d
      }
      
      result2 must contain("09:37:52","20:09:59","06:44:52","09:11:33","22:38:19")
    }
  }
}

object TimeLibSpec extends TimeLibSpec[test.YId] with test.YIdInstances {
  object Table extends TableCompanion {
    val geq: scalaz.Equal[GroupId] = scalaz.std.anyVal.intInstance
  }
}
// vim: set ts=4 sw=4 et:
