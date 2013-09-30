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
package com.precog.mimir

import org.specs2.mutable._

import com.precog.common._
import com.precog.yggdrasil._
import com.precog.common.Path
import scalaz._
import scalaz.std.list._

import com.precog.util.IdGen

import org.joda.time._
import org.joda.time.format._

trait TimeTruncationSpecs[M[+_]] extends Specification
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

  "time truncation functions (homogeneous case)" should {
    "determine date" in {
      val input = dag.Operate(BuiltInFunction1Op(Date),
        dag.AbsoluteLoad(Const(CString("/hom/iso8601"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("2010-04-29","2011-02-21","2011-09-06","2012-02-11","2012-12-28")
    }
    "determine year and month" in {
      val input = dag.Operate(BuiltInFunction1Op(YearMonth),
        dag.AbsoluteLoad(Const(CString("/hom/iso8601"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("2010-04","2011-02","2011-09","2012-02","2012-12")
    }
    "determine year and day of year" in {
      val input = dag.Operate(BuiltInFunction1Op(YearDayOfYear),
        dag.AbsoluteLoad(Const(CString("/hom/iso8601"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("2012-363", "2011-249", "2012-042", "2010-119", "2011-052")
    }
    "determine month and day" in {
      val input = dag.Operate(BuiltInFunction1Op(MonthDay),
        dag.AbsoluteLoad(Const(CString("/hom/iso8601"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("04-29","02-21","09-06","02-11","12-28")
    }
    "determine date and hour" in {
      val input = dag.Operate(BuiltInFunction1Op(DateHour),
        dag.AbsoluteLoad(Const(CString("/hom/iso8601"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("2010-04-29T09","2011-02-21T20","2011-09-06T06","2012-02-11T09","2012-12-28T22")
    }
    "determine date, hour, and minute" in {
      val input = dag.Operate(BuiltInFunction1Op(DateHourMinute),
        dag.AbsoluteLoad(Const(CString("/hom/iso8601"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("2010-04-29T09:37","2011-02-21T20:09","2011-09-06T06:44","2012-02-11T09:11","2012-12-28T22:38")
    }
    "determine date, hour, minute, and second" in {
      val input = dag.Operate(BuiltInFunction1Op(DateHourMinuteSecond),
        dag.AbsoluteLoad(Const(CString("/hom/iso8601"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("2010-04-29T09:37:52","2011-02-21T20:09:59","2011-09-06T06:44:52","2012-02-11T09:11:33","2012-12-28T22:38:19")
    }
    "determine date, hour, minute, second, and ms" in {
      val input = dag.Operate(BuiltInFunction1Op(DateHourMinuteSecondMillis),
        dag.AbsoluteLoad(Const(CString("/hom/iso8601"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("2010-04-29T09:37:52.599","2011-02-21T20:09:59.165","2011-09-06T06:44:52.848","2012-02-11T09:11:33.394","2012-12-28T22:38:19.430")
    }
    "determine time with timezone" in {
      val input = dag.Operate(BuiltInFunction1Op(TimeWithZone),
        dag.AbsoluteLoad(Const(CString("/hom/iso8601"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("09:37:52.599+08:00","20:09:59.165+09:00","06:44:52.848-10:00","09:11:33.394-07:00","22:38:19.430+06:00")
    }
    "determine time without timezone" in {
      val input = dag.Operate(BuiltInFunction1Op(TimeWithoutZone),
        dag.AbsoluteLoad(Const(CString("/hom/iso8601"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("09:37:52.599","20:09:59.165","06:44:52.848","09:11:33.394","22:38:19.430")
    }
    "determine hour and minute" in {
      val input = dag.Operate(BuiltInFunction1Op(HourMinute),
        dag.AbsoluteLoad(Const(CString("/hom/iso8601"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("09:37","20:09","06:44","09:11","22:38")
    }
    "determine hour, minute, and second" in {
      val input = dag.Operate(BuiltInFunction1Op(HourMinuteSecond),
        dag.AbsoluteLoad(Const(CString("/hom/iso8601"))(line))(line))(line)
        
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
      val input = dag.Operate(BuiltInFunction1Op(Date),
        dag.AbsoluteLoad(Const(CString("/het/iso8601"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("2010-04-29","2011-02-21","2011-09-06","2012-02-11","2012-12-28")
    }
    "determine year and month" in {
      val input = dag.Operate(BuiltInFunction1Op(YearMonth),
        dag.AbsoluteLoad(Const(CString("/het/iso8601"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("2010-04","2011-02","2011-09","2012-02","2012-12")
    }
    "determine year and day of year" in {
      val input = dag.Operate(BuiltInFunction1Op(YearDayOfYear),
        dag.AbsoluteLoad(Const(CString("/het/iso8601"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("2012-363", "2011-249", "2012-042", "2010-119", "2011-052")
    }
    "determine month and day" in {
      val input = dag.Operate(BuiltInFunction1Op(MonthDay),
        dag.AbsoluteLoad(Const(CString("/het/iso8601"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("04-29","02-21","09-06","02-11","12-28")
    }
    "determine date and hour" in {
      val input = dag.Operate(BuiltInFunction1Op(DateHour),
        dag.AbsoluteLoad(Const(CString("/het/iso8601"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("2010-04-29T09","2011-02-21T20","2011-09-06T06","2012-02-11T09","2012-12-28T22")
    }
    "determine date, hour, and minute" in {
      val input = dag.Operate(BuiltInFunction1Op(DateHourMinute),
        dag.AbsoluteLoad(Const(CString("/het/iso8601"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("2010-04-29T09:37","2011-02-21T20:09","2011-09-06T06:44","2012-02-11T09:11","2012-12-28T22:38")
    }
    "determine date, hour, minute, and second" in {
      val input = dag.Operate(BuiltInFunction1Op(DateHourMinuteSecond),
        dag.AbsoluteLoad(Const(CString("/het/iso8601"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("2010-04-29T09:37:52","2011-02-21T20:09:59","2011-09-06T06:44:52","2012-02-11T09:11:33","2012-12-28T22:38:19")
    }
    "determine date, hour, minute, second, and ms" in {
      val input = dag.Operate(BuiltInFunction1Op(DateHourMinuteSecondMillis),
        dag.AbsoluteLoad(Const(CString("/het/iso8601"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("2010-04-29T09:37:52.599","2011-02-21T20:09:59.165","2011-09-06T06:44:52.848","2012-02-11T09:11:33.394","2012-12-28T22:38:19.430")
    }
    "determine time with timezone" in {
      val input = dag.Operate(BuiltInFunction1Op(TimeWithZone),
        dag.AbsoluteLoad(Const(CString("/het/iso8601"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("09:37:52.599+08:00","20:09:59.165+09:00","06:44:52.848-10:00","09:11:33.394-07:00","22:38:19.430+06:00")
    }
    "determine time without timezone" in {
      val input = dag.Operate(BuiltInFunction1Op(TimeWithoutZone),
        dag.AbsoluteLoad(Const(CString("/het/iso8601"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("09:37:52.599","20:09:59.165","06:44:52.848","09:11:33.394","22:38:19.430")
    }
    "determine hour and minute" in {
      val input = dag.Operate(BuiltInFunction1Op(HourMinute),
        dag.AbsoluteLoad(Const(CString("/het/iso8601"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("09:37","20:09","06:44","09:11","22:38")
    }
    "determine hour, minute, and second" in {
      val input = dag.Operate(BuiltInFunction1Op(HourMinuteSecond),
        dag.AbsoluteLoad(Const(CString("/het/iso8601"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }
      
      result2 must contain("09:37:52","20:09:59","06:44:52","09:11:33","22:38:19")
    }
  }

  "time truncation functions (homogeneous case across slices)" should {
    "determine date" in {
      val input = dag.Operate(BuiltInFunction1Op(Date),
        dag.AbsoluteLoad(Const(CString("/hom/iso8601AcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }

      result2 must contain("2011-02-15", "2009-07-17", "2011-03-06", "2012-12-28", "2009-10-29", "2009-08-04", "2011-02-10", "2012-07-30", "2012-08-15", "2011-08-11", "2012-03-14", "2010-02-09", "2007-02-04", "2008-05-23", "2009-05-02", "2012-03-04", "2012-10-11", "2008-03-06", "2011-10-27", "2009-05-18", "2007-03-24", "2008-01-10")
    }
    "determine year and month" in {
      val input = dag.Operate(BuiltInFunction1Op(YearMonth),
        dag.AbsoluteLoad(Const(CString("/hom/iso8601AcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }

      result2 must contain("2010-02", "2012-08", "2007-02", "2012-12", "2008-03", "2009-08", "2012-07", "2012-03", "2007-03", "2011-03", "2009-07", "2011-10", "2011-02", "2008-05", "2012-10", "2011-08", "2008-01", "2009-05", "2009-10")
    }
    "determine year and day of year" in {
      val input = dag.Operate(BuiltInFunction1Op(YearDayOfYear),
        dag.AbsoluteLoad(Const(CString("/hom/iso8601AcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }

      result2 must contain("2008-144", "2011-223", "2009-302", "2012-363", "2010-040", "2009-122", "2008-010", "2011-065", "2012-074", "2007-083", "2012-285", "2012-228", "2012-212", "2011-300", "2009-138", "2008-066", "2012-064", "2009-198", "2011-041", "2011-046", "2007-035", "2009-216")
    }
    "determine month and day" in {
      val input = dag.Operate(BuiltInFunction1Op(MonthDay),
        dag.AbsoluteLoad(Const(CString("/hom/iso8601AcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }

      result2 must contain("03-14", "03-06", "05-18", "01-10", "10-29", "02-04", "02-15", "08-11", "07-30", "08-15", "10-27", "02-10", "05-23", "03-04", "12-28", "03-24", "02-09", "05-02", "10-11", "08-04", "07-17")
    }
    "determine date and hour" in {
      val input = dag.Operate(BuiltInFunction1Op(DateHour),
        dag.AbsoluteLoad(Const(CString("/hom/iso8601AcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }

      result2 must contain("2007-03-24T04", "2011-02-10T14", "2008-01-10T18", "2009-08-04T04", "2008-03-06T21", "2009-05-18T11", "2011-03-06T13", "2012-07-30T13", "2011-02-15T13", "2011-08-11T19", "2009-07-17T10", "2008-05-23T17", "2012-12-28T22", "2009-10-29T02", "2012-10-11T00", "2011-10-27T01", "2010-02-09T02", "2012-03-04T12", "2009-05-02T01", "2012-08-15T21", "2012-03-14T03", "2007-02-04T10")
    }
    "determine date, hour, and minute" in {
      val input = dag.Operate(BuiltInFunction1Op(DateHourMinute),
        dag.AbsoluteLoad(Const(CString("/hom/iso8601AcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }

      result2 must contain("2012-10-11T00:36", "2011-02-10T14:53", "2007-02-04T10:58", "2011-10-27T01:11", "2012-12-28T22:38", "2008-01-10T18:36", "2011-08-11T19:29", "2011-02-15T13:49", "2012-08-15T21:05", "2008-03-06T21:02", "2011-03-06T13:56", "2010-02-09T02:20", "2009-10-29T02:43", "2009-08-04T04:52", "2009-05-02T01:14", "2009-05-18T11:33", "2007-03-24T04:49", "2009-07-17T10:30", "2012-03-14T03:48", "2012-03-04T12:19", "2012-07-30T13:18", "2008-05-23T17:31")
    }
    "determine date, hour, minute, and second" in {
      val input = dag.Operate(BuiltInFunction1Op(DateHourMinuteSecond),
        dag.AbsoluteLoad(Const(CString("/hom/iso8601AcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }

      result2 must contain("2007-03-24T04:49:22", "2012-08-15T21:05:04", "2009-05-02T01:14:41", "2012-10-11T00:36:31", "2008-03-06T21:02:28", "2011-08-11T19:29:55", "2012-12-28T22:38:19", "2007-02-04T10:58:14", "2009-08-04T04:52:17", "2011-10-27T01:11:04", "2008-05-23T17:31:37", "2008-01-10T18:36:48", "2012-03-04T12:19:00", "2011-03-06T13:56:56", "2009-07-17T10:30:16", "2010-02-09T02:20:17", "2009-05-18T11:33:38", "2012-07-30T13:18:40", "2012-03-14T03:48:21", "2009-10-29T02:43:41", "2011-02-15T13:49:53", "2011-02-10T14:53:34")
    }
    "determine date, hour, minute, second, and ms" in {
      val input = dag.Operate(BuiltInFunction1Op(DateHourMinuteSecondMillis),
        dag.AbsoluteLoad(Const(CString("/hom/iso8601AcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }

      result2 must contain("2012-07-30T13:18:40.252", "2012-03-14T03:48:21.874", "2008-01-10T18:36:48.745", "2008-03-06T21:02:28.910", "2007-03-24T04:49:22.259", "2010-02-09T02:20:17.040", "2011-03-06T13:56:56.877", "2012-03-04T12:19:00.040", "2012-08-15T21:05:04.684", "2009-05-02T01:14:41.555", "2011-02-10T14:53:34.278", "2012-12-28T22:38:19.430", "2008-05-23T17:31:37.488", "2009-08-04T04:52:17.443", "2011-10-27T01:11:04.423", "2009-07-17T10:30:16.115", "2011-08-11T19:29:55.119", "2007-02-04T10:58:14.041", "2009-05-18T11:33:38.358", "2011-02-15T13:49:53.937", "2012-10-11T00:36:31.692", "2009-10-29T02:43:41.657")
    }
    "determine time with timezone" in {
      val input = dag.Operate(BuiltInFunction1Op(TimeWithZone),
        dag.AbsoluteLoad(Const(CString("/hom/iso8601AcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }

      result2 must contain("13:56:56.877-02:00", "21:02:28.910-11:00", "01:14:41.555-10:00", "22:38:19.430+06:00", "02:20:17.040-05:00", "12:19:00.040Z", "14:53:34.278-01:00", "13:49:53.937+07:00", "18:36:48.745-03:00", "04:49:22.259-09:00", "01:11:04.423-04:00", "13:18:40.252-03:00", "21:05:04.684Z", "04:52:17.443Z", "03:48:21.874Z", "11:33:38.358+11:00", "02:43:41.657+04:00", "00:36:31.692-02:00", "10:30:16.115+07:00", "17:31:37.488Z", "19:29:55.119+05:00", "10:58:14.041-01:00")
    }
    "determine time without timezone" in {
      val input = dag.Operate(BuiltInFunction1Op(TimeWithoutZone),
        dag.AbsoluteLoad(Const(CString("/hom/iso8601AcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }

      result2 must contain("02:43:41.657", "12:19:00.040", "13:56:56.877", "03:48:21.874", "04:49:22.259", "04:52:17.443", "00:36:31.692", "21:05:04.684", "02:20:17.040", "10:30:16.115", "17:31:37.488", "22:38:19.430", "13:49:53.937", "01:14:41.555", "13:18:40.252", "18:36:48.745", "10:58:14.041", "11:33:38.358", "14:53:34.278", "01:11:04.423", "21:02:28.910", "19:29:55.119")
    }
    "determine hour and minute" in {
      val input = dag.Operate(BuiltInFunction1Op(HourMinute),
        dag.AbsoluteLoad(Const(CString("/hom/iso8601AcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }

      result2 must contain("04:52", "12:19", "02:43", "21:05", "21:02", "22:38", "00:36", "13:18", "11:33", "03:48", "10:30", "19:29", "04:49", "01:11", "10:58", "14:53", "01:14", "18:36", "13:56", "17:31", "13:49", "02:20")
    }
    "determine hour, minute, and second" in {
      val input = dag.Operate(BuiltInFunction1Op(HourMinuteSecond),
        dag.AbsoluteLoad(Const(CString("/hom/iso8601AcrossSlices"))(line))(line))(line)

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
      val input = dag.Operate(BuiltInFunction1Op(Date),
        dag.AbsoluteLoad(Const(CString("/het/iso8601AcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }

      result2 must contain("2008-07-02", "2012-05-05", "2010-10-25", "2010-11-21", "2011-10-13", "2011-06-25", "2007-07-14", "2008-05-27", "2009-08-17", "2008-10-24")
    }
    "determine year and month" in {
      val input = dag.Operate(BuiltInFunction1Op(YearMonth),
        dag.AbsoluteLoad(Const(CString("/het/iso8601AcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }

      result2 must contain("2010-11", "2008-07", "2008-10", "2009-08", "2011-06", "2011-10", "2008-05", "2007-07", "2010-10", "2012-05")
    }
    "determine year and day of year" in {
      val input = dag.Operate(BuiltInFunction1Op(YearDayOfYear),
        dag.AbsoluteLoad(Const(CString("/het/iso8601AcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }

      result2 must contain("2008-148", "2008-184", "2007-195", "2009-229", "2011-286", "2010-298", "2008-298", "2012-126", "2010-325", "2011-176")
    }
    "determine month and day" in {
      val input = dag.Operate(BuiltInFunction1Op(MonthDay),
        dag.AbsoluteLoad(Const(CString("/het/iso8601AcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }

      result2 must contain("10-25", "10-24", "10-13", "06-25", "05-27", "11-21", "07-14", "07-02", "05-05", "08-17")
    }
    "determine date and hour" in {
      val input = dag.Operate(BuiltInFunction1Op(DateHour),
        dag.AbsoluteLoad(Const(CString("/het/iso8601AcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }

      result2 must contain("2008-05-27T16", "2012-05-05T08", "2008-07-02T18", "2010-11-21T23", "2009-08-17T05", "2011-10-13T15", "2010-10-25T01", "2011-06-25T00", "2008-10-24T11", "2007-07-14T03")
    }
    "determine date, hour, and minute" in {
      val input = dag.Operate(BuiltInFunction1Op(DateHourMinute),
        dag.AbsoluteLoad(Const(CString("/het/iso8601AcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }

      result2 must contain("2007-07-14T03:49", "2010-10-25T01:51", "2011-06-25T00:18", "2008-07-02T18:53", "2008-05-27T16:27", "2012-05-05T08:58", "2010-11-21T23:50", "2009-08-17T05:54", "2011-10-13T15:47", "2008-10-24T11:44")
    }
    "determine date, hour, minute, and second" in {
      val input = dag.Operate(BuiltInFunction1Op(DateHourMinuteSecond),
        dag.AbsoluteLoad(Const(CString("/het/iso8601AcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }

      result2 must contain("2008-10-24T11:44:19", "2008-07-02T18:53:43", "2007-07-14T03:49:30", "2010-10-25T01:51:16", "2008-05-27T16:27:24", "2011-06-25T00:18:50", "2012-05-05T08:58:10", "2009-08-17T05:54:08", "2010-11-21T23:50:10", "2011-10-13T15:47:40")
    }
    "determine date, hour, minute, second, and ms" in {
      val input = dag.Operate(BuiltInFunction1Op(DateHourMinuteSecondMillis),
        dag.AbsoluteLoad(Const(CString("/het/iso8601AcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }

      result2 must contain("2010-11-21T23:50:10.932", "2008-07-02T18:53:43.506", "2008-10-24T11:44:19.844", "2007-07-14T03:49:30.311", "2008-05-27T16:27:24.858", "2010-10-25T01:51:16.248", "2011-10-13T15:47:40.629", "2012-05-05T08:58:10.171", "2011-06-25T00:18:50.873", "2009-08-17T05:54:08.513")
    }
    "determine time with timezone" in {
      val input = dag.Operate(BuiltInFunction1Op(TimeWithZone),
        dag.AbsoluteLoad(Const(CString("/het/iso8601AcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }

      result2 must contain("23:50:10.932+06:00", "01:51:16.248+04:00", "15:47:40.629+08:00", "16:27:24.858Z", "03:49:30.311-07:00", "18:53:43.506-04:00", "08:58:10.171+10:00", "11:44:19.844+03:00", "00:18:50.873-11:00", "05:54:08.513+02:00")
    }
    "determine time without timezone" in {
      val input = dag.Operate(BuiltInFunction1Op(TimeWithoutZone),
        dag.AbsoluteLoad(Const(CString("/het/iso8601AcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }

      result2 must contain("16:27:24.858", "23:50:10.932", "18:53:43.506", "11:44:19.844", "05:54:08.513", "15:47:40.629", "08:58:10.171", "03:49:30.311", "00:18:50.873", "01:51:16.248")
    }
    "determine hour and minute" in {
      val input = dag.Operate(BuiltInFunction1Op(HourMinute),
        dag.AbsoluteLoad(Const(CString("/het/iso8601AcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }

      result2 must contain("16:27", "01:51", "18:53", "23:50", "05:54", "03:49", "00:18", "08:58", "11:44", "15:47")
    }
    "determine hour, minute, and second" in {
      val input = dag.Operate(BuiltInFunction1Op(HourMinuteSecond),
        dag.AbsoluteLoad(Const(CString("/het/iso8601AcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d
      }

      result2 must contain("05:54:08", "01:51:16", "16:27:24", "11:44:19", "18:53:43", "08:58:10", "00:18:50", "23:50:10", "15:47:40", "03:49:30")
    }
  }
}

object TimeTruncationSpecs extends TimeTruncationSpecs[test.YId] with test.YIdInstances
