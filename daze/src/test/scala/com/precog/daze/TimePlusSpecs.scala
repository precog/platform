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

import com.precog.common._
import com.precog.yggdrasil._
import com.precog.common.Path
import scalaz._
import scalaz.std.list._

import com.precog.util.IdGen

import org.joda.time._
import org.joda.time.format._

trait TimePlusSpecs[M[+_]] extends Specification
    with EvaluatorTestSupport[M]
    with LongIdMemoryDatasetConsumer[M] { self =>
      
  import Function._
  
  import dag._
  import instructions._
  import library._

  val testAPIKey = "testAPIKey"

  val line = Line(1, 1, "")

  def testEval(graph: DepGraph): Set[SEvent] = {
    consumeEval(graph, defaultEvaluationContext) match {
      case Success(results) => results
      case Failure(error) => throw error
    }
  }

  "time plus functions (homogeneous case)" should {
    "compute incrememtation of positive number of years" in {
      val input = Join(BuiltInFunction2Op(YearsPlus), Cross(None),
        dag.LoadLocal(Const(CString("/hom/iso8601"))(line))(line),
        Const(CLong(5))(line))(line)
        
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
      val input = Join(BuiltInFunction2Op(YearsPlus), Cross(None),
        dag.LoadLocal(Const(CString("/hom/iso8601"))(line))(line),
        Const(CLong(-5))(line))(line)
        
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
      val input = Join(BuiltInFunction2Op(YearsPlus), Cross(None),
        dag.LoadLocal(Const(CString("/hom/iso8601"))(line))(line),
        Const(CLong(0))(line))(line)
        
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
      val input = Join(BuiltInFunction2Op(MonthsPlus), Cross(None),
        dag.LoadLocal(Const(CString("/hom/iso8601"))(line))(line),
        Const(CLong(5))(line))(line)
        
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
      val input = Join(BuiltInFunction2Op(WeeksPlus), Cross(None),
        dag.LoadLocal(Const(CString("/hom/iso8601"))(line))(line),
        Const(CLong(5))(line))(line)
        
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
      val input = Join(BuiltInFunction2Op(DaysPlus), Cross(None),
        dag.LoadLocal(Const(CString("/hom/iso8601"))(line))(line),
        Const(CLong(5))(line))(line)
        
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
      val input = Join(BuiltInFunction2Op(HoursPlus), Cross(None),
        dag.LoadLocal(Const(CString("/hom/iso8601"))(line))(line),
        Const(CLong(5))(line))(line)
        
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
      val input = Join(BuiltInFunction2Op(MinutesPlus), Cross(None),
        dag.LoadLocal(Const(CString("/hom/iso8601"))(line))(line),
        Const(CLong(5))(line))(line)
        
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
      val input = Join(BuiltInFunction2Op(SecondsPlus), Cross(None),
        dag.LoadLocal(Const(CString("/hom/iso8601"))(line))(line),
        Const(CLong(5))(line))(line)
        
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
      val input = Join(BuiltInFunction2Op(MillisPlus), Cross(None),
        dag.LoadLocal(Const(CString("/hom/iso8601"))(line))(line),
        Const(CLong(5))(line))(line)
        
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
      val input = Join(BuiltInFunction2Op(YearsPlus), Cross(None),
        dag.LoadLocal(Const(CString("/het/iso8601"))(line))(line),
        Const(CLong(5))(line))(line)
        
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
      val input = Join(BuiltInFunction2Op(MonthsPlus), Cross(None),
        dag.LoadLocal(Const(CString("/het/iso8601"))(line))(line),
        Const(CLong(5))(line))(line)
        
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
      val input = Join(BuiltInFunction2Op(WeeksPlus), Cross(None),
        dag.LoadLocal(Const(CString("/het/iso8601"))(line))(line),
        Const(CLong(5))(line))(line)
        
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
      val input = Join(BuiltInFunction2Op(DaysPlus), Cross(None),
        dag.LoadLocal(Const(CString("/het/iso8601"))(line))(line),
        Const(CLong(5))(line))(line)
        
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
      val input = Join(BuiltInFunction2Op(HoursPlus), Cross(None),
        dag.LoadLocal(Const(CString("/het/iso8601"))(line))(line),
        Const(CLong(5))(line))(line)
        
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
      val input = Join(BuiltInFunction2Op(MinutesPlus), Cross(None),
        dag.LoadLocal(Const(CString("/het/iso8601"))(line))(line),
        Const(CLong(5))(line))(line)
        
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
      val input = Join(BuiltInFunction2Op(SecondsPlus), Cross(None),
        dag.LoadLocal(Const(CString("/het/iso8601"))(line))(line),
        Const(CLong(5))(line))(line)
        
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
      val input = Join(BuiltInFunction2Op(MillisPlus), Cross(None),
        dag.LoadLocal(Const(CString("/het/iso8601"))(line))(line),
        Const(CLong(5))(line))(line)
        
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

  "time plus functions (homogeneous case across slices)" should {
    "compute incrememtation of positive number of years" in {
      val input = Join(BuiltInFunction2Op(YearsPlus), Cross(None),
        dag.LoadLocal(Const(CString("/hom/iso8601AcrossSlices"))(line))(line),
        Const(CLong(5))(line))(line)

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
      val input = Join(BuiltInFunction2Op(YearsPlus), Cross(None),
        dag.LoadLocal(Const(CString("/hom/iso8601AcrossSlices"))(line))(line),
        Const(CLong(-5))(line))(line)

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
      val input = Join(BuiltInFunction2Op(YearsPlus), Cross(None),
        dag.LoadLocal(Const(CString("/hom/iso8601AcrossSlices"))(line))(line),
        Const(CLong(0))(line))(line)

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
      val input = Join(BuiltInFunction2Op(MonthsPlus), Cross(None),
        dag.LoadLocal(Const(CString("/hom/iso8601AcrossSlices"))(line))(line),
        Const(CLong(5))(line))(line)

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
      val input = Join(BuiltInFunction2Op(WeeksPlus), Cross(None),
        dag.LoadLocal(Const(CString("/hom/iso8601AcrossSlices"))(line))(line),
        Const(CLong(5))(line))(line)

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
      val input = Join(BuiltInFunction2Op(DaysPlus), Cross(None),
        dag.LoadLocal(Const(CString("/hom/iso8601AcrossSlices"))(line))(line),
        Const(CLong(5))(line))(line)

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
      val input = Join(BuiltInFunction2Op(HoursPlus), Cross(None),
        dag.LoadLocal(Const(CString("/hom/iso8601AcrossSlices"))(line))(line),
        Const(CLong(5))(line))(line)

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
      val input = Join(BuiltInFunction2Op(MinutesPlus), Cross(None),
        dag.LoadLocal(Const(CString("/hom/iso8601AcrossSlices"))(line))(line),
        Const(CLong(5))(line))(line)

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
      val input = Join(BuiltInFunction2Op(SecondsPlus), Cross(None),
        dag.LoadLocal(Const(CString("/hom/iso8601AcrossSlices"))(line))(line),
        Const(CLong(5))(line))(line)

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
      val input = Join(BuiltInFunction2Op(MillisPlus), Cross(None),
        dag.LoadLocal(Const(CString("/hom/iso8601AcrossSlices"))(line))(line),
        Const(CLong(5))(line))(line)

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
      val input = Join(BuiltInFunction2Op(YearsPlus), Cross(None),
        dag.LoadLocal(Const(CString("/het/iso8601AcrossSlices"))(line))(line),
        Const(CLong(5))(line))(line)

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
      val input = Join(BuiltInFunction2Op(MonthsPlus), Cross(None),
        dag.LoadLocal(Const(CString("/het/iso8601AcrossSlices"))(line))(line),
        Const(CLong(5))(line))(line)

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
      val input = Join(BuiltInFunction2Op(WeeksPlus), Cross(None),
        dag.LoadLocal(Const(CString("/het/iso8601AcrossSlices"))(line))(line),
        Const(CLong(5))(line))(line)

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
      val input = Join(BuiltInFunction2Op(DaysPlus), Cross(None),
        dag.LoadLocal(Const(CString("/het/iso8601AcrossSlices"))(line))(line),
        Const(CLong(5))(line))(line)

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
      val input = Join(BuiltInFunction2Op(HoursPlus), Cross(None),
        dag.LoadLocal(Const(CString("/het/iso8601AcrossSlices"))(line))(line),
        Const(CLong(5))(line))(line)

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
      val input = Join(BuiltInFunction2Op(MinutesPlus), Cross(None),
        dag.LoadLocal(Const(CString("/het/iso8601AcrossSlices"))(line))(line),
        Const(CLong(5))(line))(line)

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
      val input = Join(BuiltInFunction2Op(SecondsPlus), Cross(None),
        dag.LoadLocal(Const(CString("/het/iso8601AcrossSlices"))(line))(line),
        Const(CLong(5))(line))(line)

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
      val input = Join(BuiltInFunction2Op(MillisPlus), Cross(None),
        dag.LoadLocal(Const(CString("/het/iso8601AcrossSlices"))(line))(line),
        Const(CLong(5))(line))(line)

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
}

object TimePlusSpecs extends TimePlusSpecs[test.YId] with test.YIdInstances
