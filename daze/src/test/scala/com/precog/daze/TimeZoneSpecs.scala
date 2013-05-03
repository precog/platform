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

trait TimeZoneSpecs[M[+_]] extends Specification
    with EvaluatorTestSupport[M]
    with LongIdMemoryDatasetConsumer[M] { self =>
      
  import Function._
  
  import dag._
  import instructions._
  import library._

  val testAPIKey = "testAPIKey"

  val line = Line(1, 1, "")
  def inputOp1(op: Op1, loadFrom: String) = {
    dag.Operate(BuiltInFunction1Op(op),
      dag.LoadLocal(Const(CString(loadFrom))(line))(line))(line)
  }

  def testEval(graph: DepGraph): Set[SEvent] = {
    consumeEval(testAPIKey, graph, Path.Root) match {
      case Success(results) => results
      case Failure(error) => throw error
    }
  }

  "changing time zones (homogenous case)" should {
    "change to the correct time zone" in {
      val input = Join(BuiltInFunction2Op(ChangeTimeZone), Cross(None),
        dag.LoadLocal(Const(CString("/hom/iso8601"))(line))(line),
        Const(CString("-10:00"))(line))(line)
        
      val result = testEval(input) collect {
        case (ids, SString(d)) if ids.length == 1 => d.toString
      }
      
      result must contain("2011-02-21T01:09:59.165-10:00", "2012-02-11T06:11:33.394-10:00", "2011-09-06T06:44:52.848-10:00", "2010-04-28T15:37:52.599-10:00", "2012-12-28T06:38:19.430-10:00").only
    }

    "not modify millisecond value" in {
      val input = Join(BuiltInFunction2Op(ChangeTimeZone), Cross(None),
        dag.LoadLocal(Const(CString("/hom/iso8601"))(line))(line),
        Const(CString("-10:00"))(line))(line)
        
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
      val input = Join(BuiltInFunction2Op(ChangeTimeZone), Cross(None),
        dag.LoadLocal(Const(CString("/hom/iso8601"))(line))(line),
        Const(CString("-10:30"))(line))(line)
        
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
      val input = Join(BuiltInFunction2Op(ChangeTimeZone), Cross(None),
        dag.LoadLocal(Const(CString("/het/iso8601"))(line))(line),
        Const(CString("-10:00"))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d.toString
      }
      
      result2 must contain("2011-02-21T01:09:59.165-10:00", "2012-02-11T06:11:33.394-10:00", "2011-09-06T06:44:52.848-10:00", "2010-04-28T15:37:52.599-10:00", "2012-12-28T06:38:19.430-10:00")
    }

    "not modify millisecond value" in {
      val input = Join(BuiltInFunction2Op(ChangeTimeZone), Cross(None),
        dag.LoadLocal(Const(CString("/hom/iso8601"))(line))(line),
        Const(CString("-10:00"))(line))(line)
        
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
      val input = Join(BuiltInFunction2Op(ChangeTimeZone), Cross(None),
        dag.LoadLocal(Const(CString("/het/iso8601"))(line))(line),
        Const(CString("-10:30"))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d.toString
      }
      
      result2 must contain("2011-02-21T00:39:59.165-10:30", "2012-02-11T05:41:33.394-10:30", "2011-09-06T06:14:52.848-10:30", "2010-04-28T15:07:52.599-10:30", "2012-12-28T06:08:19.430-10:30")
    }
  }
}

object TimeZoneSpecs extends TimeZoneSpecs[test.YId] with test.YIdInstances
