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
    "parse period" in {
      val input = Operate(BuiltInFunction1Op(ParsePeriod),
        Const(CString("P3Y6M4DT12H30M5S"))(line))(line)

      val result = testEval(input)

      result must haveSize(1)

      val result2 = result collect { case (ids, period) if ids.size == 0 => period }
      result2 mustEqual(Set(SString("P3Y6M4DT12H30M5S")))
    }
  }

  def createObject(field: String, op1: Op1, value: String) = {
    Join(WrapObject, CrossLeftSort,
      Const(CString(field))(line),
      Operate(BuiltInFunction1Op(op1),
        Const(CString(value))(line))(line))(line)
  }

  "Range" should {
    "do the right thing" in {
      val start = createObject("start", ParseDateTimeFuzzy, "1987-12-09T18:33:02.037Z")
      val end = createObject("end", ParseDateTimeFuzzy, "2005-08-09T12:00:00.000Z")
      val step = createObject("step", ParsePeriod, "P3Y6M4DT12H30M5S")

      val obj = Join(JoinObject, CrossLeftSort,
        Join(JoinObject, CrossLeftSort,
          start,
          end)(line),
        step)(line)

      //val x = Operate(BuiltInFunction1Op(ParseDateTimeFuzzy), Const(CString("1987-12-09T18:33:02.037Z"))(line))(line)

      val input = Operate(BuiltInFunction1Op(TimeRange), obj)(line)

      val result = testEval(input)

      result mustEqual Set((
        Vector(),
        SArray(Vector(
          SString("1991-06-14T07:03:07.037Z"),
          SString("1994-12-18T19:33:12.037Z"),
          SString("1998-06-23T08:03:17.037Z"),
          SString("2001-12-27T20:33:22.037Z"),
          SString("2005-07-02T09:03:27.037Z")))))
    }
  }
}

object TimePeriodSpecs extends TimePeriodSpecs[test.YId] with test.YIdInstances
