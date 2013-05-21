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
import scalaz._
import scalaz.std.list._

import com.precog.util.IdGen

import org.joda.time._
import org.joda.time.format._

trait TimeExtractionSpecs[M[+_]] extends Specification
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

  "time extraction functions (homogeneous case)" should {
    "extract time zone" in {
      val input = dag.Operate(BuiltInFunction1Op(TimeZone),
        dag.LoadLocal(Const(CString("/hom/iso8601"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d.toString
      }
      
      result2 must contain("+08:00", "+09:00", "-10:00", "-07:00", "+06:00")
    }     
  
    "compute season" in {
      val input = dag.Operate(BuiltInFunction1Op(Season),
        dag.LoadLocal(Const(CString("/hom/iso8601"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d.toString
      }
      
      result2 must contain("spring", "winter", "summer")
    }

    "compute year" in {
      val input = dag.Operate(BuiltInFunction1Op(Year),
        dag.LoadLocal(Const(CString("/hom/iso8601"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(2010, 2011, 2012)
    }

    "compute quarter" in {
      val input = dag.Operate(BuiltInFunction1Op(QuarterOfYear),
        dag.LoadLocal(Const(CString("/hom/iso8601"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(1, 2, 3, 4)
    }

    "compute month of year" in {
      val input = dag.Operate(BuiltInFunction1Op(MonthOfYear),
        dag.LoadLocal(Const(CString("/hom/iso8601"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(4, 2, 9, 12)
    }
    
    "compute week of year" in {
      val input = dag.Operate(BuiltInFunction1Op(WeekOfYear),
        dag.LoadLocal(Const(CString("/hom/iso8601"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(17, 8, 36, 6, 52)
    }
    "compute week of month" in {
      val input = dag.Operate(BuiltInFunction1Op(WeekOfMonth),
        dag.LoadLocal(Const(CString("/hom/iso8601"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(2, 5, 4)
    }
    "compute day of year" in {
      val input = dag.Operate(BuiltInFunction1Op(DayOfYear),
        dag.LoadLocal(Const(CString("/hom/iso8601"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(52, 119, 42, 249, 363)
    }
    "compute day of month" in {
      val input = dag.Operate(BuiltInFunction1Op(DayOfMonth),
        dag.LoadLocal(Const(CString("/hom/iso8601"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
        
      result2 must contain(21, 29, 11, 6, 28)
    }
    "compute day of week" in {
      val input = dag.Operate(BuiltInFunction1Op(DayOfWeek),
        dag.LoadLocal(Const(CString("/hom/iso8601"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(1, 2, 6, 5, 4)
    }
    "compute hour of day" in {
      val input = dag.Operate(BuiltInFunction1Op(HourOfDay),
        dag.LoadLocal(Const(CString("/hom/iso8601"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(20, 6, 9)
    }
    "compute minute of hour" in {
      val input = dag.Operate(BuiltInFunction1Op(MinuteOfHour),
        dag.LoadLocal(Const(CString("/hom/iso8601"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(9, 44, 11, 37, 38)
    }
    "compute second of minute" in {
      val input = dag.Operate(BuiltInFunction1Op(SecondOfMinute),
        dag.LoadLocal(Const(CString("/hom/iso8601"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(19, 59, 52, 33)
    }
    "compute millis of second" in {
      val input = dag.Operate(BuiltInFunction1Op(MillisOfSecond),
        dag.LoadLocal(Const(CString("/hom/iso8601"))(line))(line))(line)
        
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
      val input = dag.Operate(BuiltInFunction1Op(TimeZone),
        dag.LoadLocal(Const(CString("/het/iso8601"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d.toString
      }
      
      result2 must contain("+08:00", "+09:00", "-10:00", "-07:00", "+06:00")
    }     
  
    "compute season" in {
      val input = dag.Operate(BuiltInFunction1Op(Season),
        dag.LoadLocal(Const(CString("/het/iso8601"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d.toString
      }
      
      result2 must contain("spring", "winter", "summer")
    }

    "compute year" in {
      val input = dag.Operate(BuiltInFunction1Op(Year),
        dag.LoadLocal(Const(CString("/het/iso8601"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(2010, 2011, 2012)
    }

    "compute quarter" in {
      val input = dag.Operate(BuiltInFunction1Op(QuarterOfYear),
        dag.LoadLocal(Const(CString("/het/iso8601"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(1, 2, 3, 4)
    }

    "compute month of year" in {
      val input = dag.Operate(BuiltInFunction1Op(MonthOfYear),
        dag.LoadLocal(Const(CString("/het/iso8601"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(4, 2, 9, 12)
    }
    
    "compute week of year" in {
      val input = dag.Operate(BuiltInFunction1Op(WeekOfYear),
        dag.LoadLocal(Const(CString("/het/iso8601"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(17, 8, 36, 6, 52)
    }
    "compute week of month" in {
      val input = dag.Operate(BuiltInFunction1Op(WeekOfMonth),
        dag.LoadLocal(Const(CString("/het/iso8601"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(2, 5, 4)
    }
    "compute day of year" in {
      val input = dag.Operate(BuiltInFunction1Op(DayOfYear),
        dag.LoadLocal(Const(CString("/het/iso8601"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(52, 119, 42, 249, 363)
    }
    "compute day of month" in {
      val input = dag.Operate(BuiltInFunction1Op(DayOfMonth),
        dag.LoadLocal(Const(CString("/het/iso8601"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
        
      result2 must contain(21, 29, 11, 6, 28)
    }
    "compute day of week" in {
      val input = dag.Operate(BuiltInFunction1Op(DayOfWeek),
        dag.LoadLocal(Const(CString("/het/iso8601"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(1, 2, 6, 5, 4)
    }
    "compute hour of day" in {
      val input = dag.Operate(BuiltInFunction1Op(HourOfDay),
        dag.LoadLocal(Const(CString("/het/iso8601"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(20, 6, 9)
    }
    "compute minute of hour" in {
      val input = dag.Operate(BuiltInFunction1Op(MinuteOfHour),
        dag.LoadLocal(Const(CString("/het/iso8601"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(9, 44, 11, 37, 38)
    }
    "compute second of minute" in {
      val input = dag.Operate(BuiltInFunction1Op(SecondOfMinute),
        dag.LoadLocal(Const(CString("/het/iso8601"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(19, 59, 52, 33)
    }
    "compute millis of second" in {
      val input = dag.Operate(BuiltInFunction1Op(MillisOfSecond),
        dag.LoadLocal(Const(CString("/het/iso8601"))(line))(line))(line)
        
      val result = testEval(input)
      
      result must haveSize(5)
      
      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }
      
      result2 must contain(430, 165, 848, 394, 599)
    }
  }

  "time extraction functions (homogeneous case across slices)" should {
    "extract time zone" in {
      val input = dag.Operate(BuiltInFunction1Op(TimeZone),
        dag.LoadLocal(Const(CString("/hom/iso8601AcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d.toString
      }

      result2 must contain("-09:00", "-11:00", "-10:00", "+04:00", "-01:00", "+11:00", "-03:00", "+05:00", "-02:00", "-05:00", "+00:00", "+06:00", "-04:00", "+07:00")
    }

    "compute season" in {
      val input = dag.Operate(BuiltInFunction1Op(Season),
        dag.LoadLocal(Const(CString("/hom/iso8601AcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d.toString
      }

      result2 must contain("spring", "winter", "summer")
    }

    "compute year" in {
      val input = dag.Operate(BuiltInFunction1Op(Year),
        dag.LoadLocal(Const(CString("/hom/iso8601AcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(2010, 2007, 2011, 2012, 2009, 2008)
    }

    "compute quarter" in {
      val input = dag.Operate(BuiltInFunction1Op(QuarterOfYear),
        dag.LoadLocal(Const(CString("/hom/iso8601AcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(1, 2, 3, 4)
    }

    "compute month of year" in {
      val input = dag.Operate(BuiltInFunction1Op(MonthOfYear),
        dag.LoadLocal(Const(CString("/hom/iso8601AcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(5, 10, 1, 2, 12, 7, 3, 8)
    }

    "compute week of year" in {
      val input = dag.Operate(BuiltInFunction1Op(WeekOfYear),
        dag.LoadLocal(Const(CString("/hom/iso8601AcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(5, 10, 52, 29, 6, 21, 33, 9, 41, 2, 32, 44, 12, 7, 18, 31, 11, 43)
    }
    "compute week of month" in {
      val input = dag.Operate(BuiltInFunction1Op(WeekOfMonth),
        dag.LoadLocal(Const(CString("/hom/iso8601AcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(5, 1, 6, 2, 3, 4)
    }
    "compute day of year" in {
      val input = dag.Operate(BuiltInFunction1Op(DayOfYear),
        dag.LoadLocal(Const(CString("/hom/iso8601AcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(138, 10, 46, 228, 216, 74, 302, 65, 285, 212, 41, 64, 144, 66, 198, 223, 35, 363, 40, 300, 122, 83)
    }
    "compute day of month" in {
      val input = dag.Operate(BuiltInFunction1Op(DayOfMonth),
        dag.LoadLocal(Const(CString("/hom/iso8601AcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(10, 24, 14, 29, 6, 28, 9, 2, 17, 27, 18, 11, 23, 30, 4, 15)
    }
    "compute day of week" in {
      val input = dag.Operate(BuiltInFunction1Op(DayOfWeek),
        dag.LoadLocal(Const(CString("/hom/iso8601AcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(5, 1, 6, 2, 7, 3, 4)
    }
    "compute hour of day" in {
      val input = dag.Operate(BuiltInFunction1Op(HourOfDay),
        dag.LoadLocal(Const(CString("/hom/iso8601AcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(0, 10, 14, 1, 21, 13, 2, 17, 22, 12, 3, 18, 11, 19, 4)
    }
    "compute minute of hour" in {
      val input = dag.Operate(BuiltInFunction1Op(MinuteOfHour),
        dag.LoadLocal(Const(CString("/hom/iso8601AcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(5, 56, 52, 14, 20, 29, 38, 33, 53, 2, 49, 48, 18, 31, 11, 43, 58, 36, 30, 19)
    }
    "compute second of minute" in {
      val input = dag.Operate(BuiltInFunction1Op(SecondOfMinute),
        dag.LoadLocal(Const(CString("/hom/iso8601AcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(22)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(0, 56, 37, 14, 28, 38, 21, 53, 41, 34, 17, 22, 48, 16, 31, 40, 55, 19, 4)
    }
    "compute millis of second" in {
      val input = dag.Operate(BuiltInFunction1Op(MillisOfSecond),
        dag.LoadLocal(Const(CString("/hom/iso8601AcrossSlices"))(line))(line))(line)

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
      val input = dag.Operate(BuiltInFunction1Op(TimeZone),
        dag.LoadLocal(Const(CString("/het/iso8601AcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d.toString
      }

      result2 must contain("+08:00", "-07:00", "-11:00", "+04:00", "+10:00", "+03:00", "+02:00", "+00:00", "+06:00", "-04:00")
    }

    "compute season" in {
      val input = dag.Operate(BuiltInFunction1Op(Season),
        dag.LoadLocal(Const(CString("/het/iso8601AcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SString(d)) if ids.length == 1 => d.toString
      }

      result2 must contain("summer", "spring", "fall")
    }

    "compute year" in {
      val input = dag.Operate(BuiltInFunction1Op(Year),
        dag.LoadLocal(Const(CString("/het/iso8601AcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(2010, 2007, 2011, 2012, 2009, 2008)
    }

    "compute quarter" in {
      val input = dag.Operate(BuiltInFunction1Op(QuarterOfYear),
        dag.LoadLocal(Const(CString("/het/iso8601AcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(4, 2, 3)
    }

    "compute month of year" in {
      val input = dag.Operate(BuiltInFunction1Op(MonthOfYear),
        dag.LoadLocal(Const(CString("/het/iso8601AcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(5, 10, 6, 7, 11, 8)
    }

    "compute week of year" in {
      val input = dag.Operate(BuiltInFunction1Op(WeekOfYear),
        dag.LoadLocal(Const(CString("/het/iso8601AcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(25, 46, 28, 41, 34, 22, 27, 18, 43)
    }
    "compute week of month" in {
      val input = dag.Operate(BuiltInFunction1Op(WeekOfMonth),
        dag.LoadLocal(Const(CString("/het/iso8601AcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(3, 5, 1, 4)
    }
    "compute day of year" in {
      val input = dag.Operate(BuiltInFunction1Op(DayOfYear),
        dag.LoadLocal(Const(CString("/het/iso8601AcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(184, 325, 229, 298, 148, 176, 286, 126, 195)
    }
    "compute day of month" in {
      val input = dag.Operate(BuiltInFunction1Op(DayOfMonth),
        dag.LoadLocal(Const(CString("/het/iso8601AcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(5, 24, 25, 14, 21, 13, 2, 17, 27)
    }
    "compute day of week" in {
      val input = dag.Operate(BuiltInFunction1Op(DayOfWeek),
        dag.LoadLocal(Const(CString("/het/iso8601AcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(5, 1, 6, 2, 7, 3, 4)
    }
    "compute hour of day" in {
      val input = dag.Operate(BuiltInFunction1Op(HourOfDay),
        dag.LoadLocal(Const(CString("/het/iso8601AcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(0, 5, 1, 3, 18, 16, 11, 23, 8, 15)
    }
    "compute minute of hour" in {
      val input = dag.Operate(BuiltInFunction1Op(MinuteOfHour),
        dag.LoadLocal(Const(CString("/het/iso8601AcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(53, 44, 27, 54, 49, 18, 50, 58, 51, 47)
    }
    "compute second of minute" in {
      val input = dag.Operate(BuiltInFunction1Op(SecondOfMinute),
        dag.LoadLocal(Const(CString("/het/iso8601AcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(10, 24, 50, 16, 43, 40, 8, 30, 19)
    }
    "compute millis of second" in {
      val input = dag.Operate(BuiltInFunction1Op(MillisOfSecond),
        dag.LoadLocal(Const(CString("/het/iso8601AcrossSlices"))(line))(line))(line)

      val result = testEval(input)

      result must haveSize(10)

      val result2 = result collect {
        case (ids, SDecimal(d)) if ids.length == 1 => d.toInt
      }

      result2 must contain(629, 873, 248, 311, 513, 858, 932, 844, 171, 506)
    }
  }
}

object TimeExtractionSpecs extends TimeExtractionSpecs[test.YId] with test.YIdInstances
