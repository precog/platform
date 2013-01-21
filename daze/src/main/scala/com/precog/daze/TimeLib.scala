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
package com.precog
package daze

import bytecode.{ Library, BinaryOperationType, UnaryOperationType, JTextT, JNumberT }

import yggdrasil._
import yggdrasil.table._

import org.joda.time._
import org.joda.time.format._

import com.precog.util.DateTimeUtil.parseDateTime

import TransSpecModule._

trait TimeLib[M[+_]] extends GenOpcode[M] {
  import trans._

  val TimeNamespace = Vector("std", "time")

  override def _lib1 = super._lib1 ++ Set(
    GetMillis,
    TimeZone,
    Season,

    Year,
    QuarterOfYear,
    MonthOfYear,
    WeekOfYear,
    WeekOfMonth,
    DayOfYear,
    DayOfMonth,
    DayOfWeek,
    HourOfDay,
    MinuteOfHour,
    SecondOfMinute,
    MillisOfSecond,

    Date,
    YearMonth,
    YearDayOfYear,
    MonthDay,
    DateHour,
    DateHourMinute,
    DateHourMinuteSecond,
    DateHourMinuteSecondMillis,
    TimeWithZone,
    TimeWithoutZone,
    HourMinute,
    HourMinuteSecond
  )

  override def _lib2 = super._lib2 ++ Set(
    YearsPlus,
    MonthsPlus,
    WeeksPlus,
    DaysPlus,
    HoursPlus,
    MinutesPlus,
    SecondsPlus,
    MillisPlus,

    YearsBetween,
    MonthsBetween,
    WeeksBetween,
    DaysBetween,
    HoursBetween,
    MinutesBetween,
    SecondsBetween,
    MillisBetween,

    MillisToISO,
    ChangeTimeZone,
    ParseDateTime
  )

  private def isValidISO(str: String): Boolean = {
    try { parseDateTime(str, true); true
    } catch {
      case e:IllegalArgumentException => { false }
    }
  }

  private def isValidTimeZone(str: String): Boolean = {
    try { DateTimeZone.forID(str); true
    } catch {
      case e:IllegalArgumentException => { false }
    }
  }
  
  private def isValidFormat(time: String, fmt: String): Boolean = {
    try { DateTimeFormat.forPattern(fmt).withOffsetParsed().parseDateTime(time); true
    } catch {
      case e: IllegalArgumentException => { false }
    }
  }

  DateTimeZone.setDefault(DateTimeZone.UTC)

  object ParseDateTime extends Op2(TimeNamespace, "parse") {
    val tpe = BinaryOperationType(JTextT, JTextT, JTextT)
    def f2(ctx: EvaluationContext): F2 = CF2P("builtin::time::parseDateTime") {
      case (c1: StrColumn, c2: StrColumn) => new Map2Column(c1, c2) with StrColumn {
        override def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row) && isValidFormat(c1(row), c2(row))

        def apply(row: Int) = {
          val time = c1(row)
          val fmt = c2(row)

          val format = DateTimeFormat.forPattern(fmt).withOffsetParsed()
          val ISO = format.parseDateTime(time)

          ISO.toString()
        }
      }
    }
  }

  object ChangeTimeZone extends Op2(TimeNamespace, "changeTimeZone") {
    val tpe = BinaryOperationType(JTextT, JTextT, JTextT)
    def f2(ctx: EvaluationContext): F2 = CF2P("builtin::time::changeTimeZone") {
      case (c1: StrColumn, c2: StrColumn) => new Map2Column(c1, c2) with StrColumn {
        override def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row) && isValidISO(c1(row)) && isValidTimeZone(c2(row))

        def apply(row: Int) = {
          val time = c1(row)
          val tz = c2(row)

          val newTime = parseDateTime(time, false)
          val timeZone = DateTimeZone.forID(tz)
          val dateTime = new DateTime(newTime, timeZone)

          dateTime.toString()
        }
      }
    }
  }

  trait TimePlus extends Op2 {
    val tpe = BinaryOperationType(JTextT, JNumberT, JTextT)
    def f2(ctx: EvaluationContext): F2 = CF2P("builtin::time::timePlus") {
      case (c1: StrColumn, c2: LongColumn) => new Map2Column(c1, c2) with StrColumn {
        override def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row) && isValidISO(c1(row))

        def apply(row: Int) = {
          val time = c1(row)
          val incr = c2(row)

          val newTime = parseDateTime(time, true)

          plus(newTime, incr.toInt)
        }
      }      
      case (c1: StrColumn, c2: NumColumn) => new Map2Column(c1, c2) with StrColumn {
        override def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row) && isValidISO(c1(row))

        def apply(row: Int) = {
          val time = c1(row)
          val incr = c2(row)

          val newTime = parseDateTime(time, true)

          plus(newTime, incr.toInt)
        }
      }      
      case (c1: StrColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with StrColumn {
        override def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row) && isValidISO(c1(row))

        def apply(row: Int) = {
          val time = c1(row)
          val incr = c2(row)

          val newTime = parseDateTime(time, true)

          plus(newTime, incr.toInt)
        }
      }
    }

    def plus(d: DateTime, i: Int): String
  }

  object YearsPlus extends Op2(TimeNamespace, "yearsPlus") with TimePlus{ 
    def plus(d: DateTime, i: Int) = d.plus(Period.years(i)).toString()
  }

  object MonthsPlus extends Op2(TimeNamespace, "monthsPlus") with TimePlus{ 
    def plus(d: DateTime, i: Int) = d.plus(Period.months(i)).toString()
  }

  object WeeksPlus extends Op2(TimeNamespace, "weeksPlus") with TimePlus{ 
    def plus(d: DateTime, i: Int) = d.plus(Period.weeks(i)).toString()
  }

  object DaysPlus extends Op2(TimeNamespace, "daysPlus") with TimePlus{ 
    def plus(d: DateTime, i: Int) = d.plus(Period.days(i)).toString()
  }

  object HoursPlus extends Op2(TimeNamespace, "hoursPlus") with TimePlus{ 
    def plus(d: DateTime, i: Int) = d.plus(Period.hours(i)).toString()
  }

  object MinutesPlus extends Op2(TimeNamespace, "minutesPlus") with TimePlus{ 
    def plus(d: DateTime, i: Int) = d.plus(Period.minutes(i)).toString()
  }

  object SecondsPlus extends Op2(TimeNamespace, "secondsPlus") with TimePlus{ 
    def plus(d: DateTime, i: Int) = d.plus(Period.seconds(i)).toString()
  }

  object MillisPlus extends Op2(TimeNamespace, "millisPlus") with TimePlus{
    def plus(d: DateTime, i: Int) = d.plus(Period.millis(i)).toString()
  }
  

  trait TimeBetween extends Op2 {
    val tpe = BinaryOperationType(JTextT, JTextT, JNumberT)
    def f2(ctx: EvaluationContext): F2 = CF2P("builtin::time::timeBetween") {
      case (c1: StrColumn, c2: StrColumn) => new Map2Column(c1, c2) with LongColumn {
        override def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row) && isValidISO(c1(row)) && isValidISO(c2(row))

        def apply(row: Int) = {
          val time1 = c1(row)
          val time2 = c2(row)

          val newTime1 = parseDateTime(time1, true)
          val newTime2 = parseDateTime(time2, true)

          between(newTime1, newTime2)
        }
      }
    }

    def between(d1: DateTime, d2: DateTime): Long
  }

  object YearsBetween extends Op2(TimeNamespace, "yearsBetween") with TimeBetween{
    def between(d1: DateTime, d2: DateTime) = Years.yearsBetween(d1, d2).getYears
  }

  object MonthsBetween extends Op2(TimeNamespace, "monthsBetween") with TimeBetween{
    def between(d1: DateTime, d2: DateTime) = Months.monthsBetween(d1, d2).getMonths
  }

  object WeeksBetween extends Op2(TimeNamespace, "weeksBetween") with TimeBetween{
    def between(d1: DateTime, d2: DateTime) = Weeks.weeksBetween(d1, d2).getWeeks
  }

  object DaysBetween extends Op2(TimeNamespace, "daysBetween") with TimeBetween{
    def between(d1: DateTime, d2: DateTime) = Days.daysBetween(d1, d2).getDays
  }

  object HoursBetween extends Op2(TimeNamespace, "hoursBetween") with TimeBetween{
    def between(d1: DateTime, d2: DateTime) = Hours.hoursBetween(d1, d2).getHours
  }

  object MinutesBetween extends Op2(TimeNamespace, "minutesBetween") with TimeBetween{
    def between(d1: DateTime, d2: DateTime) = Minutes.minutesBetween(d1, d2).getMinutes
  }

  object SecondsBetween extends Op2(TimeNamespace, "secondsBetween") with TimeBetween{
    def between(d1: DateTime, d2: DateTime) = Seconds.secondsBetween(d1, d2).getSeconds
  }

  object MillisBetween extends Op2(TimeNamespace, "millisBetween") with TimeBetween{
    def between(d1: DateTime, d2: DateTime) = d2.getMillis - d1.getMillis
  }

  object MillisToISO extends Op2(TimeNamespace, "millisToISO") {
    val tpe = BinaryOperationType(JNumberT, JTextT, JTextT)
    def f2(ctx: EvaluationContext): F2 = CF2P("builtin::time::millisToIso") {
      case (c1: LongColumn, c2: StrColumn) => new Map2Column(c1, c2) with StrColumn {
        override def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row) && c1(row) >= Long.MinValue && c1(row) <= Long.MaxValue && isValidTimeZone(c2(row))

        def apply(row: Int) = { 
          val time = c1(row)
          val tz = c2(row)

          val timeZone = DateTimeZone.forID(tz)
          val dateTime = new DateTime(time.toLong, timeZone)

          dateTime.toString()
        }
      }      
      case (c1: NumColumn, c2: StrColumn) => new Map2Column(c1, c2) with StrColumn {
        override def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row) && c1(row) >= Long.MinValue && c1(row) <= Long.MaxValue && isValidTimeZone(c2(row))

        def apply(row: Int) = { 
          val time = c1(row)
          val tz = c2(row)

          val timeZone = DateTimeZone.forID(tz)
          val dateTime = new DateTime(time.toLong, timeZone)

          dateTime.toString()
        }
      }      
      case (c1: DoubleColumn, c2: StrColumn) => new Map2Column(c1, c2) with StrColumn {
        override def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row) && c1(row) >= Long.MinValue && c1(row) <= Long.MaxValue && isValidTimeZone(c2(row))

        def apply(row: Int) = { 
          val time = c1(row)
          val tz = c2(row)

          val timeZone = DateTimeZone.forID(tz)
          val dateTime = new DateTime(time.toLong, timeZone)

          dateTime.toString()
        }
      }
    }
  }

  object GetMillis extends Op1F1(TimeNamespace, "getMillis") {
    val tpe = UnaryOperationType(JTextT, JNumberT)
    def f1(ctx: EvaluationContext): F1 = CF1P("builtin::time::getMillis") {
      case c: StrColumn => new Map1Column(c) with LongColumn {
        override def isDefinedAt(row: Int) = c.isDefinedAt(row) && isValidISO(c(row))

        def apply(row: Int) = {
          val time = c(row)

          val newTime = parseDateTime(time, true)
          newTime.getMillis()
        }
      }
    }
    def spec[A <: SourceType](ctx: EvaluationContext): TransSpec[A] => TransSpec[A] =
      transSpec => trans.Map1(transSpec, f1(ctx))
  }

  object TimeZone extends Op1F1(TimeNamespace, "timeZone") {
    val tpe = UnaryOperationType(JTextT, JTextT)
    def f1(ctx: EvaluationContext): F1 = CF1P("builtin::time::timeZone") {
      case c: StrColumn => new Map1Column(c) with StrColumn {
        override def isDefinedAt(row: Int) = c.isDefinedAt(row) && isValidISO(c(row))

        def apply(row: Int) = { 
          val time = c(row)

          val format = DateTimeFormat.forPattern("ZZ")
          val newTime = parseDateTime(time, true)
          format.print(newTime)
        }
      }
    }
    def spec[A <: SourceType](ctx: EvaluationContext): TransSpec[A] => TransSpec[A] =
      transSpec => trans.Map1(transSpec, f1(ctx))
  }

  object Season extends Op1F1(TimeNamespace, "season") {
    val tpe = UnaryOperationType(JTextT, JTextT)
    def f1(ctx: EvaluationContext): F1 = CF1P("builtin::time::season") {
      case c: StrColumn => new Map1Column(c) with StrColumn {
        override def isDefinedAt(row: Int) = c.isDefinedAt(row) && isValidISO(c(row))

        def apply(row: Int) = {
          val time = c(row)

          val newTime = parseDateTime(time, true)
          val day = newTime.dayOfYear.get
          
          if (day >= 79 & day < 171) "spring"
          else if (day >= 171 & day < 265) "summer"
          else if (day >= 265 & day < 355) "fall"
          else "winter"
        }
      }
    }
    def spec[A <: SourceType](ctx: EvaluationContext): TransSpec[A] => TransSpec[A] =
      transSpec => trans.Map1(transSpec, f1(ctx))
  } 

  trait TimeFraction extends Op1F1 {
    val tpe = UnaryOperationType(JTextT, JNumberT)
    def f1(ctx: EvaluationContext): F1 = CF1P("builtin::time::timeFraction") {
      case c: StrColumn => new Map1Column(c) with LongColumn {
        override def isDefinedAt(row: Int) = c.isDefinedAt(row) && isValidISO(c(row))

        def apply(row: Int) = {
          val time = c(row)

          val newTime = parseDateTime(time, true)
          fraction(newTime)
        }
      }
    }
    def spec[A <: SourceType](ctx: EvaluationContext): TransSpec[A] => TransSpec[A] =
      transSpec => trans.Map1(transSpec, f1(ctx))

    def fraction(d: DateTime): Int
  }

  object Year extends Op1F1(TimeNamespace, "year") with TimeFraction {
    def fraction(d: DateTime) = d.year.get
  }

  object QuarterOfYear extends Op1F1(TimeNamespace, "quarter") with TimeFraction {
    def fraction(d: DateTime) = ((d.monthOfYear.get - 1) / 3) + 1
  }

  object MonthOfYear extends Op1F1(TimeNamespace, "monthOfYear") with TimeFraction {
    def fraction(d: DateTime) = d.monthOfYear.get
  }

  object WeekOfYear extends Op1F1(TimeNamespace, "weekOfYear") with TimeFraction {
    def fraction(d: DateTime) = d.weekOfWeekyear.get
  } 

  object WeekOfMonth extends Op1F1(TimeNamespace, "weekOfMonth") with TimeFraction {
    def fraction(newTime: DateTime) = {
      val dayOfMonth = newTime.dayOfMonth().get
      val firstDate = newTime.withDayOfMonth(1)
      val firstDayOfWeek = firstDate.dayOfWeek().get
      val offset = firstDayOfWeek - 1
      ((dayOfMonth + offset) / 7) + 1
    } 
  }
 
  object DayOfYear extends Op1F1(TimeNamespace, "dayOfYear") with TimeFraction {
    def fraction(d: DateTime) = d.dayOfYear.get
  }

  object DayOfMonth extends Op1F1(TimeNamespace, "dayOfMonth") with TimeFraction {
    def fraction(d: DateTime) = d.dayOfMonth.get
  }

  object DayOfWeek extends Op1F1(TimeNamespace, "dayOfWeek") with TimeFraction {
    def fraction(d: DateTime) = d.dayOfWeek.get
  }

  object HourOfDay extends Op1F1(TimeNamespace, "hourOfDay") with TimeFraction {
    def fraction(d: DateTime) = d.hourOfDay.get
  }

  object MinuteOfHour extends Op1F1(TimeNamespace, "minuteOfHour") with TimeFraction {
    def fraction(d: DateTime) = d.minuteOfHour.get
  }

  object SecondOfMinute extends Op1F1(TimeNamespace, "secondOfMinute") with TimeFraction {
    def fraction(d: DateTime) = d.secondOfMinute.get
  }
    
  object MillisOfSecond extends Op1F1(TimeNamespace, "millisOfSecond") with TimeFraction {
    def fraction(d: DateTime) = d.millisOfSecond.get
  }

  trait TimeTruncation extends Op1F1 {
    val tpe = UnaryOperationType(JTextT, JTextT)
    def f1(ctx: EvaluationContext): F1 = CF1P("builtin::time::truncation") {
      case c: StrColumn => new Map1Column(c) with StrColumn {
        override def isDefinedAt(row: Int) = c.isDefinedAt(row) && isValidISO(c(row))

        def apply(row: Int) = {
          val time = c(row)
          
          val newTime = parseDateTime(time, true)
          fmt.print(newTime)
        }
      }
    }
    def spec[A <: SourceType](ctx: EvaluationContext): TransSpec[A] => TransSpec[A] =
      transSpec => trans.Map1(transSpec, f1(ctx))

    def fmt: DateTimeFormatter
  }

  object Date extends Op1F1(TimeNamespace, "date") with TimeTruncation {
    val fmt = ISODateTimeFormat.date()
  }

  object YearMonth extends Op1F1(TimeNamespace, "yearMonth") with TimeTruncation {
    val fmt = ISODateTimeFormat.yearMonth()
  }

  object YearDayOfYear extends Op1F1(TimeNamespace, "yearDayOfYear") with TimeTruncation {
    val fmt = ISODateTimeFormat.ordinalDate()
  }

  object MonthDay extends Op1F1(TimeNamespace, "monthDay") with TimeTruncation {
    val fmt = DateTimeFormat.forPattern("MM-dd")
  }

  object DateHour extends Op1F1(TimeNamespace, "dateHour") with TimeTruncation {
    val fmt = ISODateTimeFormat.dateHour()
  }

  object DateHourMinute extends Op1F1(TimeNamespace, "dateHourMin") with TimeTruncation {
    val fmt = ISODateTimeFormat.dateHourMinute()
  }

  object DateHourMinuteSecond extends Op1F1(TimeNamespace, "dateHourMinSec") with TimeTruncation {
    val fmt = ISODateTimeFormat.dateHourMinuteSecond()
  }

  object DateHourMinuteSecondMillis extends Op1F1(TimeNamespace, "dateHourMinSecMilli") with TimeTruncation {
    val fmt = ISODateTimeFormat.dateHourMinuteSecondMillis()
  }

  object TimeWithZone extends Op1F1(TimeNamespace, "timeWithZone") with TimeTruncation {
    val fmt = ISODateTimeFormat.time()
  }

  object TimeWithoutZone extends Op1F1(TimeNamespace, "timeWithoutZone") with TimeTruncation {
    val fmt = ISODateTimeFormat.hourMinuteSecondMillis()
  }

  object HourMinute extends Op1F1(TimeNamespace, "hourMin") with TimeTruncation {
    val fmt = ISODateTimeFormat.hourMinute()
  }

  object HourMinuteSecond extends Op1F1(TimeNamespace, "hourMinSec") with TimeTruncation {
    val fmt = ISODateTimeFormat.hourMinuteSecond()
  }
}
