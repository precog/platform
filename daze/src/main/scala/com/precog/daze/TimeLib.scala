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

import util.NumericComparisons
import util.DateTimeUtil._
import util.BitSetUtil

import bytecode._

import common._


import yggdrasil._
import yggdrasil.table._

import org.joda.time._
import org.joda.time.format._

import com.precog.util.DateTimeUtil.{parseDateTime, parseDateTimeFlexibly, isDateTimeFlexibly}
import com.precog.util.{BitSet, BitSetUtil}

import TransSpecModule._

import scala.collection.mutable.ArrayBuffer

trait TimeLibModule[M[+_]] extends ColumnarTableLibModule[M] {
  trait TimeLib extends ColumnarTableLib {
    import trans._
    import StdLib.{StrAndDateT, dateToStrCol}

    val TimeNamespace = Vector("std", "time")

    override def _lib1 = super._lib1 ++ Set(
      GetMillis,
      TimeZone,
      Season,
      TimeRange,

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
      HourMinuteSecond,

      DateHourMin,
      DateHourMinSec,
      DateHourMinSecMilli,
      HourMin,
      HourMinSec,

      ParseDateTimeFuzzy,
      ParsePeriod
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
      ParseDateTime,

      MinTimeOf,
      MaxTimeOf
    )

    //val textAndDate = JUnionT(JTextT, JDateT)

    DateTimeZone.setDefault(DateTimeZone.UTC)

    object ParseDateTime extends Op2F2(TimeNamespace, "parseDateTime") {
      val tpe = BinaryOperationType(StrAndDateT, JTextT, JDateT)
      def f2(ctx: MorphContext): F2 = CF2P("builtin::time::parseDateTime") {
        case (c1: DateColumn, c2: StrColumn) => c1

        case (c1: StrColumn, c2: StrColumn) => new DateColumn {
          def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row) && isValidFormat(c1(row), c2(row))

          def apply(row: Int): DateTime = {
            val time = c1(row)
            val fmt = c2(row)

            val format = DateTimeFormat.forPattern(fmt).withOffsetParsed()
            format.parseDateTime(time)
          }
        }
      }
    }

    object ParseDateTimeFuzzy extends Op1F1(TimeNamespace, "parseDateTimeFuzzy") {
      val tpe = UnaryOperationType(StrAndDateT, JDateT)
      def f1(ctx: MorphContext): F1 = CF1P("builtin::time::parseDateTimeFuzzy") {
        case (c: DateColumn) => c

        case (c: StrColumn) => new DateColumn {
          def isDefinedAt(row: Int): Boolean = if (!c.isDefinedAt(row)) {
            false
          } else {
            val s = c(row)
            isValidISO(s) || isDateTimeFlexibly(s)
          }
          def apply(row: Int) = {
            val s = c(row)
            try {
              parseDateTime(s, true)
            } catch {
              case e: IllegalArgumentException =>
                parseDateTimeFlexibly(s)
            }
          }
        }
      }
    }

    object ParsePeriod extends Op1F1(TimeNamespace, "parsePeriod") {
      val tpe = UnaryOperationType(JTextT, JPeriodT)
      def f1(ctx: MorphContext): F1 = CF1P("builtin::time::parsePeriod") {
        case (c: StrColumn) => new PeriodColumn {
          def isDefinedAt(row: Int) = c.isDefinedAt(row) && isValidPeriod(c(row))
          def apply(row: Int): Period = new Period(c(row))
        }
      }
    }

    //ok to `get` because CoerceToDate is defined for StrColumn
    def createDateCol(c: StrColumn) = cf.util.CoerceToDate(c) collect { case (dc: DateColumn) => dc } get

    object ChangeTimeZone extends Op2F2(TimeNamespace, "changeTimeZone") {
      val tpe = BinaryOperationType(StrAndDateT, JTextT, JDateT)

      def f2(ctx: MorphContext): F2 = CF2P("builtin::time::changeTimeZone") {
        case (c1: DateColumn, c2: StrColumn) => newColumn(c1, c2)
        case (c1: StrColumn, c2: StrColumn) => newColumn(createDateCol(c1), c2)
      }

      def newColumn(c1: DateColumn, c2: StrColumn): DateColumn = new DateColumn {
        def isDefinedAt(row: Int) =
          c1.isDefinedAt(row) && c2.isDefinedAt(row) && isValidTimeZone(c2(row))

        def apply(row: Int) = {
          val time = c1(row)
          val tz = c2(row)
          val timeZone = DateTimeZone.forID(tz)
          new DateTime(time, timeZone)
        }
      }
    }

    object TimeRange extends Op1(TimeNamespace, "range") {
      val start = "start"
      val end = "end"
      val step = "step"

      val tpe = UnaryOperationType(
        JObjectFixedT(Map(
          start -> JDateT,
          end -> JDateT,
          step -> JPeriodT)),
        JArrayHomogeneousT(JDateT))

      override val idPolicy = IdentityPolicy.Retain.Merge

      def scanner = new CScanner {
        type A = Unit
        def init = ()

        def scan(a: A, cols: Map[ColumnRef, Column], range: Range): (A, Map[ColumnRef, Column]) = {
          val startCol = cols collectFirst { case (ref, col: DateColumn) if ref.selector == CPath(start) => col }
          val endCol = cols collectFirst { case (ref, col: DateColumn) if ref.selector == CPath(end) => col }
          val stepCol = cols collectFirst { case (ref, col: PeriodColumn) if ref.selector == CPath(step) => col }

          val rawCols: Array[Column] = {
            if (startCol.isEmpty || endCol.isEmpty || stepCol.isEmpty) Array.empty[Column]
            else Array(startCol.get, endCol.get, stepCol.get)
          }

          val baseDefined = BitSetUtil.filteredRange(range.start, range.end) {
            i => Column.isDefinedAtAll(rawCols, i)
          }

          val definedRange = range.filter(baseDefined(_))

          val dateTimeArrays = Array.fill(range.length)(Array.empty[DateTime])

          val dateTimes: Array[Array[DateTime]] = {
            definedRange.toArray map { i =>
              val startTime = startCol.get(i)
              val endTime = endCol.get(i)
              val stepPeriod = stepCol.get(i)

              var rowAcc = ArrayBuffer(startTime)
              var lastTime = startTime

              while (lastTime.plus(stepPeriod).compareTo(endTime) <= 0) {
                val timePlus = lastTime.plus(stepPeriod)
                rowAcc = rowAcc :+ timePlus

                lastTime = timePlus
              }

              dateTimeArrays(i) = rowAcc.toArray
            }
            dateTimeArrays
          }

          // creates a BitSet for each array-column with index `idx`
          def defined(idx: Int): BitSet = {
            val bools = dateTimes map { _.length > idx }
            val indices = bools.zipWithIndex collect { case (true, idx) => idx }
            
            BitSetUtil.create(indices)
          }

          // creates the DateTime values for each array-column with index `idx`
          def dateCol(idx: Int): Array[DateTime] = dateTimes map { arr => 
            if (arr.length > idx) arr(idx)
            else new DateTime()
          }

          val result = {
            val lengths = dateTimes map { _.length }

            (0 until lengths.max) map { idx => 
              val colRef = ColumnRef(CPathIndex(idx), CDate)
              val col = ArrayDateColumn(defined(idx), dateCol(idx))
              (colRef, col)
            } toMap
          }

          ((), result)
        }
      }

      def spec[A <: SourceType](ctx: MorphContext)(source: TransSpec[A]): TransSpec[A] =
        Scan(source, scanner)
    }

    trait ExtremeTime extends Op2F2 {
      val tpe = BinaryOperationType(StrAndDateT, StrAndDateT, JDateT)
      def f2(ctx: MorphContext): F2 = CF2P("builtin::time::extremeTime") {
        case (c1: StrColumn, c2: StrColumn) =>
          val dateCol1 = createDateCol(c1)
          val dateCol2 = createDateCol(c2)

          new Map2Column(dateCol1, dateCol2) with DateColumn {
            def apply(row: Int) = computeExtreme(dateCol1(row), dateCol2(row))
          }

        case (c1: DateColumn, c2: StrColumn) =>
          val dateCol2 = createDateCol(c2)

          new Map2Column(c1, dateCol2) with DateColumn {
            def apply(row: Int) = computeExtreme(c1(row), dateCol2(row))
          }

        case (c1: StrColumn, c2: DateColumn) =>
          val dateCol1 = createDateCol(c1)

          new Map2Column(dateCol1, c2) with DateColumn {
            def apply(row: Int) = computeExtreme(dateCol1(row), c2(row))
          }

        case (c1: DateColumn, c2: DateColumn) => new Map2Column(c1, c2) with DateColumn {
          def apply(row: Int) = computeExtreme(c1(row), c2(row))
        }
      }

      def computeExtreme(t1: DateTime, t2: DateTime): DateTime
    }

    object MinTimeOf extends Op2F2(TimeNamespace, "minTimeOf") with ExtremeTime { 
      def computeExtreme(t1: DateTime, t2: DateTime): DateTime = {
        val res: Int = NumericComparisons.compare(t1, t2)
        if (res < 0) t1
        else t2
      }
    }

    object MaxTimeOf extends Op2F2(TimeNamespace, "maxTimeOf") with ExtremeTime { 
      def computeExtreme(t1: DateTime, t2: DateTime): DateTime = {
        val res: Int = NumericComparisons.compare(t1, t2)
        if (res > 0) t1
        else t2
      }
    }

    trait TimePlus extends Op2F2 {
      val tpe = BinaryOperationType(StrAndDateT, JNumberT, JDateT)
      def f2(ctx: MorphContext): F2 = CF2P("builtin::time::timePlus") {
        case (c1: StrColumn, c2: LongColumn) =>
          val dateCol1 = createDateCol(c1)

          new Map2Column(dateCol1, c2) with DateColumn {
            def apply(row: Int) = plus(dateCol1(row), c2(row).toInt)
          }      

        case (c1: StrColumn, c2: NumColumn) =>
          val dateCol1 = createDateCol(c1)

          new Map2Column(dateCol1, c2) with DateColumn {
            def apply(row: Int) = plus(dateCol1(row), c2(row).toInt)
          }      

        case (c1: StrColumn, c2: DoubleColumn) =>
          val dateCol1 = createDateCol(c1)

          new Map2Column(dateCol1, c2) with DateColumn {
            def apply(row: Int) = plus(dateCol1(row), c2(row).toInt)
          }      

        case (c1: DateColumn, c2: LongColumn) => new Map2Column(c1, c2) with DateColumn {
          def apply(row: Int) = plus(c1(row), c2(row).toInt)
        }      

        case (c1: DateColumn, c2: NumColumn) => new Map2Column(c1, c2) with DateColumn {
          def apply(row: Int) = plus(c1(row), c2(row).toInt)
        }      

        case (c1: DateColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DateColumn {
          def apply(row: Int) = plus(c1(row), c2(row).toInt)
        }
      }

      def plus(d: DateTime, i: Int): DateTime
    }

    object YearsPlus extends Op2F2(TimeNamespace, "yearsPlus") with TimePlus{ 
      def plus(d: DateTime, i: Int) = d.plus(Period.years(i))
    }

    object MonthsPlus extends Op2F2(TimeNamespace, "monthsPlus") with TimePlus{ 
      def plus(d: DateTime, i: Int) = d.plus(Period.months(i))
    }

    object WeeksPlus extends Op2F2(TimeNamespace, "weeksPlus") with TimePlus{ 
      def plus(d: DateTime, i: Int) = d.plus(Period.weeks(i))
    }

    object DaysPlus extends Op2F2(TimeNamespace, "daysPlus") with TimePlus{ 
      def plus(d: DateTime, i: Int) = d.plus(Period.days(i))
    }

    object HoursPlus extends Op2F2(TimeNamespace, "hoursPlus") with TimePlus{ 
      def plus(d: DateTime, i: Int) = d.plus(Period.hours(i))
    }

    object MinutesPlus extends Op2F2(TimeNamespace, "minutesPlus") with TimePlus{ 
      def plus(d: DateTime, i: Int) = d.plus(Period.minutes(i))
    }

    object SecondsPlus extends Op2F2(TimeNamespace, "secondsPlus") with TimePlus{ 
      def plus(d: DateTime, i: Int) = d.plus(Period.seconds(i))
    }

    object MillisPlus extends Op2F2(TimeNamespace, "millisPlus") with TimePlus{
      def plus(d: DateTime, i: Int) = d.plus(Period.millis(i))
    }
    
    trait TimeBetween extends Op2F2 {
      val tpe = BinaryOperationType(StrAndDateT, StrAndDateT, JNumberT)
      def f2(ctx: MorphContext): F2 = CF2P("builtin::time::timeBetween") {
        case (c1: StrColumn, c2: StrColumn) =>
          val dateCol1 = createDateCol(c1)
          val dateCol2 = createDateCol(c2)

        new Map2Column(dateCol1, dateCol2) with LongColumn {
          def apply(row: Int) = between(dateCol1(row), dateCol2(row))
        }

        case (c1: DateColumn, c2: StrColumn) =>
          val dateCol2 = createDateCol(c2)

        new Map2Column(c1, dateCol2) with LongColumn {
          def apply(row: Int) = between(c1(row), dateCol2(row))
        }

        case (c1: StrColumn, c2: DateColumn) =>
          val dateCol1 = createDateCol(c1)

        new Map2Column(dateCol1, c2) with LongColumn {
          def apply(row: Int) = between(dateCol1(row), c2(row))
        }

        case (c1: DateColumn, c2: DateColumn) => new Map2Column(c1, c2) with LongColumn {
          def apply(row: Int) = between(c1(row), c2(row))
        }
      }

      def between(d1: DateTime, d2: DateTime): Long
    }

    object YearsBetween extends Op2F2(TimeNamespace, "yearsBetween") with TimeBetween{
      def between(d1: DateTime, d2: DateTime) = Years.yearsBetween(d1, d2).getYears
    }

    object MonthsBetween extends Op2F2(TimeNamespace, "monthsBetween") with TimeBetween{
      def between(d1: DateTime, d2: DateTime) = Months.monthsBetween(d1, d2).getMonths
    }

    object WeeksBetween extends Op2F2(TimeNamespace, "weeksBetween") with TimeBetween{
      def between(d1: DateTime, d2: DateTime) = Weeks.weeksBetween(d1, d2).getWeeks
    }

    object DaysBetween extends Op2F2(TimeNamespace, "daysBetween") with TimeBetween{
      def between(d1: DateTime, d2: DateTime) = Days.daysBetween(d1, d2).getDays
    }

    object HoursBetween extends Op2F2(TimeNamespace, "hoursBetween") with TimeBetween{
      def between(d1: DateTime, d2: DateTime) = Hours.hoursBetween(d1, d2).getHours
    }

    object MinutesBetween extends Op2F2(TimeNamespace, "minutesBetween") with TimeBetween{
      def between(d1: DateTime, d2: DateTime) = Minutes.minutesBetween(d1, d2).getMinutes
    }

    object SecondsBetween extends Op2F2(TimeNamespace, "secondsBetween") with TimeBetween{
      def between(d1: DateTime, d2: DateTime) = Seconds.secondsBetween(d1, d2).getSeconds
    }

    object MillisBetween extends Op2F2(TimeNamespace, "millisBetween") with TimeBetween{
      def between(d1: DateTime, d2: DateTime) = d2.getMillis - d1.getMillis
    }

    object MillisToISO extends Op2F2(TimeNamespace, "millisToISO") {
      def checkDefined(c1: Column, c2: StrColumn, row: Int) =
        c1.isDefinedAt(row) && c2.isDefinedAt(row) && isValidTimeZone(c2(row))

      val tpe = BinaryOperationType(JNumberT, JTextT, JDateT)
      def f2(ctx: MorphContext): F2 = CF2P("builtin::time::millisToIso") {
        case (c1: LongColumn, c2: StrColumn) => new DateColumn {
          def isDefinedAt(row: Int) = checkDefined(c1, c2, row)

          def apply(row: Int) = { 
            val time = c1(row)
            val tz = c2(row)

            val timeZone = DateTimeZone.forID(tz)
            new DateTime(time.toLong, timeZone)
          }
        }      

        case (c1: NumColumn, c2: StrColumn) => new DateColumn {
          def isDefinedAt(row: Int) = checkDefined(c1, c2, row) && c1(row) >= Long.MinValue && c1(row) <= Long.MaxValue

          def apply(row: Int) = { 
            val time = c1(row)
            val tz = c2(row)

            val timeZone = DateTimeZone.forID(tz)
            new DateTime(time.toLong, timeZone)
          }
        }

        case (c1: DoubleColumn, c2: StrColumn) => new DateColumn {
          def isDefinedAt(row: Int) = checkDefined(c1, c2, row) && c1(row) >= Long.MinValue && c1(row) <= Long.MaxValue

          def apply(row: Int) = { 
            val time = c1(row)
            val tz = c2(row)

            val timeZone = DateTimeZone.forID(tz)
            new DateTime(time.toLong, timeZone)
          }
        }
      }
    }

    object GetMillis extends Op1F1(TimeNamespace, "getMillis") {
      val tpe = UnaryOperationType(StrAndDateT, JNumberT)
      def f1(ctx: MorphContext): F1 = CF1P("builtin::time::getMillis") {
        case (c: StrColumn) =>  
          val dateCol = createDateCol(c)

          new Map1Column(dateCol) with LongColumn {
            def apply(row: Int) = dateCol(row).getMillis()
        }
        case (c: DateColumn) => new Map1Column(c) with LongColumn {
          def apply(row: Int) = c(row).getMillis()
        }
      }
    }

    object TimeZone extends Op1F1(TimeNamespace, "timeZone") {
      val tpe = UnaryOperationType(StrAndDateT, JTextT)
      def f1(ctx: MorphContext): F1 = CF1P("builtin::time::timeZone") {
        case (c: StrColumn) =>
          val dateCol = createDateCol(c)

          new Map1Column(dateCol) with StrColumn {
            def apply(row: Int) = {
              val time = dateCol(row)
              val format = DateTimeFormat.forPattern("ZZ")

              format.print(time)
            }
          }
        case (c: DateColumn) => new Map1Column(c) with StrColumn {
          def apply(row: Int) = { 
            val time = c(row)
            val format = DateTimeFormat.forPattern("ZZ")

            format.print(time)
          }
        }
      }
    }

    object Season extends Op1F1(TimeNamespace, "season") {
      def determineSeason(time: DateTime): String = { 
        val day = time.dayOfYear.get
        
        if (day >= 79 & day < 171) "spring"
        else if (day >= 171 & day < 265) "summer"
        else if (day >= 265 & day < 355) "fall"
        else "winter"
      }
    
      val tpe = UnaryOperationType(StrAndDateT, JTextT)
      def f1(ctx: MorphContext): F1 = CF1P("builtin::time::season") {
        case (c: StrColumn) =>
          val dateCol = createDateCol(c)

          new Map1Column(dateCol) with StrColumn {
            def apply(row: Int) = determineSeason(dateCol(row))
          }
        case (c: DateColumn) => new Map1Column(c) with StrColumn {
          def apply(row: Int) = determineSeason(c(row))
        }
      }
    } 

    trait TimeFraction extends Op1F1 {
      val tpe = UnaryOperationType(StrAndDateT, JNumberT)
      def f1(ctx: MorphContext): F1 = CF1P("builtin::time::timeFraction") {
        case (c: StrColumn) =>
          val dateCol = createDateCol(c)

          new Map1Column(dateCol) with LongColumn {
            def apply(row: Int) = fraction(dateCol(row))
          }
        case (c: DateColumn) => new Map1Column(c) with LongColumn {
          def apply(row: Int) = fraction(c(row))
        }
      }

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
      val tpe = UnaryOperationType(StrAndDateT, JTextT)
      def f1(ctx: MorphContext): F1 = CF1P("builtin::time::truncation") {
        case (c: StrColumn) =>
          val dateCol = createDateCol(c)

          new Map1Column(dateCol) with StrColumn {
            def apply(row: Int) = fmt.print(dateCol(row))
          }
        case (c: DateColumn) => new Map1Column(c) with StrColumn {
          def apply(row: Int) = fmt.print(c(row))
        }
      }

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

    object DateHourMin extends Op1F1(TimeNamespace, "dateHourMin") with TimeTruncation {
      val fmt = ISODateTimeFormat.dateHourMinute()
      override val deprecation = Some("use dateHourMinute instead")
    }

    object DateHourMinute extends Op1F1(TimeNamespace, "dateHourMinute") with TimeTruncation {
      val fmt = ISODateTimeFormat.dateHourMinute()
    }

    object DateHourMinSec extends Op1F1(TimeNamespace, "dateHourMinSec") with TimeTruncation {
      val fmt = ISODateTimeFormat.dateHourMinuteSecond()
      override val deprecation = Some("use dateHourMinuteSecond instead")
    }

    object DateHourMinuteSecond extends Op1F1(TimeNamespace, "dateHourMinuteSecond") with TimeTruncation {
      val fmt = ISODateTimeFormat.dateHourMinuteSecond()
    }

    object DateHourMinSecMilli extends Op1F1(TimeNamespace, "dateHourMinSecMilli") with TimeTruncation {
      val fmt = ISODateTimeFormat.dateHourMinuteSecondMillis()
      override val deprecation = Some("use dateHourMinuteSecondMillis instead")
    }

    object DateHourMinuteSecondMillis extends Op1F1(TimeNamespace, "dateHourMinuteSecondMillis") with TimeTruncation {
      val fmt = ISODateTimeFormat.dateHourMinuteSecondMillis()
    }

    object TimeWithZone extends Op1F1(TimeNamespace, "timeWithZone") with TimeTruncation {
      val fmt = ISODateTimeFormat.time()
    }

    object TimeWithoutZone extends Op1F1(TimeNamespace, "timeWithoutZone") with TimeTruncation {
      val fmt = ISODateTimeFormat.hourMinuteSecondMillis()
    }

    object HourMin extends Op1F1(TimeNamespace, "hourMin") with TimeTruncation {
      val fmt = ISODateTimeFormat.hourMinute()
      override val deprecation = Some("use hourMinute instead")
    }

    object HourMinute extends Op1F1(TimeNamespace, "hourMinute") with TimeTruncation {
      val fmt = ISODateTimeFormat.hourMinute()
    }

    object HourMinSec extends Op1F1(TimeNamespace, "hourMinSec") with TimeTruncation {
      val fmt = ISODateTimeFormat.hourMinuteSecond()
      override val deprecation = Some("use hourMinuteSecond instead")
    }

    object HourMinuteSecond extends Op1F1(TimeNamespace, "hourMinuteSecond") with TimeTruncation {
      val fmt = ISODateTimeFormat.hourMinuteSecond()
    }
  }
}
