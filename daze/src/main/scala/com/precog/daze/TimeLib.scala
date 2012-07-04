package com.precog
package daze

import bytecode.Library

import yggdrasil._
import yggdrasil.table._

import org.joda.time._
import org.joda.time.format._

object TimeLib extends TimeLib

trait TimeLib extends GenOpcode with ImplLibrary {
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
    YearsBetween,
    MonthsBetween,
    WeeksBetween,
    DaysBetween,
    HoursBetween,
    MinutesBetween,
    SecondsBetween,
    MillisBetween,

    MillisToISO,
    ChangeTimeZone
  )

  private def isValidISO(str: String): Boolean = {
    try { new DateTime(str); true
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

  object ChangeTimeZone extends Op2(TimeNamespace, "changeTimeZone") {
    def f2: F2 = new CF2P({
      case (c1: StrColumn, c2: StrColumn) => new Map2Column(c1, c2) with StrColumn {
        def apply(row: Int) = {
          val time = c1(row)
          val tz = c2(row)

          if (isValidISO(time) && isValidTimeZone(tz)) {
            val newTime = ISODateTimeFormat.dateTimeParser().parseDateTime(time)
            val timeZone = DateTimeZone.forID(tz)
            val dateTime = new DateTime(newTime, timeZone)

            dateTime.toString()
          } else sys.error("todo")
        }
      }
    })
    
    /* val operandType = (Some(SString), Some(SString))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SString(time), SString(tz)) if (isValidISO(time) && isValidTimeZone(tz)) => 
        val format = ISODateTimeFormat.dateTime()
        val timeZone = DateTimeZone.forID(tz)
        val dateTime = new DateTime(time, timeZone)
        SString(format.print(dateTime))
    } */
  }

  trait TimeBetween extends Op2 {
    def f2: F2 = new CF2P({
      case (c1: StrColumn, c2: StrColumn) => new Map2Column(c1, c2) with LongColumn {
        def apply(row: Int) = {
          val time1 = c1(row)
          val time2 = c2(row)

          if (isValidISO(time1) && isValidISO(time2)) {
            val newTime1 = ISODateTimeFormat.dateTimeParser().withOffsetParsed.parseDateTime(time1)
            val newTime2 = ISODateTimeFormat.dateTimeParser().withOffsetParsed.parseDateTime(time2)

            between(newTime1, newTime2)
          } else sys.error("todo'")
        }

        def between(d1: DateTime, d2: DateTime): Long
      }
    })
    /* val operandType = (Some(SString), Some(SString)) 

    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SString(time1), SString(time2)) if (isValidISO(time1) && isValidISO(time2)) => 
        val newTime1 = ISODateTimeFormat.dateTime().withOffsetParsed.parseDateTime(time1)
        val newTime2 = ISODateTimeFormat.dateTime().withOffsetParsed.parseDateTime(time2)
        SDecimal(between(newTime1, newTime2))
    }

    def between(d1: DateTime, d2: DateTime): Long */
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
    def f2: F2 = new CF2P({
      case (c1: StrColumn, c2: StrColumn) => new Map2Column(c1, c2) with StrColumn {
        def apply(row: Int) = 
      }
    })
    
    /* val operandType = (Some(SDecimal), Some(SString))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SDecimal(time), SString(tz)) if (time >= Long.MinValue && time <= Long.MaxValue && isValidTimeZone(tz)) =>  
        val format = ISODateTimeFormat.dateTime()
        val timeZone = DateTimeZone.forID(tz)
        val dateTime = new DateTime(time.toLong, timeZone)
        SString(format.print(dateTime))
    } */
  }

  object GetMillis extends Op1(TimeNamespace, "getMillis") {
    def f1: F1 = new CF1P({
      case c: StrColumn => new Map1Column(c) with StrColumn {
        def apply(row: Int) = 
      }
    })
    
    /* val operandType = Some(SString)
    val operation: PartialFunction[SValue, SValue] = {
      case SString(time) if isValidISO(time) => 
        val newTime = ISODateTimeFormat.dateTime().withOffsetParsed.parseDateTime(time)
        SDecimal(newTime.getMillis)
    } */    
  }

  object TimeZone extends Op1(TimeNamespace, "timeZone") {
    def f1: F1 = new CF1P({
      case c: StrColumn => new Map1Column(c) with StrColumn {
        def apply(row: Int) = 
      }
    })
    
    /* val operandType = Some(SString)
    val operation: PartialFunction[SValue, SValue] = {
      case SString(time) if isValidISO(time) => 
        val format = DateTimeFormat.forPattern("ZZ")
        val newTime = ISODateTimeFormat.dateTime().withOffsetParsed.parseDateTime(time)
        SString(format.print(newTime))
    } */
  }

  object Season extends Op1(TimeNamespace, "season") {
    def f1: F1 = new CF1P({
      case c: StrColumn => new Map1Column(c) with StrColumn {
        def apply(row: Int) = 
      }
    })
    
    /* val operandType = Some(SString)
    val operation: PartialFunction[SValue, SValue] = {
      case SString(time) if isValidISO(time) => 
        val newTime = ISODateTimeFormat.dateTime().withOffsetParsed.parseDateTime(time)
        val day = newTime.dayOfYear.get
        SString(
          if (day >= 79 & day < 171) "spring"
          else if (day >= 171 & day < 265) "summer"
          else if (day >= 265 & day < 355) "fall"
          else "winter"
        )
    } */
  } 

  trait TimeFraction extends Op1 {
    /* val operandType = Some(SString)
    val operation: PartialFunction[SValue, SValue] = {
      case SString(time) if isValidISO(time) => 
        val newTime = ISODateTimeFormat.dateTime().withOffsetParsed.parseDateTime(time)
        SDecimal(fraction(newTime))
    }

    def fraction(d: DateTime): Int */
  }

  object Year extends Op1(TimeNamespace, "year") with TimeFraction {
    def f1: F1 = new CF1P({
      case c: StrColumn => new Map1Column(c) with StrColumn {
        def apply(row: Int) = 
      }
    })
    
    // def fraction(d: DateTime) = d.year.get
  }

  object QuarterOfYear extends Op1(TimeNamespace, "quarter") with TimeFraction {
    def f1: F1 = new CF1P({
      case c: StrColumn => new Map1Column(c) with StrColumn {
        def apply(row: Int) = 
      }
    })
    
    // def fraction(d: DateTime) = ((d.monthOfYear.get - 1) / 3) + 1
  }

  object MonthOfYear extends Op1(TimeNamespace, "monthOfYear") with TimeFraction {
    def f1: F1 = new CF1P({
      case c: StrColumn => new Map1Column(c) with StrColumn {
        def apply(row: Int) = 
      }
    })
    
    // def fraction(d: DateTime) = d.monthOfYear.get
  }

  object WeekOfYear extends Op1(TimeNamespace, "weekOfYear") with TimeFraction {
    def f1: F1 = new CF1P({
      case c: StrColumn => new Map1Column(c) with StrColumn {
        def apply(row: Int) = 
      }
    })
    
    // def fraction(d: DateTime) = d.weekOfWeekyear.get
  } 

  object WeekOfMonth extends Op1(TimeNamespace, "weekOfMonth") with TimeFraction {
    def f1: F1 = new CF1P({
      case c: StrColumn => new Map1Column(c) with StrColumn {
        def apply(row: Int) = 
      }
    })
    
    /* def fraction(newTime: DateTime) = {
      val dayOfMonth = newTime.dayOfMonth().get
      val firstDate = newTime.withDayOfMonth(1)
      val firstDayOfWeek = firstDate.dayOfWeek().get
      val offset = firstDayOfWeek - 1
      ((dayOfMonth + offset) / 7) + 1
    } */
  }
 
  object DayOfYear extends Op1(TimeNamespace, "dayOfYear") with TimeFraction {
    def f1: F1 = new CF1P({
      case c: StrColumn => new Map1Column(c) with StrColumn {
        def apply(row: Int) = 
      }
    })
    
    // def fraction(d: DateTime) = d.dayOfYear.get
  }

  object DayOfMonth extends Op1(TimeNamespace, "dayOfMonth") with TimeFraction {
    def f1: F1 = new CF1P({
      case c: StrColumn => new Map1Column(c) with StrColumn {
        def apply(row: Int) = 
      }
    })
    
    // def fraction(d: DateTime) = d.dayOfMonth.get
  }

  object DayOfWeek extends Op1(TimeNamespace, "dayOfWeek") with TimeFraction {
    def f1: F1 = new CF1P({
      case c: StrColumn => new Map1Column(c) with StrColumn {
        def apply(row: Int) = 
      }
    })
    
    // def fraction(d: DateTime) = d.dayOfWeek.get
  }

  object HourOfDay extends Op1(TimeNamespace, "hourOfDay") with TimeFraction {
    def f1: F1 = new CF1P({
      case c: StrColumn => new Map1Column(c) with StrColumn {
        def apply(row: Int) = 
      }
    })
    
    // def fraction(d: DateTime) = d.hourOfDay.get
  }

  object MinuteOfHour extends Op1(TimeNamespace, "minuteOfHour") with TimeFraction {
    def f1: F1 = new CF1P({
      case c: StrColumn => new Map1Column(c) with StrColumn {
        def apply(row: Int) = 
      }
    })
    
    // def fraction(d: DateTime) = d.minuteOfHour.get
  }

  object SecondOfMinute extends Op1(TimeNamespace, "secondOfMinute") with TimeFraction {
    def f1: F1 = new CF1P({
      case c: StrColumn => new Map1Column(c) with StrColumn {
        def apply(row: Int) = 
      }
    })
    
    // def fraction(d: DateTime) = d.secondOfMinute.get
  }
    
  object MillisOfSecond extends Op1(TimeNamespace, "millisOfSecond") with TimeFraction {
    def f1: F1 = new CF1P({
      case c: StrColumn => new Map1Column(c) with StrColumn {
        def apply(row: Int) = 
      }
    })
    
    // def fraction(d: DateTime) = d.millisOfSecond.get
  }

  trait TimeTruncation extends Op1 {
    /* val operandType = Some(SString)
    val operation: PartialFunction[SValue, SValue] = {
      case SString(time) if isValidISO(time) => 
        val newTime = ISODateTimeFormat.dateTime().withOffsetParsed.parseDateTime(time)
        SString(fmt.print(newTime))
    }

    def fmt: DateTimeFormatter */
  }

  object Date extends Op1(TimeNamespace, "date") with TimeTruncation {
    def f1: F1 = new CF1P({
      case c: StrColumn => new Map1Column(c) with StrColumn {
        def apply(row: Int) = 
      }
    })
    
    // val fmt = ISODateTimeFormat.date()
  }

  object YearMonth extends Op1(TimeNamespace, "yearMonth") with TimeTruncation {
    def f1: F1 = new CF1P({
      case c: StrColumn => new Map1Column(c) with StrColumn {
        def apply(row: Int) = 
      }
    })
    
    // val fmt = ISODateTimeFormat.yearMonth()
  }

  object YearDayOfYear extends Op1(TimeNamespace, "yearDayOfYear") with TimeTruncation {
    def f1: F1 = new CF1P({
      case c: StrColumn => new Map1Column(c) with StrColumn {
        def apply(row: Int) = 
      }
    })
    
    // val fmt = ISODateTimeFormat.ordinalDate()
  }

  object MonthDay extends Op1(TimeNamespace, "monthDay") with TimeTruncation {
    def f1: F1 = new CF1P({
      case c: StrColumn => new Map1Column(c) with StrColumn {
        def apply(row: Int) = 
      }
    })
    
    // val fmt = DateTimeFormat.forPattern("MM-dd")
  }

  object DateHour extends Op1(TimeNamespace, "dateHour") with TimeTruncation {
    def f1: F1 = new CF1P({
      case c: StrColumn => new Map1Column(c) with StrColumn {
        def apply(row: Int) = 
      }
    })
    
    // val fmt = ISODateTimeFormat.dateHour()
  }

  object DateHourMinute extends Op1(TimeNamespace, "dateHourMin") with TimeTruncation {
    def f1: F1 = new CF1P({
      case c: StrColumn => new Map1Column(c) with StrColumn {
        def apply(row: Int) = 
      }
    })
    
    // val fmt = ISODateTimeFormat.dateHourMinute()
  }

  object DateHourMinuteSecond extends Op1(TimeNamespace, "dateHourMinSec") with TimeTruncation {
    def f1: F1 = new CF1P({
      case c: StrColumn => new Map1Column(c) with StrColumn {
        def apply(row: Int) = 
      }
    })
    
    // val fmt = ISODateTimeFormat.dateHourMinuteSecond()
  }

  object DateHourMinuteSecondMillis extends Op1(TimeNamespace, "dateHourMinSecMilli") with TimeTruncation {
    def f1: F1 = new CF1P({
      case c: StrColumn => new Map1Column(c) with StrColumn {
        def apply(row: Int) = 
      }
    })
    
    // val fmt = ISODateTimeFormat.dateHourMinuteSecondMillis()
  }

  object TimeWithZone extends Op1(TimeNamespace, "timeWithZone") with TimeTruncation {
    def f1: F1 = new CF1P({
      case c: StrColumn => new Map1Column(c) with StrColumn {
        def apply(row: Int) = 
      }
    })
    
    // val fmt = ISODateTimeFormat.time()
  }

  object TimeWithoutZone extends Op1(TimeNamespace, "timeWithoutZone") with TimeTruncation {
    def f1: F1 = new CF1P({
      case c: StrColumn => new Map1Column(c) with StrColumn {
        def apply(row: Int) = 
      }
    })
    
    // val fmt = ISODateTimeFormat.hourMinuteSecondMillis()
  }

  object HourMinute extends Op1(TimeNamespace, "hourMin") with TimeTruncation {
    def f1: F1 = new CF1P({
      case c: StrColumn => new Map1Column(c) with StrColumn {
        def apply(row: Int) = 
      }
    })
    
    // val fmt = ISODateTimeFormat.hourMinute()
  }

  object HourMinuteSecond extends Op1(TimeNamespace, "hourMinSec") with TimeTruncation {
    def f1: F1 = new CF1P({
      case c: StrColumn => new Map1Column(c) with StrColumn {
        def apply(row: Int) = 
      }
    })
    
    // val fmt = ISODateTimeFormat.hourMinuteSecond()
  }
}
