package com.precog
package daze

import bytecode.Library
import bytecode.BuiltInFunc1
import bytecode.BuiltInFunc2
import yggdrasil._
import org.joda.time._
import org.joda.time.format._

trait GenOpcode extends Library {
  private val defaultUnaryOpcode = new java.util.concurrent.atomic.AtomicInteger(0)
  abstract class BIF1(val namespace: Vector[String], val name: String, val opcode: Int = defaultUnaryOpcode.getAndIncrement) extends BuiltInFunc1 {
    val operation: PartialFunction[SValue, SValue]
    val operandType: Option[SType]
  }

  private val defaultBinaryOpcode = new java.util.concurrent.atomic.AtomicInteger(0)
  abstract class BIF2(val namespace: Vector[String], val name: String, val opcode: Int = defaultBinaryOpcode.getAndIncrement) extends BuiltInFunc2 {
    val operation: PartialFunction[(SValue, SValue), SValue]
    val operandType: (Option[SType], Option[SType])
  }
}

trait ImplLibrary extends Library {
  lazy val lib1 = _lib1
  lazy val lib2 = _lib2

  def _lib1: Set[BIF1] = Set()
  def _lib2: Set[BIF2] = Set()
}

trait Stdlib extends Timelib

trait Timelib extends GenOpcode with ImplLibrary {
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
    HourMinuteSecond,

    Distinct
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
    MillisToISO
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

  object Distinct extends BIF1(Vector(), "distinct") {
    val operandType = Some(SString)
    val operation: PartialFunction[SValue, SValue] = {
      case SString(time) => SString(time)
    }
  }

  object ChangeTimeZone extends BIF2(Vector("std", "time"), "changeTimeZone") {
    val operandType = (Some(SString), Some(SString))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SString(time), SString(tz)) if (isValidISO(time) && isValidTimeZone(tz)) => 
        val format = ISODateTimeFormat.dateTime()
        val timeZone = DateTimeZone.forID(tz)
        val dateTime = new DateTime(time, timeZone)
        SString(format.print(dateTime))
    }
  }

  trait TimeBetween extends BIF2 {
    val operandType = (Some(SString), Some(SString)) 

    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SString(time1), SString(time2)) if (isValidISO(time1) && isValidISO(time2)) => 
        val newTime1 = ISODateTimeFormat.dateTime().withOffsetParsed.parseDateTime(time1)
        val newTime2 = ISODateTimeFormat.dateTime().withOffsetParsed.parseDateTime(time2)
        SLong(between(newTime1, newTime2))
    }

    def between(d1: DateTime, d2: DateTime): Long
  }

  object YearsBetween extends BIF2(Vector("std", "time"), "yearsBetween") with TimeBetween{ 
    def between(d1: DateTime, d2: DateTime) = Years.yearsBetween(d1, d2).getYears
  }

  object MonthsBetween extends BIF2(Vector("std", "time"), "monthsBetween") with TimeBetween{ 
    def between(d1: DateTime, d2: DateTime) = Months.monthsBetween(d1, d2).getMonths
  }

  object WeeksBetween extends BIF2(Vector("std", "time"), "weeksBetween") with TimeBetween{ 
    def between(d1: DateTime, d2: DateTime) = Weeks.weeksBetween(d1, d2).getWeeks
  }

  object DaysBetween extends BIF2(Vector("std", "time"), "daysBetween") with TimeBetween{ 
    def between(d1: DateTime, d2: DateTime) = Days.daysBetween(d1, d2).getDays
  }

  object HoursBetween extends BIF2(Vector("std", "time"), "hoursBetween") with TimeBetween{ 
    def between(d1: DateTime, d2: DateTime) = Hours.hoursBetween(d1, d2).getHours
  }

  object MinutesBetween extends BIF2(Vector("std", "time"), "minutesBetween") with TimeBetween{ 
    def between(d1: DateTime, d2: DateTime) = Minutes.minutesBetween(d1, d2).getMinutes
  }

  object SecondsBetween extends BIF2(Vector("std", "time"), "secondsBetween") with TimeBetween{ 
    def between(d1: DateTime, d2: DateTime) = Seconds.secondsBetween(d1, d2).getSeconds
  }

  object MillisBetween extends BIF2(Vector("std", "time"), "millisBetween") with TimeBetween{  
    def between(d1: DateTime, d2: DateTime) = math.abs(d1.getMillis - d2.getMillis)
  }

  object MillisToISO extends BIF2(Vector("std", "time"), "millisToISO") { 
    val operandType = (Some(SDecimal), Some(SString))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SDecimal(time), SString(tz)) if (time >= Long.MinValue && time <= Long.MaxValue && isValidTimeZone(tz)) =>  
        val format = ISODateTimeFormat.dateTime()
        val timeZone = DateTimeZone.forID(tz)
        val dateTime = new DateTime(time.toLong, timeZone)
        SString(format.print(dateTime))
    }
  }

  object GetMillis extends BIF1(Vector("std", "time"), "getMillis") { 
    val operandType = Some(SString)
    val operation: PartialFunction[SValue, SValue] = {
      case SString(time) if isValidISO(time) => 
        val newTime = ISODateTimeFormat.dateTime().withOffsetParsed.parseDateTime(time)
        SLong(newTime.getMillis)
    }    
  }

  object TimeZone extends BIF1(Vector("std", "time"), "timeZone") { 
    val operandType = Some(SString)
    val operation: PartialFunction[SValue, SValue] = {
      case SString(time) if isValidISO(time) => 
        val format = DateTimeFormat.forPattern("ZZ")
        val newTime = ISODateTimeFormat.dateTime().withOffsetParsed.parseDateTime(time)
        SString(format.print(newTime))
    }
  }

  object Season extends BIF1(Vector("std", "time"), "season") { 
    val operandType = Some(SString)
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
    }
  } 

  trait TimeFraction extends BIF1 {
    val operandType = Some(SString)
    val operation: PartialFunction[SValue, SValue] = {
      case SString(time) if isValidISO(time) => 
        val newTime = ISODateTimeFormat.dateTime().withOffsetParsed.parseDateTime(time)
        SLong(fraction(newTime))
    }

    def fraction(d: DateTime): Int
  }

  object Year extends BIF1(Vector("std", "time"), "year") with TimeFraction { 
    def fraction(d: DateTime) = d.year.get
  }

  object QuarterOfYear extends BIF1(Vector("std", "time"), "quarter") with TimeFraction { 
    def fraction(d: DateTime) = ((d.monthOfYear.get - 1) / 3) + 1
  }

  object MonthOfYear extends BIF1(Vector("std", "time"), "monthOfYear") with TimeFraction {
    def fraction(d: DateTime) = d.monthOfYear.get
  }

  object WeekOfYear extends BIF1(Vector("std", "time"), "weekOfYear") with TimeFraction {
    def fraction(d: DateTime) = d.weekOfWeekyear.get
  } 

  object WeekOfMonth extends BIF1(Vector("std", "time"), "weekOfMonth") with TimeFraction {
    def fraction(newTime: DateTime) = {
      val dayOfMonth = newTime.dayOfMonth().get
      val firstDate = newTime.withDayOfMonth(1)
      val firstDayOfWeek = firstDate.dayOfWeek().get
      val offset = firstDayOfWeek - 1
      ((dayOfMonth + offset) / 7) + 1
    }
  }
 
  object DayOfYear extends BIF1(Vector("std", "time"), "dayOfYear") with TimeFraction {
    def fraction(d: DateTime) = d.dayOfYear.get
  }

  object DayOfMonth extends BIF1(Vector("std", "time"), "dayOfMonth") with TimeFraction {
    def fraction(d: DateTime) = d.dayOfMonth.get
  }

  object DayOfWeek extends BIF1(Vector("std", "time"), "dayOfWeek") with TimeFraction {
    def fraction(d: DateTime) = d.dayOfWeek.get
  }

  object HourOfDay extends BIF1(Vector("std", "time"), "hourOfDay") with TimeFraction {  
    def fraction(d: DateTime) = d.hourOfDay.get
  }

  object MinuteOfHour extends BIF1(Vector("std", "time"), "minuteOfHour") with TimeFraction {
    def fraction(d: DateTime) = d.minuteOfHour.get
  }

  object SecondOfMinute extends BIF1(Vector("std", "time"), "secondOfMinute") with TimeFraction {
    def fraction(d: DateTime) = d.secondOfMinute.get
  }
    
  object MillisOfSecond extends BIF1(Vector("std", "time"), "millisOfSecond") with TimeFraction {
    def fraction(d: DateTime) = d.millisOfSecond.get
  }

  trait TimeTruncation extends BIF1 {
    val operandType = Some(SString)
    val operation: PartialFunction[SValue, SValue] = {
      case SString(time) if isValidISO(time) => 
        val newTime = ISODateTimeFormat.dateTime().withOffsetParsed.parseDateTime(time)
        SString(fmt.print(newTime))
    }

    def fmt: DateTimeFormatter
  }

  object Date extends BIF1(Vector("std", "time"), "date") with TimeTruncation {
    val fmt = ISODateTimeFormat.date()
  }

  object YearMonth extends BIF1(Vector("std", "time"), "yearMonth") with TimeTruncation {
    val fmt = ISODateTimeFormat.yearMonth()
  }

  object YearDayOfYear extends BIF1(Vector("std", "time"), "yearDayOfYear") with TimeTruncation {
    val fmt = ISODateTimeFormat.ordinalDate()
  }

  object MonthDay extends BIF1(Vector("std", "time"), "monthDay") with TimeTruncation {
    val fmt = DateTimeFormat.forPattern("MM-dd")
  }

  object DateHour extends BIF1(Vector("std", "time"), "dateHour") with TimeTruncation {
    val fmt = ISODateTimeFormat.dateHour()
  }

  object DateHourMinute extends BIF1(Vector("std", "time"), "dateHourMin") with TimeTruncation {
    val fmt = ISODateTimeFormat.dateHourMinute()
  }

  object DateHourMinuteSecond extends BIF1(Vector("std", "time"), "dateHourMinSec") with TimeTruncation {
    val fmt = ISODateTimeFormat.dateHourMinuteSecond()
  }

  object DateHourMinuteSecondMillis extends BIF1(Vector("std", "time"), "dateHourMinSecMilli") with TimeTruncation {
    val fmt = ISODateTimeFormat.dateHourMinuteSecondMillis()
  }

  object TimeWithZone extends BIF1(Vector("std", "time"), "timeWithZone") with TimeTruncation {
    val fmt = ISODateTimeFormat.time()
  }

  object TimeWithoutZone extends BIF1(Vector("std", "time"), "timeWithoutZone") with TimeTruncation {
    val fmt = ISODateTimeFormat.hourMinuteSecondMillis()
  }

  object HourMinute extends BIF1(Vector("std", "time"), "hourMin") with TimeTruncation {
    val fmt = ISODateTimeFormat.hourMinute()
  }

  object HourMinuteSecond extends BIF1(Vector("std", "time"), "hourMinSec") with TimeTruncation {
    val fmt = ISODateTimeFormat.hourMinuteSecond()
  }
}
