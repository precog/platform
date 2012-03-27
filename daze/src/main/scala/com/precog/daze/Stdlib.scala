package com.precog
package daze

import bytecode.Library
import bytecode.BuiltInFunc1
import bytecode.BuiltInFunc2
import yggdrasil._
import org.joda.time._
import org.joda.time.format._

trait GenOpcode extends ImplLibrary {
  private val defaultUnaryOpcode = new java.util.concurrent.atomic.AtomicInteger(0)
  abstract class BIF1(val namespace: Vector[String], val name: String, val opcode: Int = defaultUnaryOpcode.getAndIncrement) extends BuiltInFunc1Impl 

  private val defaultBinaryOpcode = new java.util.concurrent.atomic.AtomicInteger(0)
  abstract class BIF2(val namespace: Vector[String], val name: String, val opcode: Int = defaultBinaryOpcode.getAndIncrement) extends BuiltInFunc2Impl
}

trait ImplLibrary extends Library {
  lazy val lib1 = _lib1
  lazy val lib2 = _lib2

  def _lib1: Set[BIF1] = Set()
  def _lib2: Set[BIF2] = Set()

  trait BuiltInFunc1Impl extends BuiltInFunc1 {
    val operation: PartialFunction[SValue, SValue]
    val operandType: Option[SType]
  }

  trait BuiltInFunc2Impl extends BuiltInFunc2 {
    val operation: PartialFunction[(SValue, SValue), SValue]
    val operandType: (Option[SType], Option[SType])
  }

  type BIF1 <: BuiltInFunc1Impl
  type BIF2 <: BuiltInFunc2Impl
}

trait Stdlib extends Timelib with Infixlib with Genlib

trait Infixlib extends ImplLibrary with GenOpcode {
  object Infix {
    val InfixNamespace = Vector("std", "infix")

    object Add extends BIF2(InfixNamespace, "add") {
      val operation: PartialFunction[(SValue, SValue), SValue] = {
        case (SDecimal(l), SDecimal(r)) => SDecimal(l + r)
      }

      val operandType = (Some(SDecimal), Some(SDecimal))
    }

    object Sub extends BIF2(InfixNamespace, "subtract") {
      val operation: PartialFunction[(SValue, SValue), SValue] = {
        case (SDecimal(l), SDecimal(r)) => SDecimal(l - r)
      }

      val operandType = (Some(SDecimal), Some(SDecimal))
    }

    object Mul extends BIF2(InfixNamespace, "multiply") {
      val operation: PartialFunction[(SValue, SValue), SValue] = {
        case (SDecimal(l), SDecimal(r)) => SDecimal(l * r)
      }

      val operandType = (Some(SDecimal), Some(SDecimal))
    }

    object Div extends BIF2(InfixNamespace, "divide") {
      val operation: PartialFunction[(SValue, SValue), SValue] = {
        case (SDecimal(l), SDecimal(r)) if r != BigDecimal(0) => SDecimal(l / r)
      }

      val operandType = (Some(SDecimal), Some(SDecimal))
    }

    object Lt extends BIF2(InfixNamespace, "lt") {
      val operation: PartialFunction[(SValue, SValue), SValue] = {
        case (SDecimal(l), SDecimal(r)) => SBoolean(l < r)
      }

      val operandType = (Some(SDecimal), Some(SDecimal))
    }

    object LtEq extends BIF2(InfixNamespace, "lte") {
      val operation: PartialFunction[(SValue, SValue), SValue] = {
        case (SDecimal(l), SDecimal(r)) => SBoolean(l <= r)
      }

      val operandType = (Some(SDecimal), Some(SDecimal))
    }

    object Gt extends BIF2(InfixNamespace, "gt") {
      val operation: PartialFunction[(SValue, SValue), SValue] = {
        case (SDecimal(l), SDecimal(r)) => SBoolean(l > r)
      }

      val operandType = (Some(SDecimal), Some(SDecimal))
    }

    object GtEq extends BIF2(InfixNamespace, "gte") {
      val operation: PartialFunction[(SValue, SValue), SValue] = {
        case (SDecimal(l), SDecimal(r)) => SBoolean(l >= r)
      }

      val operandType = (Some(SDecimal), Some(SDecimal))
    }

    object Eq extends BIF2(InfixNamespace, "eq") {
      val operation: PartialFunction[(SValue, SValue), SValue] = {
        case (a, b) => SBoolean(a == b)
      }

      val operandType = (None, None)
    }

    object NotEq extends BIF2(InfixNamespace, "ne") {
      val operation: PartialFunction[(SValue, SValue), SValue] = {
        case (a, b) => SBoolean(a != b)
      }

      val operandType = (None, None)
    }

    object And extends BIF2(InfixNamespace, "and") {
      val operation: PartialFunction[(SValue, SValue), SValue] = {
        case (v1: SBooleanValue, v2: SBooleanValue) => v1 && v2
      }

      val operandType = (Some(SBoolean), Some(SBoolean))
    }

    object Or extends BIF2(InfixNamespace, "or") {
      val operation: PartialFunction[(SValue, SValue), SValue] = {
        case (v1: SBooleanValue, v2: SBooleanValue) => v1 || v2
      }

      val operandType = (Some(SBoolean), Some(SBoolean))
    }

    object WrapObject extends BIF2(InfixNamespace, "wrap") {
      val operation: PartialFunction[(SValue, SValue), SValue] = {
        case (SString(key), value) => SObject(Map(key -> value))
      }

      val operandType = (Some(SString), None)
    }

    object JoinObject extends BIF2(InfixNamespace, "merge") {
      val operation: PartialFunction[(SValue, SValue), SValue] = {
        case (SObject(left), SObject(right)) => SObject(left ++ right)
      }

      val operandType = (Some(SObject), Some(SObject))
    }

    object JoinArray extends BIF2(InfixNamespace, "concat") {
      val operation: PartialFunction[(SValue, SValue), SValue] = {
        case (SArray(left), SArray(right)) => SArray(left ++ right)
      }

      val operandType = (Some(SArray), Some(SArray))
    }

    object ArraySwap extends BIF2(InfixNamespace, "swap") {
      val operation: PartialFunction[(SValue, SValue), SValue] = {
        case (SArray(arr), SDecimal(i)) if i.isValidInt && (i.toInt >= 0 && i.toInt < arr.length) => 
          val (left, right) = arr splitAt i.toInt
          SArray(left.init ++ Vector(right.head, left.last) ++ right.tail)
      }

      val operandType = (Some(SArray), Some(SDecimal))
    }

    object DerefObject extends BIF2(InfixNamespace, "get") {
      val operation: PartialFunction[(SValue, SValue), SValue] = {
        case (SObject(obj), SString(key)) if obj.contains(key) => obj(key)
      }

      val operandType = (Some(SObject), Some(SString))
    }

    object DerefArray extends BIF2(InfixNamespace, "valueAt") {
      val operation: PartialFunction[(SValue, SValue), SValue] = {
        case (SArray(arr), SDecimal(i)) if i.isValidInt && arr.length < i.toInt => arr(i.toInt)
      }

      val operandType = (Some(SArray), Some(SDecimal))
    }
  }
}

object Infixlib extends Infixlib

trait Timelib extends GenOpcode with ImplLibrary {
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

  object ChangeTimeZone extends BIF2(TimeNamespace, "changeTimeZone") {
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
        SDecimal(between(newTime1, newTime2))
    }

    def between(d1: DateTime, d2: DateTime): Long
  }

  object YearsBetween extends BIF2(TimeNamespace, "yearsBetween") with TimeBetween{ 
    def between(d1: DateTime, d2: DateTime) = Years.yearsBetween(d1, d2).getYears
  }

  object MonthsBetween extends BIF2(TimeNamespace, "monthsBetween") with TimeBetween{ 
    def between(d1: DateTime, d2: DateTime) = Months.monthsBetween(d1, d2).getMonths
  }

  object WeeksBetween extends BIF2(TimeNamespace, "weeksBetween") with TimeBetween{ 
    def between(d1: DateTime, d2: DateTime) = Weeks.weeksBetween(d1, d2).getWeeks
  }

  object DaysBetween extends BIF2(TimeNamespace, "daysBetween") with TimeBetween{ 
    def between(d1: DateTime, d2: DateTime) = Days.daysBetween(d1, d2).getDays
  }

  object HoursBetween extends BIF2(TimeNamespace, "hoursBetween") with TimeBetween{ 
    def between(d1: DateTime, d2: DateTime) = Hours.hoursBetween(d1, d2).getHours
  }

  object MinutesBetween extends BIF2(TimeNamespace, "minutesBetween") with TimeBetween{ 
    def between(d1: DateTime, d2: DateTime) = Minutes.minutesBetween(d1, d2).getMinutes
  }

  object SecondsBetween extends BIF2(TimeNamespace, "secondsBetween") with TimeBetween{ 
    def between(d1: DateTime, d2: DateTime) = Seconds.secondsBetween(d1, d2).getSeconds
  }

  object MillisBetween extends BIF2(TimeNamespace, "millisBetween") with TimeBetween{  
    def between(d1: DateTime, d2: DateTime) = math.abs(d1.getMillis - d2.getMillis)
  }

  object MillisToISO extends BIF2(TimeNamespace, "millisToISO") { 
    val operandType = (Some(SDecimal), Some(SString))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SDecimal(time), SString(tz)) if (time >= Long.MinValue && time <= Long.MaxValue && isValidTimeZone(tz)) =>  
        val format = ISODateTimeFormat.dateTime()
        val timeZone = DateTimeZone.forID(tz)
        val dateTime = new DateTime(time.toLong, timeZone)
        SString(format.print(dateTime))
    }
  }

  object GetMillis extends BIF1(TimeNamespace, "getMillis") { 
    val operandType = Some(SString)
    val operation: PartialFunction[SValue, SValue] = {
      case SString(time) if isValidISO(time) => 
        val newTime = ISODateTimeFormat.dateTime().withOffsetParsed.parseDateTime(time)
        SDecimal(newTime.getMillis)
    }    
  }

  object TimeZone extends BIF1(TimeNamespace, "timeZone") { 
    val operandType = Some(SString)
    val operation: PartialFunction[SValue, SValue] = {
      case SString(time) if isValidISO(time) => 
        val format = DateTimeFormat.forPattern("ZZ")
        val newTime = ISODateTimeFormat.dateTime().withOffsetParsed.parseDateTime(time)
        SString(format.print(newTime))
    }
  }

  object Season extends BIF1(TimeNamespace, "season") { 
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
        SDecimal(fraction(newTime))
    }

    def fraction(d: DateTime): Int
  }

  object Year extends BIF1(TimeNamespace, "year") with TimeFraction { 
    def fraction(d: DateTime) = d.year.get
  }

  object QuarterOfYear extends BIF1(TimeNamespace, "quarter") with TimeFraction { 
    def fraction(d: DateTime) = ((d.monthOfYear.get - 1) / 3) + 1
  }

  object MonthOfYear extends BIF1(TimeNamespace, "monthOfYear") with TimeFraction {
    def fraction(d: DateTime) = d.monthOfYear.get
  }

  object WeekOfYear extends BIF1(TimeNamespace, "weekOfYear") with TimeFraction {
    def fraction(d: DateTime) = d.weekOfWeekyear.get
  } 

  object WeekOfMonth extends BIF1(TimeNamespace, "weekOfMonth") with TimeFraction {
    def fraction(newTime: DateTime) = {
      val dayOfMonth = newTime.dayOfMonth().get
      val firstDate = newTime.withDayOfMonth(1)
      val firstDayOfWeek = firstDate.dayOfWeek().get
      val offset = firstDayOfWeek - 1
      ((dayOfMonth + offset) / 7) + 1
    }
  }
 
  object DayOfYear extends BIF1(TimeNamespace, "dayOfYear") with TimeFraction {
    def fraction(d: DateTime) = d.dayOfYear.get
  }

  object DayOfMonth extends BIF1(TimeNamespace, "dayOfMonth") with TimeFraction {
    def fraction(d: DateTime) = d.dayOfMonth.get
  }

  object DayOfWeek extends BIF1(TimeNamespace, "dayOfWeek") with TimeFraction {
    def fraction(d: DateTime) = d.dayOfWeek.get
  }

  object HourOfDay extends BIF1(TimeNamespace, "hourOfDay") with TimeFraction {  
    def fraction(d: DateTime) = d.hourOfDay.get
  }

  object MinuteOfHour extends BIF1(TimeNamespace, "minuteOfHour") with TimeFraction {
    def fraction(d: DateTime) = d.minuteOfHour.get
  }

  object SecondOfMinute extends BIF1(TimeNamespace, "secondOfMinute") with TimeFraction {
    def fraction(d: DateTime) = d.secondOfMinute.get
  }
    
  object MillisOfSecond extends BIF1(TimeNamespace, "millisOfSecond") with TimeFraction {
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

  object Date extends BIF1(TimeNamespace, "date") with TimeTruncation {
    val fmt = ISODateTimeFormat.date()
  }

  object YearMonth extends BIF1(TimeNamespace, "yearMonth") with TimeTruncation {
    val fmt = ISODateTimeFormat.yearMonth()
  }

  object YearDayOfYear extends BIF1(TimeNamespace, "yearDayOfYear") with TimeTruncation {
    val fmt = ISODateTimeFormat.ordinalDate()
  }

  object MonthDay extends BIF1(TimeNamespace, "monthDay") with TimeTruncation {
    val fmt = DateTimeFormat.forPattern("MM-dd")
  }

  object DateHour extends BIF1(TimeNamespace, "dateHour") with TimeTruncation {
    val fmt = ISODateTimeFormat.dateHour()
  }

  object DateHourMinute extends BIF1(TimeNamespace, "dateHourMin") with TimeTruncation {
    val fmt = ISODateTimeFormat.dateHourMinute()
  }

  object DateHourMinuteSecond extends BIF1(TimeNamespace, "dateHourMinSec") with TimeTruncation {
    val fmt = ISODateTimeFormat.dateHourMinuteSecond()
  }

  object DateHourMinuteSecondMillis extends BIF1(TimeNamespace, "dateHourMinSecMilli") with TimeTruncation {
    val fmt = ISODateTimeFormat.dateHourMinuteSecondMillis()
  }

  object TimeWithZone extends BIF1(TimeNamespace, "timeWithZone") with TimeTruncation {
    val fmt = ISODateTimeFormat.time()
  }

  object TimeWithoutZone extends BIF1(TimeNamespace, "timeWithoutZone") with TimeTruncation {
    val fmt = ISODateTimeFormat.hourMinuteSecondMillis()
  }

  object HourMinute extends BIF1(TimeNamespace, "hourMin") with TimeTruncation {
    val fmt = ISODateTimeFormat.hourMinute()
  }

  object HourMinuteSecond extends BIF1(TimeNamespace, "hourMinSec") with TimeTruncation {
    val fmt = ISODateTimeFormat.hourMinuteSecond()
  }
}
