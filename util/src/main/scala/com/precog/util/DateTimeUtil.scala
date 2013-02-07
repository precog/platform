package com.precog.util

import org.joda.time._
import org.joda.time.format._

object DateTimeUtil {
  private val fullParser = ISODateTimeFormat.dateTimeParser
  private val basicParser = ISODateTimeFormat.basicDateTime

  def parseDateTime(value0: String, withOffset: Boolean): DateTime = {
    val value = value0.trim.replace(" ", "T")

    val parser = if (value.contains("-") || value.contains(":")) {
      fullParser
    } else {
      basicParser
    }

    val p = if (withOffset) parser.withOffsetParsed else parser
    p.parseDateTime(value)
  }

  import com.mdimension.jchronic._

  //val defaultOptions = new Options()
  val defaultOptions = new Options(0)
  val defaultTimeZone = java.util.TimeZone.getDefault()

  def parseDateTimeFlexibly(s: String): DateTime = {
    val span = Chronic.parse(s, defaultOptions)
    val msecs = span.getBegin * 1000

    // jchronic doesn't really handle time zones.
    //
    // all times are parsed as if they are local time, but the DateTime
    // constructor reads in UTC. so we will manually compute the offsets.
    new DateTime(msecs + defaultTimeZone.getOffset(msecs))
  }

  def isDateTimeFlexibly(s: String): Boolean = try {
    Chronic.parse(s, defaultOptions) != null
  } catch {
    case e: Exception => false
  }

  def isValidISO(str: String): Boolean = try {
    parseDateTime(str, true); true
  } catch {
    case e:IllegalArgumentException => { false }
  }

  def isValidTimeZone(str: String): Boolean = try {
    DateTimeZone.forID(str); true
  } catch {
    case e:IllegalArgumentException => { false }
  }
  
  def isValidFormat(time: String, fmt: String): Boolean = try {
    DateTimeFormat.forPattern(fmt).withOffsetParsed().parseDateTime(time); true
  } catch {
    case e: IllegalArgumentException => { false }
  }
}
