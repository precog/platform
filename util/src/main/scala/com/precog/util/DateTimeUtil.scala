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
package com.precog.util

import org.joda.time._
import org.joda.time.format._

import java.util.regex.Pattern

object DateTimeUtil {
  private val fullParser = ISODateTimeFormat.dateTimeParser
  private val basicParser = ISODateTimeFormat.basicDateTime

  //2013-02-04T18:07:39.608835
  private val dateTimeRegex = Pattern.compile("^[0-9]{4}-?[0-9]{2}-?[0-9]{2}.*$")

  def looksLikeIso8601(s: String): Boolean = dateTimeRegex.matcher(s).matches

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

  def isValidPeriod(period: String): Boolean = try {
    new Period(period); true
  } catch {
    case e: IllegalArgumentException => { false }
  }
}
