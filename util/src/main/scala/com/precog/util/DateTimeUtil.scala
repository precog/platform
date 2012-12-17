package com.precog.util

import org.joda.time._
import org.joda.time.format._

object DateTimeUtil {
  private val fullParser = ISODateTimeFormat.dateTimeParser
  private val basicParser = ISODateTimeFormat.basicDateTime

  def parseDateTime(value: String, withOffset: Boolean): DateTime = {
    val parser = if (value.contains("-") || value.contains(":")) {
      fullParser
    } else {
      basicParser
    }

    val p = if (withOffset) parser.withOffsetParsed else parser
    p.parseDateTime(value)
  }
}
