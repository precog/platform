package com.precog.daze

import org.specs2.mutable._

class TimelibSpec extends Specification {
  "all time functions" should {
    "validate input" in todo
    "return failing validations for bad input" in todo
  }

  "changing time zones" should {
    "not modify millisecond value" in todo
    "work correctly for fractional zones" in todo
  }

  "time difference functions" should {
    "correctly compute difference of years" in todo
    "correctly compute difference of months" in todo
    "correctly compute difference of weeks" in todo
    "correctly compute difference of days" in todo
    "correctly compute difference of hours" in todo
    "correctly compute difference of minutes" in todo
    "correctly compute difference of seconds" in todo
    "correctly compute difference of ms" in todo
  }

  "converting a millis value to an ISO time string" should {
    "default to UTC if timezone is not specified" in todo
  }

  "time extraction functions" should {
    "extract time zone" in todo
    "compute season" in todo
    "compute year" in todo
    "compute month of year" in todo
    "compute week of year" in todo
    "compute week of month" in todo
    "compute day of year" in todo
    "compute day of month" in todo
    "compute day of week" in todo
    "compute hour of day" in todo
    "compute minute of hour" in todo
    "compute second of minute" in todo
    "compute millis of second" in todo
  }

  "time truncation functions" should {
    "reformat dates" in todo
  }
}

// vim: set ts=4 sw=4 et:
