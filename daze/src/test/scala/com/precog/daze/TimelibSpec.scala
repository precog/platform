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
