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
package ragnarok
package test

object KeenfulTestSuite2 extends PerfTestSuite {
  query(
    """
import std::time::*
data := //keenful

range := data where
  getMillis (data.action.created_date) > getMillis ("2013-03-03") &
  getMillis (data.action.created_date) < getMillis ("2013-03-10")

solve 'day
  data' := range where
    dateHour(range.action.created_date) = 'day
  { day: 'day,
    views: count(data'.action.verb where data'.action.verb = "view"),
    clicks: count(data'.action.verb where data'.action.verb = "click"),
    recommendations: count(data'.action.verb where data'.action.verb = "recommend"),
    total: count(data'.action.verb),
    sets: count(distinct(data'.action.set_id where data'.action.verb = "recommend"))
  }
--41644 ms (1/1) before
--8152 ms (1/1) after
--6707 ms (6/3)
    """)
}
