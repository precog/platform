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
