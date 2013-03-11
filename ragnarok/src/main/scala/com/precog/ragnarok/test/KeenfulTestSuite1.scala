package com.precog
package ragnarok
package test

object KeenfulTestSuite1 extends PerfTestSuite {
  query(
    """
import std::time::*

data := //keenful

data' := data where
  getMillis (data.action.created_date) > getMillis("2013-03-03") &
  getMillis (data.action.created_date) < getMillis("2013-03-10")

count(distinct(data'.visitor.id))
--26136 ms
--2971 ms (1/1)
--3113 ms (6/3)
    """)
}
