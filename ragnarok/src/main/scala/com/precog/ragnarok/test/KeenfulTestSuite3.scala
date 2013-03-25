package com.precog
package ragnarok
package test

object KeenfulTestSuite3 extends PerfTestSuite {
  query(
    """
import std::stats::rank
data := //keenful

data' := data where data.action.verb = "view"

byItem := solve 'item_id, 'item_url 
  data'' := data' where data'.item.id = 'item_id & data'.item.url = 'item_url
  {
    item_id: 'item_id, 
    count: count(data''), 
    item_url : 'item_url
  }  

byItem' := byItem with {rank : rank(byItem.count)}

byItem' where byItem'.rank > max(byItem'.rank - 5)
    """)
}
