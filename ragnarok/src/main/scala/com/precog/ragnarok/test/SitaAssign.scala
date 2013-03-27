package com.precog
package ragnarok
package test

object SitaAssign extends PerfTestSuite {
  query(
    """
locations := //sita
--locations := //sita100k
import std::stats::*

model := kMedians({x: locations.x, y: locations.y}, 5)

-- sloooow
assignClusters( locations, model)
    """)
}
