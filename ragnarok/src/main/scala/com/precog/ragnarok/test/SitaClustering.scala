package com.precog
package ragnarok
package test

object SitaClustering extends PerfTestSuite {
  query(
    """
    import std::stats::*

    locations := //sita1k

    points := { x: locations.x, y: locations.y }
    kMedians(points, 5)
    """
  )
}
