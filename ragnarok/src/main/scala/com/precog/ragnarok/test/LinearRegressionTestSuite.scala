package com.precog
package ragnarok
package test

object LinearRegressionTestSuite extends PerfTestSuite {
  query(
    """
      locations := //sita
      
      std::stats::linearRegression(locations.acc, { y: locations.y , x: locations.x, id: locations.id })
    """)
}
