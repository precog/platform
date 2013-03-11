package com.precog
package ragnarok
package test

object SitaTestSuite extends PerfTestSuite {
  query(
    """
import std::time::*

locations := //sita

locations' := locations with {millis : getMillis(locations.captureTimestamp)}

binSize := 1000*60*5
bins(data, sizeOfBins) := std::math::floor(data.millis / sizeOfBins)
locations'' := locations' with { bins: bins(locations', binSize) * binSize }

r := locations'' with {timestampBin: millisToISO(locations''.bins, "+00:00")}

r' := {x: r.x, y: r.y, timestampBin: r.timestampBin, binMillis: r.bins, deviceId: r.deviceId}

solve 'bin, 'id
  r'' := r' where r'.timestampBin = 'bin & r'.deviceId = 'id
  
  {
  bin: 'bin,
  deviceId: 'id,
   x: r''.x where r''.binMillis = max(r''.binMillis), 
   y: r''.y where r''.binMillis = max(r''.binMillis)
  }
    """)
}
