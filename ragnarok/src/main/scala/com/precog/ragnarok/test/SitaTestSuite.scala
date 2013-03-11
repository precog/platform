package com.precog
package ragnarok
package test

object SitaTestSuite extends PerfTestSuite {
  query(
    """
import std::time::*

locations := //sita
--locations
-- 7.5s at locations

--locations.captureTimestamp
-- 5.4s without comp

getMillis(locations.captureTimestamp)
-- 8.2s

--locations
-- 7.7 without comp

--locations' := locations with {millis : getMillis(locations.captureTimestamp)}
-- 10s at locations'

--binSize := 1000*60*5
--bins(data, sizeOfBins) := std::math::floor(data.millis / sizeOfBins)
--locations'' := locations' with { bins: bins(locations', binSize) * binSize }

--locations'' := locations' with { bins: std::math::floor(locations'.millis) }
-- 19s at locations''

--r := locations'' with {timestampBin: millisToISO(locations''.bins, "+00:00")}
-- 28s at r

--r' := {x: r.x, y: r.y, timestampBin: r.timestampBin, binMillis: r.bins, deviceId: r.deviceId}
--r'
-- 88s at r'
-- 140s at r' with bins() function

(-solve 'bin, 'id
  r'' := r' where r'.timestampBin = 'bin & r'.deviceId = 'id
  
  {
  bin: 'bin,
  deviceId: 'id,
   x: r''.x where r''.binMillis = max(r''.binMillis), 
   y: r''.y where r''.binMillis = max(r''.binMillis)
  }-)
    """)
}
