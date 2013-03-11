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
