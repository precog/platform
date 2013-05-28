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

object NoDataTestSuite extends PerfTestSuite {
  val input = """
    | import std::stats::*
    | import std::time::*
    | 
    | --locations := //devicelocations/2012/07/01
    | locations := //test
    | deviceTimes := [ locations.deviceId, locations.captureTimestamp ]
    | 
    | order := denseRank(deviceTimes)
    | 
    | locations' := locations with { rank : order }
    | --locations'
    | newLocations := new locations'
    | newLocations' := newLocations with { rank : newLocations.rank - 1 }
    | 
    | joined := newLocations' ~ locations'
    |   { first : locations', second : newLocations' } where locations'.rank
    | = newLocations'.rank
    | 
    | r := joined where joined.first.deviceId = joined.second.deviceId
    | 
    | r' := 
    |   {
    |   data: r.first,
    |   nextLocation: r.second.currentZone,
    |   dwellTime: getMillis(r.second.captureTimestamp) - getMillis(r.first.captureTimestamp)
    |   }
    | 
    | markov := solve 'location, 'nextLocation
    |   r'' := r' where r'.data.currentZone = 'location & r'.nextLocation = 'nextLocation
    |   total := count(r'.data.currentZone where r'.data.currentZone = 'location)
    |   {
    |   matrix: ['location, 'nextLocation],
    |   prob: count(r''.nextLocation)/total
    |   }
    | 
    | --markov
    | 
    | --function for creating cumulative probability distribution and formatting intervals
    | createModel(data) :=  
    |   predictedDistribution := data with {rank : indexedRank(data.prob) }
    | 
    |   cumProb := solve 'rank = predictedDistribution.rank
    |     {
    |     location: predictedDistribution.matrix where  predictedDistribution.rank = 'rank,
    |     prob: sum(predictedDistribution.prob where predictedDistribution.rank <= 'rank),
    |     rank: 'rank
    |     }
    | 
    |   buckets := solve 'rank = cumProb.rank
    |     minimum:= cumProb.prob where cumProb.rank = 'rank
    |     maximum:= min(cumProb.prob where cumProb.rank > 'rank)
    |     {
    |      range: [minimum, maximum],
    |      name: cumProb.matrix where cumProb.rank = 'rank
    |     }
    | buckets
    | --end createModel Function
    | 
    | markov' := createModel(markov)
    | markov'
    | """.stripMargin

  query(input)
}
