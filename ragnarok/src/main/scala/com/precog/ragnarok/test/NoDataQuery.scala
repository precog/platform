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
