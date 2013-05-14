package com.precog
package ragnarok
package test

object SnapTestSuite extends PerfTestSuite {
  query(
    """
import std::stats::*
import std::time::*
--agents := //8504352d-b063-400b-a10b-d6c637539469/status
agents := //snap
--se8429501/8504352d-b063-400b-a10b-d6c637539469

lowerBound :=  1353135306278 
upperBound :=  lowerBound + (1000*60*60*24*95)
--count(agents where agents.timestamp > lowerBound & agents.timestamp < upperBound)

--millisToISO(upperBound, "+00:00")
extraLB := lowerBound - (24*60*60000)

results := solve 'agent
  agents' := agents where agents.timestamp <= upperBound & agents.timestamp >= extraLB & agents.agentId = 'agent
  order := denseRank(agents'.timestamp)

  agents'' := agents' with { rank: order }
  newagents := new agents''
  newagents' := newagents with { rank: newagents.rank - 2 }

  result := newagents' ~ agents'' { first: agents'', second: newagents' } where newagents'.rank = agents''.rank

 {start: std::math::maxOf(result.first.timestamp, lowerBound),
  end: result.second.timestamp,
  agentId: result.first.agentId,
  status: result.first.status,
  name: result.first.agentAlias,
  data: result.first}

results where results.end > lowerBound
    """)
}
