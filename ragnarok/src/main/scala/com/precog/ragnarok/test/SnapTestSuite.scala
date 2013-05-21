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
