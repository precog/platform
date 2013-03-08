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
package com.precog.jprofiler

import com.precog.ragnarok._

class Suite(name: String)(qs: List[String]) extends PerfTestSuite {
  override def suiteName = name
  qs.foreach(q => query(q))
}

object Run {
  def main(args: Array[String]): Unit = {
    val cwd = new java.io.File(".").getCanonicalFile
    val db = cwd.getName match {
      case "jprofiler" => "jprofiler.db"
      case _ => "jprofiler/jprofiler.db"
    }

    val args2 = args.toList ++ List("--root-dir", db)
    val config = RunConfig.fromCommandLine(args2) | sys.error("invalid arguments!")

    val queries = List(
      """
      foo := //foo where (//foo).a
      solve 'b
        foo' := foo where foo.b = 'b
        foo'.c
      """
//      """
//import std::stats::*
//import std::time::*
//--agents := //8504352d-b063-400b-a10b-d6c637539469/status
//agents := //snap
//--se8429501/8504352d-b063-400b-a10b-d6c637539469
//
//lowerBound :=  1353135306278 
//upperBound :=  lowerBound + (1000*60*60*24*95)
//--count(agents where agents.timestamp > lowerBound & agents.timestamp < upperBound)
//
//--millisToISO(upperBound, "+00:00")
//extraLB := lowerBound - (24*60*60000)
//
//results := solve 'agent
//  agents' := agents where agents.timestamp <= upperBound & agents.timestamp >= extraLB & agents.agentId = 'agent
//  order := denseRank(agents'.timestamp)
//
//  agents'' := agents' with { rank: order }
//  newagents := new agents''
//  newagents' := newagents with { rank: newagents.rank - 1 }
//
//  result := newagents' ~ agents'' { first: agents'', second: newagents' } where newagents'.rank = agents''.rank
//
// {start: std::math::max(result.first.timestamp, lowerBound),
//  end: result.second.timestamp,
//  agentId: result.first.agentId,
//  status: result.first.status,
//  name: result.first.agentAlias,
//  data: result.first}
//
//results where results.end > lowerBound
//      """
    )

    config.rootDir match {
      case Some(d) if d.exists =>
        println("starting benchmark")
        new Suite("jprofiling")(queries).run(config)
        println("finishing benchmark")

      case Some(d) =>
        println("ERROR: --root-dir %s not found!" format d)
        println("did you forget to run 'extract-data'?")

      case None =>
        println("ERROR: --root-dir is missing somehow")
        println("default should have been %s" format db)
    }
  }
}
