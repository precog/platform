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
package daze

import scalaz.Monoid

import com.precog.util._
import scalaz.std.map._

trait ReductionFinder extends DAG {
  import instructions._
  import dag._

  def findReductions(node: DepGraph): Map[DepGraph, Vector[dag.Reduce]] = node.foldDown[Map[DepGraph, Vector[dag.Reduce]]] {
    case node @ dag.Reduce(_, _, parent) => Map(parent -> Vector(node))
  }

  def megaReduce(node: DepGraph, reds: Map[DepGraph, Vector[dag.Reduce]]): DepGraph = node.mapDown { recurse => {
    case graph if reds isDefinedAt graph => {
      dag.MegaReduce(graph.loc, reds(graph), recurse(graph))
    }
    case r @ dag.Reduce(loc, red, parent) => {
      val index: Int = reds(parent).indexOf(r)
      dag.Join(loc, DerefArray, CrossLeftSort, dag.MegaReduce(loc, reds(parent), recurse(parent)), Root(loc, PushNum("`index`")))
    }
  }}
}
