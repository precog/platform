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

import scala.collection.mutable

import com.precog.util.IdGen

trait JoinOptimizer extends DAGTransform {
  import dag._
  import instructions.{ DerefObject, Eq, JoinObject, Line, PushString, WrapObject }

  def optimize(graph: DepGraph, idGen: IdGen = IdGen) : DepGraph = {
    def rewriteUnderEq(graph: DepGraph, lhsEq: DepGraph, rhsEq: DepGraph, sortFieldLHS: String, sortFieldRHS: String, sortId: Int): DepGraph =
      transformBottomUp(graph) {
        _ match {
          case Join(_, DerefObject, _, lhsEq, Root(_, PushString(valueField))) =>
            SortBy(lhsEq, sortFieldLHS, valueField, sortId)
            
          case Join(_, DerefObject, _, rhsEq, Root(_, PushString(valueField))) =>
            SortBy(rhsEq, sortFieldRHS, valueField, sortId)
          case other => other
        }
      }

    transformBottomUp(graph) {
      _ match {
        case
          Filter(loc, IdentitySort,
            Join(_, op, _, lhs, rhs),
            Join(_, Eq, CrossLeftSort,
              Join(_, DerefObject, CrossLeftSort,
                lhsEq,
                Root(_, PushString(sortFieldLHS))),
              Join(_, DerefObject, CrossLeftSort,
                rhsEq,
                Root(_, PushString(sortFieldRHS))))) => {
            val sortId = idGen.nextInt()
            Join(loc, op, ValueSort(sortId),
              rewriteUnderEq(lhs, lhsEq, rhsEq, sortFieldLHS, sortFieldRHS, sortId),
              rewriteUnderEq(rhs, lhsEq, rhsEq, sortFieldLHS, sortFieldRHS, sortId))
          }
        
        case other => other 
      }
    }
  }
}
