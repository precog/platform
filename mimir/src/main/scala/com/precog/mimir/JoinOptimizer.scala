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
package mimir

import scala.collection.mutable

import com.precog.yggdrasil.TableModule
import com.precog.yggdrasil.execution.EvaluationContext
import com.precog.util.IdGen
import com.precog.util.Timing
import com.precog.common._

trait JoinOptimizerModule[M[+_]] extends DAGTransform with TransSpecableModule[M] {

  trait JoinOptimizer extends TransSpecable {
    import dag._
    import instructions._
    import TableModule.CrossOrder
    import CrossOrder._
  
    def optimizeJoins(graph: DepGraph, ctx: EvaluationContext, idGen: IdGen = IdGen): DepGraph = {

      def compareAncestor(lhs: DepGraph, rhs: DepGraph): Boolean =
        findAncestor(lhs, ctx) == findAncestor(rhs, ctx)

      def liftRewrite(graph: DepGraph, eq: DepGraph, lifted: DepGraph): DepGraph =
        transformBottomUp(graph) { g => if (g == eq) lifted else g }

      def rewrite(filter: dag.Filter, body: DepGraph, eqA: DepGraph, eqB: DepGraph): DepGraph = {
        val sortId = idGen.nextInt()

        def lift(keyGraph: DepGraph, valueGraph: DepGraph): DepGraph = {
          val key = "key"
          val value = "value"

          AddSortKey(
            Join(JoinObject, IdentitySort,
              Join(WrapObject, Cross(Some(CrossRight)),
                Const(CString(key))(filter.loc),
                keyGraph)(filter.loc),
              Join(WrapObject, Cross(Some(CrossRight)),
                Const(CString(value))(filter.loc),
                valueGraph)(filter.loc))(filter.loc),
            key, value, sortId)
        }

        val rewritten = transformBottomUp(body) {
          case j @ Join(_, _, Const(_), _) => j

          case j @ Join(_, _, _, Const(_)) => j

          case j @ Join(op, Cross(_), lhs, rhs)
            if (compareAncestor(lhs, eqA) && compareAncestor(rhs, eqB)) ||
               (compareAncestor(lhs, eqB) && compareAncestor(rhs, eqA)) => {

            val (eqLHS, eqRHS) = {
              if (compareAncestor(lhs, eqA))
                (eqA, eqB)
              else
                (eqB, eqA)
            }

            val ancestorLHS = findOrderAncestor(lhs, ctx) getOrElse lhs
            val ancestorRHS = findOrderAncestor(rhs, ctx) getOrElse rhs

            val liftedLHS = lift(eqLHS, ancestorLHS)
            val liftedRHS = lift(eqRHS, ancestorRHS)

            Join(op, ValueSort(sortId),
              liftRewrite(lhs, ancestorLHS, liftedLHS),
              liftRewrite(rhs, ancestorRHS, liftedRHS))(j.loc)
          }

          case other => other
        }

        if (rewritten == body) filter else rewritten
      }

      transformBottomUp(graph) {
        case filter @ Filter(IdentitySort,
          body @ Join(BuiltInFunction2Op(op2), _, _, _),
          Join(Eq, Cross(_), eqA, eqB))
            if op2.rowLevel => rewrite(filter, body, eqA, eqB)

        case filter @ Filter(IdentitySort,
          body @ Join(BuiltInMorphism2(morph2), _, _, _),
          Join(Eq, Cross(_), eqA, eqB))
            if morph2.rowLevel => rewrite(filter, body, eqA, eqB)

        case filter @ Filter(IdentitySort,
          body @ Join(_, _, _, _),
          Join(Eq, Cross(_), eqA, eqB)) => rewrite(filter, body, eqA, eqB)

        case filter @ Filter(IdentitySort,
          body @ Operate(BuiltInFunction1Op(op1), _),
          Join(Eq, Cross(_), eqA, eqB))
            if op1.rowLevel => rewrite(filter, body, eqA, eqB)
        
        case other => other
      }
    }
  }
}
