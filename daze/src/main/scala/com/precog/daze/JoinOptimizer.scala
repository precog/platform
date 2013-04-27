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
import com.precog.util.Timing
import com.precog.common._

trait JoinOptimizer extends DAGTransform {
  import dag._
  import instructions.{ DerefObject, Eq, JoinObject, Line, PushString, WrapObject }

  def optimizeJoins(graph: DepGraph, idGen: IdGen = IdGen): DepGraph = {
    
    def determinedBy(determinee: DepGraph, determiner: DepGraph): Boolean = {
      
      def merge(x: (Boolean, Boolean), y: (Boolean, Boolean)) = (x, y) match {
        case ((false, _),  (false, _))  => (false, true)
        case ((false, _),  (true, rhs)) => (true, rhs)
        case ((true, lhs), (false, _))  => (true, lhs)
        case ((true, lhs), (true, rhs)) => (true, lhs && rhs)
      }
      
      def determinedByAux(determinee: DepGraph, determiner: DepGraph): (Boolean, Boolean) = determinee match {
        case g if g == determiner => (true, true)
        
        case r : Root => (false, true)
        
        case Join(_, _, left, right) =>
          merge(determinedByAux(left, determiner), determinedByAux(right, determiner))

        case Filter(IdentitySort, body, _) => determinedByAux(body, determiner)

        case Operate(_, body) => determinedByAux(body, determiner)
        
        case Memoize(parent, _) => determinedByAux(parent, determiner)
    
        case _ => (true, false)
      }
      
      determinedByAux(determinee, determiner) match {
        case (false, _)  => false
        case (true, det) => det
      }
    }

    def liftRewrite(graph: DepGraph, eq: DepGraph, lifted: DepGraph): DepGraph = {
      transformBottomUp(graph) { g => if(g == eq) lifted else g }
    }

    def rewriteUnderEq(graph: DepGraph, eqA: DepGraph, eqB: DepGraph, liftedA: DepGraph, liftedB: DepGraph, sortId: Int): DepGraph =
      transformBottomUp(graph) {
        _ match {
          case j @ Join(op, CrossLeftSort | CrossRightSort, lhs, rhs)
            if (determinedBy(lhs, eqA) && determinedBy(rhs, eqB)) ||
               (determinedBy(lhs, eqB) && determinedBy(rhs, eqA)) => {
            
            val (eqLHS, eqRHS, liftedLHS, liftedRHS) =
              if (determinedBy(lhs, eqA)) (eqA, eqB, liftedA, liftedB) else(eqB, eqA, liftedB, liftedA) 
              
              Join(op, ValueSort(sortId),
                liftRewrite(lhs, eqLHS, liftedLHS),
                liftRewrite(rhs, eqRHS, liftedRHS))(j.loc)
          }

          case j @ Join(op, IdentitySort, lhs, rhs)
            if (determinedBy(lhs, eqA) && determinedBy(rhs, eqA)) ||
               (determinedBy(lhs, eqB) && determinedBy(rhs, eqB)) => {
            Join(op, ValueSort(sortId), lhs, rhs)(j.loc)
          }

          case other => other
        }
      }

    transformBottomUp(graph) {
      _ match {
        case
          f @ Filter(IdentitySort,
            body,
            Join(Eq, CrossLeftSort | CrossRightSort,
              Join(DerefObject, CrossLeftSort,
                eqLHS,
                Const(CString(sortFieldLHS))),
              Join(DerefObject, CrossLeftSort,
                eqRHS,
                Const(CString(sortFieldRHS))))) => {
                  
            val sortId = idGen.nextInt()
            
            def lift(graph: DepGraph, sortField: String) = {
              SortBy(
                Join(JoinObject, IdentitySort,
                  Join(WrapObject, CrossLeftSort,
                    Const(CString("key"))(f.loc),
                    Join(DerefObject, CrossLeftSort, graph, Const(CString(sortField))(f.loc))(f.loc))(f.loc),
                  Join(WrapObject, CrossLeftSort, Const(CString("value"))(f.loc), graph)(f.loc))(f.loc),
                "key", "value", sortId)
            }
            
            val rewritten = rewriteUnderEq(body, eqLHS, eqRHS, lift(eqLHS, sortFieldLHS), lift(eqRHS, sortFieldRHS), sortId)
            if(rewritten == body) f else rewritten
          }
        
        case other => other 
      }
    }
  }
}
