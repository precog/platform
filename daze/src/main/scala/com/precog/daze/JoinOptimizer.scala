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
