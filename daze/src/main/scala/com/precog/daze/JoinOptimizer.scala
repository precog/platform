package com.precog
package daze

import scala.collection.mutable

import com.precog.util.IdGen
import com.precog.yggdrasil.CString

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
        
        case Join(_, _, _, left, right) =>
          merge(determinedByAux(left, determiner), determinedByAux(right, determiner))

        case Filter(_, IdentitySort, body, _) => determinedByAux(body, determiner)
        
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
          case j @ Join(loc1, op, CrossLeftSort | CrossRightSort, lhs, rhs)
            if (determinedBy(lhs, eqA) && determinedBy(rhs, eqB)) ||
               (determinedBy(lhs, eqB) && determinedBy(rhs, eqA)) => {
            
            val (eqLHS, eqRHS, liftedLHS, liftedRHS) =
              if (determinedBy(lhs, eqA)) (eqA, eqB, liftedA, liftedB) else(eqB, eqA, liftedB, liftedA) 
 
              Join(loc1, op, ValueSort(sortId),
                liftRewrite(lhs, eqLHS, liftedLHS),
                liftRewrite(rhs, eqRHS, liftedRHS))
          }

          case Join(loc1, op, IdentitySort, lhs, rhs)
            if (determinedBy(lhs, eqA) && determinedBy(rhs, eqA)) ||
               (determinedBy(lhs, eqB) && determinedBy(rhs, eqB)) => {
            Join(loc1, op, ValueSort(sortId), lhs, rhs)
          }

          case other => other
        }
      }

    transformBottomUp(graph) {
      _ match {
        case
          f @ Filter(loc, IdentitySort,
            body,
            Join(_, Eq, CrossLeftSort | CrossRightSort,
              Join(_, DerefObject, CrossLeftSort,
                eqLHS,
                Const(_, CString(sortFieldLHS))),
              Join(_, DerefObject, CrossLeftSort,
                eqRHS,
                Const(_, CString(sortFieldRHS))))) => {
                  
            val sortId = idGen.nextInt()
            
            def lift(graph: DepGraph, sortField: String) = {
              SortBy(
                Join(loc, JoinObject, IdentitySort,
                  Join(loc, WrapObject, CrossLeftSort,
                    Const(loc, CString("key")),
                    Join(loc, DerefObject, CrossLeftSort, graph, Const(loc, CString(sortField)))),
                  Join(loc, WrapObject, CrossLeftSort, Const(loc, CString("value")), graph)),
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
