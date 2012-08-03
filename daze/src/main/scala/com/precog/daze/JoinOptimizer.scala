package com.precog
package daze

import scala.collection.mutable

import com.precog.util.IdGen

trait JoinOptimizer extends DAGTransform {
  import dag._
  import instructions.{ DerefObject, Eq, JoinObject, Line, PushString, WrapObject }

  def optimize(graph: DepGraph, idGen: IdGen = IdGen): DepGraph = {
    
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
    
        case Operate(_, _, parent) => determinedByAux(parent, determiner)
    
        case Morph1(_, _, parent) => determinedByAux(parent, determiner)
    
        case Morph2(_, _, left, right) => merge(determinedByAux(left, determiner), determinedByAux(right, determiner)) 
    
        case Join(_, _, _, left, right) => merge(determinedByAux(left, determiner), determinedByAux(right, determiner))
        
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
          case Join(loc1, op, CrossLeftSort | CrossRightSort, lhs, rhs)
            if (determinedBy(lhs, eqA) && determinedBy(rhs, eqB)) ||
               (determinedBy(lhs, eqB) && determinedBy(rhs, eqA)) => {
            
            val (eqLHS, eqRHS, liftedLHS, liftedRHS) =
              if (determinedBy(lhs, eqA)) (eqA, eqB, liftedA, liftedB) else(eqB, eqA, liftedB, liftedA) 
 
            Sort(
              Join(loc1, op, ValueSort(sortId),
                liftRewrite(lhs, eqLHS, liftedLHS),
                liftRewrite(rhs, eqRHS, liftedRHS)),
              Vector(0)) // @djspiewak How do we know which identities to specifiy here?
          }

          case other => other
        }
      }

    transformBottomUp(graph) {
      _ match {
        case
          Filter(loc, IdentitySort,
            body,
            Join(_, Eq, CrossLeftSort | CrossRightSort,
              Join(_, DerefObject, CrossLeftSort,
                eqLHS,
                Root(_, PushString(sortFieldLHS))),
              Join(_, DerefObject, CrossLeftSort,
                eqRHS,
                Root(_, PushString(sortFieldRHS))))) => {
                  
            val sortId = idGen.nextInt()
            
            def lift(graph: DepGraph, sortField: String) = {
              SortBy(
                Join(loc, JoinObject, IdentitySort,
                  Join(loc, WrapObject, CrossLeftSort,
                    Root(loc, PushString("key")),
                    Join(loc, DerefObject, CrossLeftSort, graph, Root(loc, PushString(sortField)))),
                  Join(loc, WrapObject, CrossLeftSort, Root(loc, PushString("value")), graph)),
                "key", "value", sortId) 
            }
            
            rewriteUnderEq(body, eqLHS, eqRHS, lift(eqLHS, sortFieldLHS), lift(eqRHS, sortFieldRHS), sortId)
          }
        
        case other => other 
      }
    }
  }
}
