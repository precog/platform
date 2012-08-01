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

trait JoinOptimizer extends DAG {
  import dag._
  import instructions.{ DerefObject, Eq, JoinObject, Line, PushString, WrapObject }

  def optimize(graph: DepGraph) : DepGraph = {

    val memotable = mutable.Map[DepGraph, DepGraph]()

    def optimizeSpec(splits: => Map[Split, Split], spec: BucketSpec): BucketSpec = spec match {
      case UnionBucketSpec(left, right) =>
        UnionBucketSpec(optimizeSpec(splits, left), optimizeSpec(splits, right))
      
      case IntersectBucketSpec(left, right) =>
        IntersectBucketSpec(optimizeSpec(splits, left), optimizeSpec(splits, right))
      
      case Group(id, target, child) =>
        Group(id, optimizeAux(splits, target), optimizeSpec(splits, child))
      
      case UnfixedSolution(id, target) =>
        UnfixedSolution(id, optimizeAux(splits, target))
      
      case Extra(target) =>
        Extra(optimizeAux(splits, target))
    }

    def optimizeAux(splits0: => Map[Split, Split], graph: DepGraph) : DepGraph = {
      lazy val splits = splits0

      def inner(graph: DepGraph): DepGraph = graph match {
        case r : Root => r
  
        case New(loc, parent) => New(loc, optimizeAux(splits, parent))
  
        case l @ LoadLocal(loc, parent, jtpe) => LoadLocal(loc, optimizeAux(splits, parent), jtpe)
  
        case Operate(loc, op, parent) => Operate(loc, op, optimizeAux(splits, parent))
  
        case Reduce(loc, red, parent) => Reduce(loc, red, optimizeAux(splits, parent))
  
        case Morph1(loc, m, parent) => Morph1(loc, m, optimizeAux(splits, parent))
  
        case Morph2(loc, m, left, right) => Morph2(loc, m, optimizeAux(splits, left), optimizeAux(splits, right))
  
        case Join(loc, op, joinSort, left, right) => Join(loc, op, joinSort, optimizeAux(splits, left), optimizeAux(splits, right))
  
        case IUI(loc, union, left, right) => IUI(loc, union, optimizeAux(splits, left), optimizeAux(splits, right))

        case Diff(loc, left, right) => Diff(loc, optimizeAux(splits, left), optimizeAux(splits, right))

        case
          Filter(loc, IdentitySort,
            Join(_, op1, CrossLeftSort,
              Join(_, op2, CrossLeftSort,
                op2LHS,
                Join(_, DerefObject, CrossLeftSort,
                  lhs,
                  Root(_, PushString(valueFieldLHS)))),
              Join(_, op3, CrossLeftSort,
                op3LHS,
                Join(_, DerefObject, CrossLeftSort,
                  rhs,
                  Root(_, PushString(valueFieldRHS))))),
            Join(_, Eq, CrossLeftSort,
              Join(_, DerefObject, CrossLeftSort,
                lhsEq,
                Root(_, PushString(sortField))),
              Join(_, DerefObject, CrossLeftSort,
                rhsEq,
                Root(_, PushString(sortFieldRHS))))) if sortField == sortFieldRHS &&
                  ((lhs == lhsEq && rhs == rhsEq) || (rhs == lhsEq && lhs == rhsEq)) => {

          Join(loc, op1, IdentitySort,
            Join(loc, op2, CrossLeftSort,
              optimizeAux(splits, op2LHS),
              Join(loc, DerefObject, CrossLeftSort, 
                SortBy(optimizeAux(splits, lhs), sortField, valueFieldLHS, 0), 
                Root(loc, PushString(valueFieldLHS)))),
            Join(loc, op3, CrossLeftSort,
              optimizeAux(splits, op3LHS),
              Join(loc, DerefObject, CrossLeftSort,
                SortBy(optimizeAux(splits, rhs), sortField, valueFieldRHS, 0), 
                Root(loc, PushString(valueFieldRHS)))))
        }

        case Filter(loc, cross, target, boolean) =>
          Filter(loc, cross, optimizeAux(splits, target), optimizeAux(splits, boolean))
  
        case Sort(parent, indices) => Sort(optimizeAux(splits, parent), indices)

        case SortBy(parent, sortField, valueField, id) => SortBy(optimizeAux(splits, parent), sortField, valueField, id)
        
        case ReSortBy(parent, id) => ReSortBy(optimizeAux(splits, parent), id)
  
        case Memoize(parent, priority) => Memoize(optimizeAux(splits, parent), priority)
  
        case Distinct(loc, parent) => Distinct(loc, optimizeAux(splits, parent))
  
        case s @ Split(loc, spec, child) => {
          lazy val splits2 = splits + (s -> s2)
          lazy val spec2 = optimizeSpec(splits2, spec)
          lazy val child2 = optimizeAux(splits2, child)
          lazy val s2: Split = Split(loc, spec2, child2)
          s2
        }
  
        case s @ SplitGroup(loc, id, provenance) => SplitGroup(loc, id, provenance)(splits(s.parent))
  
        case s @ SplitParam(loc, id) => SplitParam(loc, id)(splits(s.parent))
      }

      memotable.get(graph) getOrElse {
        val result = inner(graph)
        memotable += (graph -> result)
        result
      }
    }
    
    optimizeAux(Map(), graph)
  }
}
