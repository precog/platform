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

trait DAGTransform extends DAG {
  import dag._
  import instructions.{ DerefObject, Eq, JoinObject, Line, PushString, WrapObject }

  def transformBottomUp(graph: DepGraph)(f : DepGraph => DepGraph) : DepGraph = {

    val memotable = mutable.Map[DepGraph, DepGraph]()

    def transformSpec(splits: => Map[Split, Split], spec: BucketSpec): BucketSpec = spec match {
      case UnionBucketSpec(left, right) =>
        UnionBucketSpec(transformSpec(splits, left), transformSpec(splits, right))
      
      case IntersectBucketSpec(left, right) =>
        IntersectBucketSpec(transformSpec(splits, left), transformSpec(splits, right))
      
      case Group(id, target, child) =>
        Group(id, transformAux(splits, target), transformSpec(splits, child))
      
      case UnfixedSolution(id, target) =>
        UnfixedSolution(id, transformAux(splits, target))
      
      case Extra(target) =>
        Extra(transformAux(splits, target))
    }

    def transformAux(splits0: => Map[Split, Split], graph: DepGraph) : DepGraph = {
      lazy val splits = splits0
      
      def inner(graph: DepGraph): DepGraph = graph match {
        case r : Root => f(r)
  
        case New(loc, parent) => f(New(loc, transformAux(splits, parent)))
  
        case LoadLocal(loc, parent, jtpe) => f(LoadLocal(loc, transformAux(splits, parent), jtpe))
  
        case Operate(loc, op, parent) => f(Operate(loc, op, transformAux(splits, parent)))
  
        case Reduce(loc, red, parent) => f(Reduce(loc, red, transformAux(splits, parent)))
        
        case MegaReduce(loc, reds, parent) => f(MegaReduce(loc, reds, transformAux(splits, parent)))
  
        case Morph1(loc, m, parent) => f(Morph1(loc, m, transformAux(splits, parent)))
  
        case Morph2(loc, m, left, right) => f(Morph2(loc, m, transformAux(splits, left), transformAux(splits, right)))
  
        case Join(loc, op, joinSort, left, right) => f(Join(loc, op, joinSort, transformAux(splits, left), transformAux(splits, right)))
  
        case IUI(loc, union, left, right) => f(IUI(loc, union, transformAux(splits, left), transformAux(splits, right)))

        case Diff(loc, left, right) => f(Diff(loc, transformAux(splits, left), transformAux(splits, right)))

        case Filter(loc, cross, target, boolean) =>
          f(Filter(loc, cross, transformAux(splits, target), transformAux(splits, boolean)))
  
        case Sort(parent, indices) => f(Sort(transformAux(splits, parent), indices))

        case SortBy(parent, sortField, valueField, id) => f(SortBy(transformAux(splits, parent), sortField, valueField, id))
        
        case ReSortBy(parent, id) => f(ReSortBy(transformAux(splits, parent), id))
  
        case Memoize(parent, priority) => f(Memoize(transformAux(splits, parent), priority))
  
        case Distinct(loc, parent) => f(Distinct(loc, transformAux(splits, parent)))
  
        case s @ Split(loc, spec, child) => {
          lazy val splits2 = splits + (s -> s2)
          lazy val spec2 = transformSpec(splits2, spec)
          lazy val child2 = transformAux(splits2, child)
          lazy val s2: Split = Split(loc, spec2, child2)
          f(s2)
        }
  
        case s @ SplitGroup(loc, id, provenance) => f(SplitGroup(loc, id, provenance)(splits(s.parent)))
  
        case s @ SplitParam(loc, id) => f(SplitParam(loc, id)(splits(s.parent)))
      }

      memotable.get(graph) getOrElse {
        val result = inner(graph)
        memotable += (graph -> result)
        result
      }
    }
    
    transformAux(Map(), graph)
  }
}
