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

import com.precog.yggdrasil.TableModule

import scala.collection.mutable

trait CrossOrdering extends DAG {
  import TableModule.CrossOrder // TODO: Move CrossOrder out somewhere else.
  import instructions._
  import dag._

  def orderCrosses(node: DepGraph): DepGraph = {
    val memotable = mutable.Map[DepGraphWrapper, DepGraph]()
    
    def memoizedSpec(spec: BucketSpec): BucketSpec = spec match {
      case UnionBucketSpec(left, right) =>
        UnionBucketSpec(memoizedSpec(left), memoizedSpec(right))
      
      case IntersectBucketSpec(left, right) =>
        IntersectBucketSpec(memoizedSpec(left), memoizedSpec(right))
      
      case dag.Group(id, target, child) =>
        dag.Group(id, memoized(target), memoizedSpec(child))
      
      case UnfixedSolution(id, target) =>
        UnfixedSolution(id, memoized(target))
      
      case dag.Extra(target) =>
        dag.Extra(memoized(target))
    }
    
    def memoized(node: DepGraph): DepGraph = {
      def inner(node: DepGraph): DepGraph = node match {
        // not using extractors due to bug
        case node: SplitParam =>
          SplitParam(node.id, node.parentId)(node.loc)
        
        // not using extractors due to bug
        case node: SplitGroup =>
          SplitGroup(node.id, node.identities, node.parentId)(node.loc)
        
        case node @ Const(_) => node

        case node @ Undefined() => node
        
        case node @ dag.New(parent) =>
          dag.New(memoized(parent))(node.loc)
        
        case node @ dag.AbsoluteLoad(parent, tpe) =>
          dag.AbsoluteLoad(memoized(parent), tpe)(node.loc)
        
        case node @ dag.RelativeLoad(parent, tpe) =>
          dag.RelativeLoad(memoized(parent), tpe)(node.loc)

        case node @ Operate(op, parent) =>
          Operate(op, memoized(parent))(node.loc)
        
        case node @ dag.Morph1(m, parent) =>
          dag.Morph1(m, memoized(parent))(node.loc)
        
        case node @ dag.Morph2(m, left, right) =>
          dag.Morph2(m, memoized(left), memoized(right))(node.loc)
        
        case node @ dag.Distinct(parent) =>
          dag.Distinct(memoized(parent))(node.loc)
                
        case node @ dag.Reduce(red, parent) =>
          dag.Reduce(red, memoized(parent))(node.loc)
        
        case dag.MegaReduce(reds, parent) =>
          dag.MegaReduce(reds, memoized(parent))

        case s @ dag.Split(spec, child, id) => {
          val spec2 = memoizedSpec(spec)
          val child2 = memoized(child)
          dag.Split(spec2, child2, id)(s.loc)
        }
        
        case node @ dag.Assert(pred, child) =>
          dag.Assert(memoized(pred), memoized(child))(node.loc)
        
        case node @ dag.Cond(pred, left, leftJoin, right, rightJoin) =>
          dag.Cond(memoized(pred), memoized(left), leftJoin, memoized(right), rightJoin)(node.loc)
        
        case node @ dag.Observe(data, samples) =>
          dag.Observe(memoized(data), memoized(samples))(node.loc)
        
        case node @ IUI(union, left, right) =>
          IUI(union, memoized(left), memoized(right))(node.loc)
        
        case node @ Diff(left, right) =>
          Diff(memoized(left), memoized(right))(node.loc)
        
        case node @ Join(op, Cross(hint), left, right) => {
          import CrossOrder._
          if (right.isSingleton)
            Join(op, Cross(Some(CrossLeft)), memoized(left), memoized(right))(node.loc)
          else if (left.isSingleton)
            Join(op, Cross(Some(CrossRight)), memoized(left), memoized(right))(node.loc)
          else {
            val right2 = memoized(right)
            
            right2 match {
              case _: Memoize | _: AddSortKey | _: AbsoluteLoad =>
                Join(op, Cross(hint), memoized(left), right2)(node.loc)
              
              case _ =>
                Join(op, Cross(hint), memoized(left), Memoize(right2, 100))(node.loc)
            }
          }
        }

        case node @ Join(op, joinSort, left, right) =>
          Join(op, joinSort, memoized(left), memoized(right))(node.loc)
        
        case node @ Filter(joinSort, target, boolean) =>
          Filter(joinSort, memoized(target), memoized(boolean))(node.loc)
        
        case AddSortKey(parent, sortField, valueField, id) =>
          AddSortKey(memoized(parent), sortField, valueField, id)
        
        case Memoize(parent, priority) => Memoize(memoized(parent), priority)
      }
  
      memotable.get(new DepGraphWrapper(node)) getOrElse {
        val result = inner(node)
        memotable += (new DepGraphWrapper(node) -> result)
        result
      }
    }
    
    memoized(node)
  }
}
