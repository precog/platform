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

trait Memoizer extends DAG {
  import dag._
  
  val MemoThreshold = 1
  
  // TODO scale priority by cost
  def memoize(graph: DepGraph): DepGraph = {
    val reftable = countRefs(graph)
    val memotable = mutable.Map[DepGraph, DepGraph]()
    
    def memoizedSpec(spec: BucketSpec, splits: => Map[Split, Split]): BucketSpec = spec match {
      case UnionBucketSpec(left, right) =>
        UnionBucketSpec(memoizedSpec(left, splits), memoizedSpec(right, splits))
      
      case IntersectBucketSpec(left, right) =>
        IntersectBucketSpec(memoizedSpec(left, splits), memoizedSpec(right, splits))
      
      case dag.Group(id, target, child) =>
        dag.Group(id, memoized(target, splits), memoizedSpec(child, splits))
      
      case UnfixedSolution(id, target) =>
        UnfixedSolution(id, memoized(target, splits))
      
      case dag.Extra(target) =>
        dag.Extra(memoized(target, splits))
    }
    
    // TODO rewrite!
    def memoized(graph: DepGraph, _splits: => Map[Split, Split]): DepGraph = {
      lazy val splits = _splits
      
      def inner(graph: DepGraph): DepGraph = {
        lazy val refs = reftable.getOrElse(graph, 0)
        
        graph match {
          case s @ SplitParam(loc, index) => SplitParam(loc, index)(splits(s.parent))
          
          case s @ SplitGroup(loc, index, provenance) => SplitGroup(loc, index, provenance)(splits(s.parent))
          
          case _: Root => graph
          
          case New(loc, parent) =>
            New(loc, memoized(parent, splits))
          
          case LoadLocal(loc, parent, tpe) =>
            LoadLocal(loc, memoized(parent, splits), tpe)
          
          case Operate(loc, op, parent) =>
            Operate(loc, op, memoized(parent, splits))
          
          case Reduce(loc, red, parent) =>
            Reduce(loc, red, memoized(parent, splits))

          case Morph1(loc, m, parent) => {
            val back = Morph1(loc, m, memoized(parent, splits))
            
            if (refs > 1)
              Memoize(back, refs)
            else
              back
          }

          case Morph2(loc, m, left, right) => {
            val back = Morph2(loc, m, memoized(left, splits), memoized(right, splits))
            
            if (refs > 1)
              Memoize(back, refs)
            else
              back
          }

          case Distinct(loc, parent) => {
            val back = Distinct(loc, memoized(parent, splits))
            
            if (refs > 1)
              Memoize(back, refs)
            else
              back
          }
          
          case s @ Split(loc, spec, child) => {
            lazy val splits2 = splits + (s -> result)
            lazy val spec2 = memoizedSpec(spec, splits2)
            lazy val child2 = memoized(child, splits2)
            lazy val result: Split = Split(loc, spec2, child2)
            
            if (refs > 1)
              Memoize(result, refs)
            else
              result
          }
          
          case Join(loc, op, joinSort, left, right) => {
            val left2 = memoized(left, splits)
            val right2 = memoized(right, splits)
            
            Join(loc, op, joinSort, left2, right2)
          }
          
          case IUI(loc, union, left, right) => {
            val left2 = memoized(left, splits)
            val right2 = memoized(right, splits)
            
            IUI(loc, union, left2, right2)
          }
          
          case Diff(loc, left, right) => {
            val left2 = memoized(left, splits)
            val right2 = memoized(right, splits)
            
            Diff(loc, left2, right2)
          }
          
          case Filter(loc, joinSort, target, boolean) => {
            val target2 = memoized(target, splits)
            val boolean2 = memoized(boolean, splits)
            
            Filter(loc, joinSort, target2, boolean2)
          }
          
          case Sort(parent, indexes) =>
            Sort(memoized(parent, splits), indexes)
          
          case SortBy(parent, sortField, valueField, id) =>
            SortBy(memoized(parent, splits), sortField, valueField, id)
          
          case Memoize(parent, _) => memoized(parent, splits)
        }
      }
      
      memotable get graph getOrElse {
        val result = inner(graph)
        memotable += (graph -> result)
        result
      }
    }
    
    memoized(graph, Map())
  }
  
  def countSpecRefs(spec: BucketSpec): Map[DepGraph, Int] = spec match {
    case UnionBucketSpec(left, right) =>
      merge(countSpecRefs(left), countSpecRefs(right))
    
    case IntersectBucketSpec(left, right) =>
      merge(countSpecRefs(left), countSpecRefs(right))
    
    case dag.Group(_, target, child) =>
      merge(increment(countRefs(target), target, 1), countSpecRefs(child))
    
    case UnfixedSolution(_, target) =>
      increment(countRefs(target), target, 1)
    
    case dag.Extra(target) =>
      increment(countRefs(target), target, 1)
  }
  
  def countRefs(graph: DepGraph): Map[DepGraph, Int] = graph match {
    case New(_, parent) =>
      increment(countRefs(parent), parent, 1)
    
    case LoadLocal(_, parent, _) =>
      increment(countRefs(parent), parent, 1)
    
    case Operate(_, _, parent) =>
      increment(countRefs(parent), parent, 1)
    
    case Reduce(_, _, parent) =>
      increment(countRefs(parent), parent, 1)
        
    case Morph1(_, _, parent) =>
      increment(countRefs(parent), parent, 1)
        
    case Morph2(_, _, left, right) => {
      val rec = merge(countRefs(left), countRefs(right))
      increment(increment(rec, left, 1), right, 1)
    }
        
    case Distinct(_, parent) =>
      increment(countRefs(parent), parent, 1)
    
    case Split(_, spec, child) =>
      merge(countRefs(child), countSpecRefs(spec))
    
    case Join(_, _, _, left, right) => {
      val rec = merge(countRefs(left), countRefs(right))
      increment(increment(rec, left, 1), right, 1)
    }
    
    case Filter(_, _, target, boolean) => {
      val rec = merge(countRefs(target), countRefs(boolean))
      increment(increment(rec, boolean, 1), target, 1)
    }
    
    case Sort(parent, _) =>
      increment(countRefs(parent), parent, 1)
    
    case SortBy(parent, _, _, _) =>
      increment(countRefs(parent), parent, 1)
    
    case Memoize(parent, _) =>
      increment(countRefs(parent), parent, 1)
    
    case _ => Map()
  }
  
  private def increment[A](map: Map[A, Int], a: A, delta: Int): Map[A, Int] = {
    val current = map get a getOrElse 0
    map.updated(a, current + delta)
  }
  
  private def merge[A](left: Map[A, Int], right: Map[A, Int]): Map[A, Int] = {
    left.foldLeft(right) {
      case (acc, (key, value)) => increment(acc, key, value)
    }
  }
}
