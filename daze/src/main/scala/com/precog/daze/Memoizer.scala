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
    
    def memoizedSpec(spec: BucketSpec): BucketSpec = spec match {
      case MergeBucketSpec(left, right, and) =>
        MergeBucketSpec(memoizedSpec(left), memoizedSpec(right), and)
      
      case ZipBucketSpec(left, right) =>
        ZipBucketSpec(memoizedSpec(left), memoizedSpec(right))
      
      case SingleBucketSpec(target, solution) =>
        SingleBucketSpec(memoized(target), memoized(solution))
    }
    
    def memoized(graph: DepGraph): DepGraph = {
      def inner(graph: DepGraph): DepGraph = {
        lazy val refs = reftable.getOrElse(graph, 0)
        
        graph match {
          case _: SplitParam => graph
          
          case _: SplitGroup => graph
          
          case _: Root => graph
          
          case New(loc, parent) => {
            if (refs > MemoThreshold)
              Memoize(New(loc, memoized(parent)), refs)
            else
              New(loc, memoized(parent))
          }
          
          case LoadLocal(loc, range, parent, tpe) => {
            if (refs > MemoThreshold)
              Memoize(LoadLocal(loc, range, memoized(parent), tpe), refs)
            else
              LoadLocal(loc, range, memoized(parent), tpe)
          }
          
          case Operate(loc, op, parent) => {
            if (refs > MemoThreshold)
              Memoize(Operate(loc, op, memoized(parent)), refs)
            else
              Operate(loc, op, memoized(parent))
          }
          
          case Reduce(loc, red, parent) => {
            if (refs > MemoThreshold)
              Memoize(Reduce(loc, red, memoized(parent)), refs)
            else
              Reduce(loc, red, memoized(parent))
          }          

          case SetReduce(loc, red, parent) => {
            if (refs > MemoThreshold)
              Memoize(SetReduce(loc, red, memoized(parent)), refs)
            else
              SetReduce(loc, red, memoized(parent))
          }
          
          case Split(loc, specs, child) => {
            val specs2 = specs map memoizedSpec
            val child2 = memoized(child)
            
            if (refs > 1)
              Memoize(Split(loc, specs2, child2), refs)
            else
              Split(loc, specs2, child2)
          }
          
          case Join(loc, instr, left, right) => {
            val left2 = memoized(left)
            val right2 = memoized(right)
            
            if (refs > 1)
              Memoize(Join(loc, instr, left2, right2), refs)
            else
              Join(loc, instr, left2, right2)
          }
          
          case Filter(loc, cross, range, target, boolean) => {
            val target2 = memoized(target)
            val boolean2 = memoized(boolean)
            
            if (refs > 1)
              Memoize(Filter(loc, cross, range, target2, boolean2), refs)
            else
              Filter(loc, cross, range, target2, boolean2)
          }
          
          case Sort(parent, indexes) => {
            if (refs > MemoThreshold)
              Memoize(Sort(memoized(parent), indexes), refs)
            else
              Sort(memoized(parent), indexes)
          }
          
          case Memoize(parent, _) => memoized(parent)
        }
      }
      
      memotable get graph getOrElse {
        val result = inner(graph)
        memotable += (graph -> result)
        result
      }
    }
    
    memoized(graph)
  }
  
  def countSpecRefs(spec: BucketSpec): Map[DepGraph, Int] = spec match {
    case MergeBucketSpec(left, right, _) =>
      merge(countSpecRefs(left), countSpecRefs(right))
    
    case ZipBucketSpec(left, right) =>
      merge(countSpecRefs(left), countSpecRefs(right))
    
    case SingleBucketSpec(target, solution) =>
      merge(increment(countRefs(target), target, 1), increment(countRefs(solution), solution, 1))
  }
  
  def countRefs(graph: DepGraph): Map[DepGraph, Int] = graph match {
    case New(_, parent) =>
      increment(countRefs(parent), parent, 1)
    
    case LoadLocal(_, _, parent, _) =>
      increment(countRefs(parent), parent, 1)
    
    case Operate(_, _, parent) =>
      increment(countRefs(parent), parent, 1)
    
    case Reduce(_, _, parent) =>
      increment(countRefs(parent), parent, 1)
        
    case SetReduce(_, _, parent) =>
      increment(countRefs(parent), parent, 1)
    
    case Split(_, specs, child) =>
      merge(countRefs(child), specs map countSpecRefs reduce merge[DepGraph])
    
    case Join(_, _, left, right) => {
      val rec = merge(countRefs(left), countRefs(right))
      increment(increment(rec, left, 1), right, 1)
    }
    
    case Filter(_, _, _, target, boolean) => {
      val rec = merge(countRefs(target), countRefs(boolean))
      increment(increment(rec, boolean, 1), target, 1)
    }
    
    case Sort(parent, _) =>
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
