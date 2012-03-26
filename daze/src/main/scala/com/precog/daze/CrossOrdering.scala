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

trait CrossOrdering extends DAG {
  import instructions._
  import dag._

  def orderCrosses(node: DepGraph): DepGraph = {
    val memotable = mutable.Map[DepGraph, DepGraph]()
    
    def memoizedSpec(spec: BucketSpec, splits: => Map[dag.Split, dag.Split]): BucketSpec = spec match {
      case MergeBucketSpec(left, right, and) =>
        MergeBucketSpec(memoizedSpec(left, splits), memoizedSpec(right, splits), and)
      
      case ZipBucketSpec(left, right) =>
        ZipBucketSpec(memoizedSpec(left, splits), memoizedSpec(right, splits))
      
      case SingleBucketSpec(target, solution) =>
        SingleBucketSpec(memoized(target, splits), memoized(solution, splits))
    }
    
    def memoized(node: DepGraph, _splits: => Map[dag.Split, dag.Split]): DepGraph = {
      lazy val splits = _splits
      
      def inner(node: DepGraph): DepGraph = node match {
        case node @ SplitParam(loc, index) => SplitParam(loc, index)(splits(node.parent))
        
        case node @ SplitGroup(loc, index, provenance) => SplitGroup(loc, index, provenance)(splits(node.parent))
        
        case node @ Root(_, _) => node
        
        case dag.New(loc, parent) =>
          dag.New(loc, memoized(parent, splits))
        
        case dag.LoadLocal(loc, range, parent, tpe) =>
          dag.LoadLocal(loc, range, memoized(parent, splits), tpe)
        
        case Operate(loc, op, parent) =>
          Operate(loc, op, memoized(parent, splits))
        
        case dag.SetReduce(loc, red, parent) =>
          dag.SetReduce(loc, red, memoized(parent, splits))
                
        case dag.Reduce(loc, red, parent) =>
          dag.Reduce(loc, red, memoized(parent, splits))
        
        case s @ dag.Split(loc, specs, child) => {
          lazy val splits2 = splits + (s -> result)
          lazy val specs2 = specs map { s => memoizedSpec(s, splits2) }
          lazy val child2 = memoized(child, splits2)
          lazy val result: dag.Split = dag.Split(loc, specs2, child2)
          result
        }
        
        case Join(loc, instr: Map2Match, left, right) => {
          val left2 = memoized(left, splits)
          val right2 = memoized(right, splits)
          
          val (leftIndexes, rightIndexes) = determineSort(left2, right2)
          
          val leftPrefix = leftIndexes zip (Stream from 0) forall { case (a, b) => a == b }
          val rightPrefix = rightIndexes zip (Stream from 0) forall { case (a, b) => a == b }
          
          if (leftPrefix && rightPrefix)
            Join(loc, instr, left2, right2)
          else if (leftPrefix && !rightPrefix)
            Join(loc, instr, left2, Sort(right2, rightIndexes))
          else if (!leftPrefix && rightPrefix)
            Join(loc, instr, Sort(left2, leftIndexes), right2)
          else
            Join(loc, instr, Sort(left2, leftIndexes), Sort(right2, rightIndexes))
        }
        
        case Join(loc, Map2Cross(op), left, right) => {
          if (right.isSingleton)
            Join(loc, Map2CrossLeft(op), memoized(left, splits), memoized(right, splits))
          else if (left.isSingleton)
            Join(loc, Map2CrossRight(op), memoized(left, splits), memoized(right, splits))
          else
            Join(loc, Map2CrossLeft(op), memoized(left, splits), memoized(right, splits))
        }
        
        case Join(loc, instr, left, right) =>
          Join(loc, instr, memoized(left, splits), memoized(right, splits))
        
        case Filter(loc, None, range, target, boolean) => {
          val target2 = memoized(target, splits)
          val boolean2 = memoized(boolean, splits)
          
          val (targetIndexes, booleanIndexes) = determineSort(target2, boolean2)
          
          val targetPrefix = targetIndexes zip (Stream from 0) forall { case (a, b) => a == b }
          val booleanPrefix = booleanIndexes zip (Stream from 0) forall { case (a, b) => a == b }
          
          if (targetPrefix && booleanPrefix)
            Filter(loc, None, range, target2, boolean2)
          else if (targetPrefix && !booleanPrefix)
            Filter(loc, None, range, target2, Sort(boolean2, booleanIndexes))
          else if (!targetPrefix && booleanPrefix)
            Filter(loc, None, range, Sort(target2, targetIndexes), boolean2)
          else  
            Filter(loc, None, range, Sort(target2, targetIndexes), Sort(boolean2, booleanIndexes))
        }
        
        case Filter(loc, cross, range, target, boolean) =>
          Filter(loc, cross, range, memoized(target, splits), memoized(boolean, splits))
        
        case Sort(parent, _) => memoized(parent, splits)
        
        case Memoize(parent, priority) => Memoize(memoized(parent, splits), priority)
      }
  
      memotable.get(node) getOrElse {
        val result = inner(node)
        memotable += (node -> result)
        result
      }
    }
    
    memoized(node, Map())
  }

  private def determineSort(left2: DepGraph, right2: DepGraph): (Vector[Int], Vector[Int]) = {
    val leftPairs = left2.provenance.zipWithIndex filter {
      case (p, i) => right2.provenance contains p
    }
    
    val rightPairs = right2.provenance.zipWithIndex filter {
      case (p, i) => left2.provenance contains p
    }
    
    val (_, leftIndexes) = leftPairs.unzip
    
    val (_, rightIndexes) = rightPairs sortWith {
      case ((p1, i1), (p2, i2)) => {
        val leftIndex = leftPairs indexWhere {
          case (`p1`, _) => true
          case _ => false
        }
        
        val rightIndex = leftPairs indexWhere {
          case (`p2`, _) => true
          case _ => false
        }
        
        leftIndex < rightIndex
      }
    } unzip

    (leftIndexes, rightIndexes)
  }
}
