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
    
    def memoized(node: DepGraph, _splits: => Map[dag.Split, dag.Split]): DepGraph = {
      lazy val splits = _splits
      
      def inner(node: DepGraph): DepGraph = node match {
        case node @ SplitParam(loc, index) => SplitParam(loc, index)(splits(node.parent))
        
        case node @ SplitGroup(loc, index, identities) => SplitGroup(loc, index, identities)(splits(node.parent))
        
        case node @ Root(_, _) => node
        
        case dag.New(loc, parent) =>
          dag.New(loc, memoized(parent, splits))
        
        case dag.LoadLocal(loc, parent, tpe) =>
          dag.LoadLocal(loc, memoized(parent, splits), tpe)

        case Operate(loc, op, parent) =>
          Operate(loc, op, memoized(parent, splits))
        
        case dag.Morph1(loc, m, parent) =>
          dag.Morph1(loc, m, memoized(parent, splits))
        
        case dag.Morph2(loc, m, left, right) =>
          dag.Morph2(loc, m, memoized(left, splits), memoized(right, splits))
        
        case dag.Distinct(loc, parent) =>
          dag.Distinct(loc, memoized(parent, splits))
                
        case dag.Reduce(loc, red, parent) =>
          dag.Reduce(loc, red, memoized(parent, splits))
        
        case dag.MegaReduce(loc, reds, parent) =>
          dag.MegaReduce(loc, reds, memoized(parent, splits))

        case s @ dag.Split(loc, spec, child) => {
          lazy val splits2 = splits + (s -> result)
          lazy val spec2 = memoizedSpec(spec, splits2)
          lazy val child2 = memoized(child, splits2)
          lazy val result: dag.Split = dag.Split(loc, spec2, child2)
          result
        }
        
        case IUI(loc, union, left, right) =>
          IUI(loc, union, memoized(left, splits), memoized(right, splits))
        
        case Diff(loc, left, right) =>
          Diff(loc, memoized(left, splits), memoized(right, splits))
        
        case Join(loc, op, ValueSort(id), left, right) => {
          val left2 = memoized(left, splits)
          val right2 = memoized(right, splits)
          
          def resortLeft = ReSortBy(left2, id)
          def resortRight = ReSortBy(right2, id)
          
          (left2.sorting, right2.sorting) match {
            case (ValueSort(`id`), ValueSort(`id`)) => Join(loc, op, ValueSort(id), left2, right2)
            case (ValueSort(`id`), _              ) => Join(loc, op, ValueSort(id), left2, resortRight)
            case (_,               ValueSort(`id`)) => Join(loc, op, ValueSort(id), resortLeft, right2)
            case _                                  => Join(loc, op, ValueSort(id), resortLeft, resortRight)
          }
        }

        case Join(loc, op, IdentitySort, left, right) => {
          val left2 = memoized(left, splits)
          val right2 = memoized(right, splits)
          
          val (leftIndices, rightIndices) = determineSort(left2, right2)
          
          val leftPrefix = leftIndices zip (Stream from 0) forall { case (a, b) => a == b }
          val rightPrefix = rightIndices zip (Stream from 0) forall { case (a, b) => a == b }
          
          def sortLeft = Sort(left2, leftIndices)
          def sortRight = Sort(right2, rightIndices)
          
          def sortLeftAux = Sort(left2, Vector(0 until left2.identities.length: _*))
          def sortRightAux = Sort(right2, Vector(0 until right2.identities.length: _*))
          
          (left2.sorting, leftPrefix, right2.sorting, rightPrefix) match {
            case (IdentitySort, true,  IdentitySort, true ) => Join(loc, op, IdentitySort, left2, right2)
            case (IdentitySort, true,  IdentitySort, false) => Join(loc, op, IdentitySort, left2, sortRight)
            case (IdentitySort, false, IdentitySort, true ) => Join(loc, op, IdentitySort, sortLeft, right2)
            case (IdentitySort, false, IdentitySort, false) => Join(loc, op, IdentitySort, sortLeft, sortRight)
            
            case (_,            _,     IdentitySort, true ) => Join(loc, op, IdentitySort, sortLeftAux, right2)
            case (_,            _,     IdentitySort, false) => Join(loc, op, IdentitySort, sortLeftAux, sortRight)

            case (IdentitySort, true,  _,            _    ) => Join(loc, op, IdentitySort, left2, sortRightAux) 
            case (IdentitySort, false, _,            _    ) => Join(loc, op, IdentitySort, sortLeft, sortRightAux)
            
            case _                                          => Join(loc, op, IdentitySort, sortLeftAux, sortRightAux)
              
          }
        }
        
        case Join(loc, op, CrossLeftSort | CrossRightSort, left, right) => {
          if (right.isSingleton)
            Join(loc, op, CrossLeftSort, memoized(left, splits), memoized(right, splits))
          else if (left.isSingleton)
            Join(loc, op, CrossRightSort, memoized(left, splits), memoized(right, splits))
          else {
            val right2 = memoized(right, splits)
            
            right2 match {
              case _: Memoize | _: Sort | _: SortBy | _: ReSortBy | _: LoadLocal =>
                Join(loc, op, CrossLeftSort, memoized(left, splits), right2)
              
              case _ =>
                Join(loc, op, CrossLeftSort, memoized(left, splits), Memoize(right2, 100))
            }
          }
        }

        case Join(loc, op, joinSort, left, right) =>
          Join(loc, op, joinSort, memoized(left, splits), memoized(right, splits))
        
        case Filter(loc, ValueSort(id), target, boolean) => {
          val target2 = memoized(target, splits)
          val boolean2 = memoized(boolean, splits)
          
          def resortTarget  = ReSortBy(target2, id)
          def resortBoolean = ReSortBy(boolean2, id)
          
          (target2.sorting, boolean2.sorting) match {
            case (ValueSort(`id`), ValueSort(`id`)) => Filter(loc, ValueSort(id), target2, boolean2) 
            case (ValueSort(`id`), _              ) => Filter(loc, ValueSort(id), target2, resortBoolean) 
            case (_,               ValueSort(`id`)) => Filter(loc, ValueSort(id), resortTarget, boolean2)
            case _                                  => Filter(loc, ValueSort(id), resortTarget, resortBoolean)
          }
        }
          
        case Filter(loc, IdentitySort, target, boolean) => {
          val target2 = memoized(target, splits)
          val boolean2 = memoized(boolean, splits)
          
          val (targetIndexes, booleanIndexes) = determineSort(target2, boolean2)
          
          val targetPrefix = targetIndexes zip (Stream from 0) forall { case (a, b) => a == b }
          val booleanPrefix = booleanIndexes zip (Stream from 0) forall { case (a, b) => a == b }
          
          def sortTarget     = Sort(target2, targetIndexes)
          def sortBoolean    = Sort(boolean2, booleanIndexes)

          def sortTargetAux  = Sort(target2, Vector(0 until target2.identities.length: _*))
          def sortBooleanAux = Sort(boolean2, Vector(0 until boolean2.identities.length: _*))
          
          (target2.sorting, targetPrefix, boolean2.sorting, booleanPrefix) match {
            case (IdentitySort, true,  IdentitySort, true ) => Filter(loc, IdentitySort, target2, boolean2)
            case (IdentitySort, true,  IdentitySort, false) => Filter(loc, IdentitySort, target2, sortBoolean)
            case (IdentitySort, false, IdentitySort, true ) => Filter(loc, IdentitySort, sortTarget, boolean2)
            case (IdentitySort, false, IdentitySort, false) => Filter(loc, IdentitySort, sortTarget, sortBoolean)
            
            case (_, _,                IdentitySort, true ) => Filter(loc, IdentitySort, sortTargetAux, boolean2)
            case (_, _,                IdentitySort, false) => Filter(loc, IdentitySort, sortTargetAux, sortBoolean)

            case (IdentitySort, true,  _,            _    ) => Filter(loc, IdentitySort, target2, sortBooleanAux)
            case (IdentitySort, false, _,            _    ) => Filter(loc, IdentitySort, sortTarget, sortBooleanAux)
              
            case _                                          => Filter(loc, IdentitySort, sortTargetAux, sortBooleanAux)
          }
        }

        case Filter(loc, joinSort, target, boolean) =>
          Filter(loc, joinSort, memoized(target, splits), memoized(boolean, splits))
        
        case Sort(parent, _) => memoized(parent, splits)
        
        case SortBy(parent, sortField, valueField, id) =>
          SortBy(memoized(parent, splits), sortField, valueField, id)
        
        case ReSortBy(parent, id) =>
          ReSortBy(memoized(parent, splits), id)
        
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
    val leftPairs = left2.identities.zipWithIndex filter {
      case (p, i) => right2.identities contains p
    }

    val rightPairs = right2.identities.zipWithIndex filter {
      case (p, i) => left2.identities contains p
    }
    
    val (_, leftIndices) = leftPairs.unzip
    
    val (_, rightIndices) = rightPairs sortWith {
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

    (leftIndices, rightIndices)
  }
}
