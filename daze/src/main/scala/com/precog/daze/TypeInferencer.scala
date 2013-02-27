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

import bytecode._
import yggdrasil._

trait TypeInferencer extends DAG {
  import instructions.{
    BinaryOperation, ArraySwap, WrapArray, WrapObject, DerefArray, DerefObject,
    JoinInstr, Map2, Map2Cross, Map2CrossLeft, Map2CrossRight
  }
  import dag._

  def inferTypes(jtpe: JType)(graph: DepGraph): DepGraph = {

    val memotable = mutable.Map[DepGraph, DepGraph]()

    def collectTypes(universe: JType, graph: DepGraph): Map[DepGraph, Set[JType]] = {
      def collectSpecTypes(jtpe: JType, typing: Map[DepGraph, Set[JType]], spec: BucketSpec): Map[DepGraph, Set[JType]] = spec match {
        case UnionBucketSpec(left, right) =>
          collectSpecTypes(jtpe, collectSpecTypes(jtpe, typing, left), right) 
        
        case IntersectBucketSpec(left, right) =>
          collectSpecTypes(jtpe, collectSpecTypes(jtpe, typing, left), right) 
        
        case Group(id, target, child) =>
          collectSpecTypes(jtpe, inner(jtpe, typing, target), child)
        
        case UnfixedSolution(id, target) =>
          inner(jtpe, typing, target)
        
        case Extra(target) =>
          inner(jtpe, typing, target)
      }
    
      def inner(jtpe: JType, typing: Map[DepGraph, Set[JType]], graph: DepGraph): Map[DepGraph, Set[JType]] = {
        graph match {
          case _: Root => typing 
    
          case New(parent) => inner(jtpe, typing, parent)
    
          case ld @ LoadLocal(parent, _) =>
            val typing0 = inner(JTextT, typing, parent)
            typing0 get ld map { jtpes => typing + (ld -> (jtpes + jtpe)) } getOrElse (typing + (ld -> Set(jtpe)))
  
          case Operate(op, parent) => inner(op.tpe.arg, typing, parent)
    
          case Reduce(red, parent) => inner(red.tpe.arg, typing, parent)

          // TODO: this is correct, but could be more precise
          // we should use the trans specs to narrow the possible types of parent
          case MegaReduce(_, parent) => inner(JType.JUnfixedT, typing, parent)
    
          case Morph1(m, parent) => inner(m.tpe.arg, typing, parent)
    
          case Morph2(m, left, right) => inner(m.tpe.arg1, inner(m.tpe.arg0, typing, left), right)
    
          case Join(DerefObject, CrossLeftSort | CrossRightSort, left, right @ ConstString(str)) =>
            inner(JObjectFixedT(Map(str -> jtpe)), typing, left)
    
          case Join(DerefArray, CrossLeftSort | CrossRightSort, left, right @ ConstDecimal(d)) =>
            inner(JArrayFixedT(Map(d.toInt -> jtpe)), typing, left)
    
          case Join(WrapObject, CrossLeftSort | CrossRightSort, ConstString(str), right) => {
            val jtpe2 = jtpe match {
              case JObjectFixedT(map) =>
                map get str getOrElse universe
              
              case _ => universe
            }
            
            inner(jtpe2, typing, right)
          }
    
          case Join(ArraySwap, CrossLeftSort | CrossRightSort, left, right) => {
            val jtpe2 = jtpe match {
              case JArrayFixedT(_) => jtpe
              case _ => JArrayUnfixedT
            }
            
            inner(JNumberT, inner(jtpe2, typing, left), right)
          }
    
          case Join(op: BinaryOperation, _, left, right) =>
            inner(op.tpe.arg1, inner(op.tpe.arg0, typing, left), right)
    
          case Assert(pred, child) => inner(jtpe, inner(jtpe, typing, pred), child)
          
          case Observe(data, samples) => inner(jtpe, inner(jtpe, typing, data), samples)
          
          case IUI(_, left, right) => inner(jtpe, inner(jtpe, typing, left), right)
  
          case Diff(left, right) => inner(jtpe, inner(jtpe, typing, left), right)
    
          case Filter(_, target, boolean) =>
            inner(JBooleanT, inner(jtpe, typing, target), boolean)
    
          case Sort(parent, _) => inner(jtpe, typing, parent)
    
          case SortBy(parent, _, _, _) => inner(jtpe, typing, parent)
          
          case ReSortBy(parent, _) => inner(jtpe, typing, parent)
  
          case Memoize(parent, _) => inner(jtpe, typing, parent)
    
          case Distinct(parent) => inner(jtpe, typing, parent)
    
          case s @ Split(spec, child) =>
            inner(jtpe, collectSpecTypes(universe, typing, spec), child)
          
          // not using extractors due to bug
          case s: SplitGroup => {
            val Split(spec, _) = s.parent
            findGroup(spec, s.id) map { inner(jtpe, typing, _) } getOrElse typing
          }
          
          // not using extractors due to bug
          case s: SplitParam => {
            val Split(spec, _) = s.parent
            
            findParams(spec, s.id).foldLeft(typing) { (typing, graph) =>
              inner(jtpe, typing, graph)
            }
          }
        }
      }
      
      inner(universe, Map(), graph)
    }

    def applyTypes(typing: Map[DepGraph, JType], graph: DepGraph): DepGraph = {
      graph mapDown { recurse => {
        case ld @ LoadLocal(parent, _) =>
          LoadLocal(recurse(parent), typing(ld))(ld.loc)
      }}
    }
    
    def findGroup(spec: BucketSpec, id: Int): Option[DepGraph] = spec match {
      case UnionBucketSpec(left, right) => findGroup(left, id) orElse findGroup(right, id)
      case IntersectBucketSpec(left, right) => findGroup(left, id) orElse findGroup(right, id)
      
      case Group(`id`, target, _) => Some(target)
      case Group(_, _, _) => None
      
      case UnfixedSolution(_, _) => None
      case Extra(_) => None
    }
    
    def findParams(spec: BucketSpec, id: Int): Set[DepGraph] = spec match {
      case UnionBucketSpec(left, right) => findParams(left, id) ++ findParams(right, id)
      case IntersectBucketSpec(left, right) => findParams(left, id) ++ findParams(right, id)
      
      case Group(_, _, child) => findParams(child, id)
      
      case UnfixedSolution(`id`, child) => Set(child)
      case UnfixedSolution(_, _) => Set()
      case Extra(_) => Set()
    }
    
    val collectedTypes = collectTypes(jtpe, graph)
    val typing = collectedTypes.mapValues(_.reduce(JUnionT)) 
    applyTypes(typing, graph)
  }
}
