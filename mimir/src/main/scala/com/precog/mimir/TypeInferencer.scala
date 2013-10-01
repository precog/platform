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

import scala.collection.mutable

import com.precog.bytecode._
import com.precog.yggdrasil._
import com.precog.util.Identifier

trait TypeInferencer extends DAG {
  import instructions.{
    BinaryOperation, ArraySwap, WrapArray, WrapObject, DerefArray, DerefObject
  }
  import dag._

  def inferTypes(jtpe: JType)(graph: DepGraph): DepGraph = {

    def collectTypes(universe: JType, graph: DepGraph): Map[DepGraph, Set[JType]] = {
      def collectSpecTypes(typing: Map[DepGraph, Set[JType]], splits: Map[Identifier, Split], spec: BucketSpec): Map[DepGraph, Set[JType]] = spec match {
        case UnionBucketSpec(left, right) =>
          collectSpecTypes(collectSpecTypes(typing, splits, left), splits, right) 
        
        case IntersectBucketSpec(left, right) =>
          collectSpecTypes(collectSpecTypes(typing, splits, left), splits, right)
        
        case Group(id, target, child) =>
          collectSpecTypes(inner(None, typing, splits, target), splits, child)
        
        case UnfixedSolution(id, target) =>
          inner(Some(universe), typing, splits, target)
        
        case Extra(target) =>
          inner(Some(universe), typing, splits, target)
      }
    
      def inner(jtpe: Option[JType], typing: Map[DepGraph, Set[JType]], splits: Map[Identifier, Split], graph: DepGraph): Map[DepGraph, Set[JType]] = {
        graph match {
          case _: Root => typing 
    
          case New(parent) => inner(jtpe, typing, splits, parent)
    
          case ld @ AbsoluteLoad(parent, _) =>
            val typing0 = inner(Some(JTextT), typing, splits, parent)
            jtpe map { jtpe0 =>
              typing0 get ld map { jtpes =>
                typing + (ld -> (jtpes + jtpe0))
              } getOrElse {
                typing + (ld -> Set(jtpe0))
              }
            } getOrElse typing
            
          case ld @ RelativeLoad(parent, _) =>
            val typing0 = inner(Some(JTextT), typing, splits, parent)
            jtpe map { jtpe0 =>
              typing0 get ld map { jtpes =>
                typing + (ld -> (jtpes + jtpe0))
              } getOrElse {
                typing + (ld -> Set(jtpe0))
              }
            } getOrElse typing
  
          case Operate(op, parent) => inner(Some(op.tpe.arg), typing, splits, parent)
    
          case Reduce(red, parent) => inner(Some(red.tpe.arg), typing, splits, parent)

          case MegaReduce(_, _) =>
            sys.error("Cannot infer type of MegaReduce. MegaReduce optimization must come after inferTypes.")
    
          case Morph1(m, parent) => inner(Some(m.tpe.arg), typing, splits, parent)
    
          case Morph2(m, left, right) =>
            inner(Some(m.tpe.arg1), inner(Some(m.tpe.arg0), typing, splits, left), splits, right)
    
          case Join(DerefObject, Cross(_), left, right @ ConstString(str)) =>
            inner(jtpe map { jtpe0 => JObjectFixedT(Map(str -> jtpe0)) }, typing, splits, left)
    
          case Join(DerefArray, Cross(_), left, right @ ConstDecimal(d)) =>
            inner(jtpe map { jtpe0 => JArrayFixedT(Map(d.toInt -> jtpe0)) }, typing, splits, left)
    
          case Join(WrapObject, Cross(_), ConstString(str), right) => {
            val jtpe2 = jtpe map {
              case JObjectFixedT(map) =>
                map get str getOrElse universe
              
              case _ => universe
            }
            
            inner(jtpe2, typing, splits, right)
          }
    
          case Join(ArraySwap, Cross(_), left, right) => {
            val jtpe2 = jtpe flatMap {
              case JArrayFixedT(_) => jtpe
              case _ => Some(JArrayUnfixedT)
            }
            
            inner(Some(JNumberT), inner(jtpe2, typing, splits, left), splits, right)
          }
    
          case Join(op: BinaryOperation, _, left, right) =>
            inner(Some(op.tpe.arg1), inner(Some(op.tpe.arg0), typing, splits, left), splits, right)
    
          case Assert(pred, child) => inner(jtpe, inner(jtpe, typing, splits, pred), splits, child)
          
          case graph @ Cond(pred, left, _, right, _) =>
            inner(jtpe, typing, splits, graph.peer)
          
          case Observe(data, samples) => inner(jtpe, inner(jtpe, typing, splits, data), splits, samples)
          
          case IUI(_, left, right) => inner(jtpe, inner(jtpe, typing, splits, left), splits, right)
  
          case Diff(left, right) => inner(jtpe, inner(jtpe, typing, splits, left), splits, right)
    
          case Filter(_, target, boolean) =>
            inner(Some(JBooleanT), inner(jtpe, typing, splits, target), splits, boolean)
    
          case AddSortKey(parent, _, _, _) => inner(jtpe, typing, splits, parent)
          
          case Memoize(parent, _) => inner(jtpe, typing, splits, parent)
    
          case Distinct(parent) => inner(jtpe, typing, splits, parent)
    
          case s @ Split(spec, child, id) =>
            inner(jtpe, collectSpecTypes(typing, splits, spec), splits + (id -> s), child)
          
          // not using extractors due to bug
          case s: SplitGroup => {
            val Split(spec, _, _) = splits(s.parentId)
            findGroup(spec, s.id) map { inner(jtpe, typing, splits, _) } getOrElse typing
          }
          
          // not using extractors due to bug
          case s: SplitParam => {
            val Split(spec, _, _) = splits(s.parentId)
            
            findParams(spec, s.id).foldLeft(typing) { (typing, graph) =>
              inner(jtpe, typing, splits, graph)
            }
          }
        }
      }
      
      inner(Some(universe), Map(), Map(), graph)
    }

    def applyTypes(typing: Map[DepGraph, JType], graph: DepGraph): DepGraph = {
      graph mapDown { recurse => {
        case ld @ AbsoluteLoad(parent, _) =>
          AbsoluteLoad(recurse(parent), typing(ld))(ld.loc)
        
        case ld @ RelativeLoad(parent, _) =>
          RelativeLoad(recurse(parent), typing(ld))(ld.loc)
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
