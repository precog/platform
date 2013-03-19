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
      def collectSpecTypes(typing: Map[DepGraph, Set[JType]], spec: BucketSpec): Map[DepGraph, Set[JType]] = spec match {
        case UnionBucketSpec(left, right) =>
          collectSpecTypes(collectSpecTypes(typing, left), right) 
        
        case IntersectBucketSpec(left, right) =>
          collectSpecTypes(collectSpecTypes(typing, left), right) 
        
        case Group(id, target, child) =>
          collectSpecTypes(inner(None, typing, target), child)
        
        case UnfixedSolution(id, target) =>
          inner(Some(universe), typing, target)
        
        case Extra(target) =>
          inner(Some(universe), typing, target)
      }
    
      def inner(jtpe: Option[JType], typing: Map[DepGraph, Set[JType]], graph: DepGraph): Map[DepGraph, Set[JType]] = {
        graph match {
          case _: Root => typing 
    
          case New(parent) => inner(jtpe, typing, parent)
    
          case ld @ LoadLocal(parent, _) =>
            val typing0 = inner(Some(JTextT), typing, parent)
            jtpe map { jtpe0 =>
              typing0 get ld map { jtpes =>
                typing + (ld -> (jtpes + jtpe0))
              } getOrElse {
                typing + (ld -> Set(jtpe0))
              }
            } getOrElse typing
  
          case Operate(op, parent) => inner(Some(op.tpe.arg), typing, parent)
    
          case Reduce(red, parent) => inner(Some(red.tpe.arg), typing, parent)

          case MegaReduce(_, _) =>
            sys.error("Cannot infer type of MegaReduce. MegaReduce optimization must come after inferTypes.")
    
          case Morph1(m, parent) => inner(Some(m.tpe.arg), typing, parent)
    
          case Morph2(m, left, right) => inner(Some(m.tpe.arg1), inner(Some(m.tpe.arg0), typing, left), right)
    
          case Join(DerefObject, CrossLeftSort | CrossRightSort, left, right @ ConstString(str)) =>
            inner(jtpe map { jtpe0 => JObjectFixedT(Map(str -> jtpe0)) }, typing, left)
    
          case Join(DerefArray, CrossLeftSort | CrossRightSort, left, right @ ConstDecimal(d)) =>
            inner(jtpe map { jtpe0 => JArrayFixedT(Map(d.toInt -> jtpe0)) }, typing, left)
    
          case Join(WrapObject, CrossLeftSort | CrossRightSort, ConstString(str), right) => {
            val jtpe2 = jtpe map {
              case JObjectFixedT(map) =>
                map get str getOrElse universe
              
              case _ => universe
            }
            
            inner(jtpe2, typing, right)
          }
    
          case Join(ArraySwap, CrossLeftSort | CrossRightSort, left, right) => {
            val jtpe2 = jtpe flatMap {
              case JArrayFixedT(_) => jtpe
              case _ => Some(JArrayUnfixedT)
            }
            
            inner(Some(JNumberT), inner(jtpe2, typing, left), right)
          }
    
          case Join(op: BinaryOperation, _, left, right) =>
            inner(Some(op.tpe.arg1), inner(Some(op.tpe.arg0), typing, left), right)
    
          case Assert(pred, child) => inner(jtpe, inner(jtpe, typing, pred), child)
          
          case graph @ Cond(pred, left, _, right, _) =>
            inner(jtpe, typing, graph.peer)
          
          case Observe(data, samples) => inner(jtpe, inner(jtpe, typing, data), samples)
          
          case IUI(_, left, right) => inner(jtpe, inner(jtpe, typing, left), right)
  
          case Diff(left, right) => inner(jtpe, inner(jtpe, typing, left), right)
    
          case Filter(_, target, boolean) =>
            inner(Some(JBooleanT), inner(jtpe, typing, target), boolean)
    
          case Sort(parent, _) => inner(jtpe, typing, parent)
    
          case SortBy(parent, _, _, _) => inner(jtpe, typing, parent)
          
          case ReSortBy(parent, _) => inner(jtpe, typing, parent)
  
          case Memoize(parent, _) => inner(jtpe, typing, parent)
    
          case Distinct(parent) => inner(jtpe, typing, parent)
    
          case s @ Split(spec, child) =>
            inner(jtpe, collectSpecTypes(typing, spec), child)
          
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
      
      inner(Some(universe), Map(), graph)
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
