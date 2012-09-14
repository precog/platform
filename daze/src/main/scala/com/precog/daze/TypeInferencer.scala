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

  def inferTypes(jtpe: JType)(graph: DepGraph) : DepGraph = {

    val memotable = mutable.Map[DepGraph, DepGraph]()

    def collectSpecTypes(jtpe: JType, typing: Map[DepGraph, Set[JType]], spec: BucketSpec): Map[DepGraph, Set[JType]] = spec match {
      case UnionBucketSpec(left, right) =>
        collectSpecTypes(jtpe, collectSpecTypes(jtpe, typing, left), right) 
      
      case IntersectBucketSpec(left, right) =>
        collectSpecTypes(jtpe, collectSpecTypes(jtpe, typing, left), right) 
      
      case Group(id, target, child) =>
        collectSpecTypes(jtpe, collectTypes(jtpe, typing, target), child)
      
      case UnfixedSolution(id, target) =>
        collectTypes(jtpe, typing, target)
      
      case Extra(target) =>
        collectTypes(jtpe, typing, target)
    }

    def collectTypes(jtpe: JType, typing: Map[DepGraph, Set[JType]], graph: DepGraph): Map[DepGraph, Set[JType]] = {
      graph match {
        case _ : Root => typing 
  
        case New(_, parent) => collectTypes(jtpe, typing, parent)
  
        case l @ LoadLocal(_, parent, _) =>
          val typing0 = collectTypes(JTextT, typing, parent)
          typing0.get(l).map { jtpes => typing + (l -> (jtpes + jtpe)) }.getOrElse(typing + (l -> Set(jtpe)))

        case Operate(_, op, parent) => collectTypes(op.tpe.arg, typing, parent)
  
        case Reduce(_, red, parent) => collectTypes(red.tpe.arg, typing, parent)

        case MegaReduce(_, reds, parent) => collectTypes(JType.JUnfixedT, typing, parent)
  
        case Morph1(_, m, parent) => collectTypes(m.tpe.arg, typing, parent)
  
        case Morph2(_, m, left, right) => collectTypes(m.tpe.arg1, collectTypes(m.tpe.arg0, typing, left), right)
  
        case Join(_, DerefObject, CrossLeftSort | CrossRightSort, left, right @ ConstString(str)) =>
          collectTypes(JObjectFixedT(Map(str -> jtpe)), typing, left)
  
        case Join(_, DerefArray, CrossLeftSort | CrossRightSort, left, right @ ConstDecimal(d)) =>
          collectTypes(JArrayFixedT(Map(d.toInt -> jtpe)), typing, left)
  
        case Join(_, WrapObject, CrossLeftSort | CrossRightSort, left, right) =>
          collectTypes(jtpe, collectTypes(JTextT, typing, left), right)
  
        case Join(_, ArraySwap, CrossLeftSort | CrossRightSort, left, right) =>
          collectTypes(JNumberT, collectTypes(jtpe, typing, left), right)
  
        case Join(_, op : BinaryOperation, _, left, right) =>
          collectTypes(op.tpe.arg1, collectTypes(op.tpe.arg0, typing, left), right)
  
        case Join(_, _, _, left, right) => collectTypes(jtpe, collectTypes(jtpe, typing, left), right)

        case IUI(_, _, left, right) => collectTypes(jtpe, collectTypes(jtpe, typing, left), right)

        case Diff(_, left, right) => collectTypes(jtpe, collectTypes(jtpe, typing, left), right)
  
        case Filter(_, _, target, boolean) =>
          collectTypes(JBooleanT, collectTypes(jtpe, typing, target), boolean)
  
        case Sort(parent, _) => collectTypes(jtpe, typing, parent)
  
        case SortBy(parent, _, _, _) => collectTypes(jtpe, typing, parent)
        
        case ReSortBy(parent, _) => collectTypes(jtpe, typing, parent)

        case Memoize(parent, _) => collectTypes(jtpe, typing, parent)
  
        case Distinct(_, parent) => collectTypes(jtpe, typing, parent)
  
        case s @ Split(_, spec, child) => collectTypes(jtpe, collectSpecTypes(jtpe, typing, spec), child)
  
        case _ : SplitGroup | _ : SplitParam => typing
      }
    }

    def applyTypes(typing: Map[DepGraph, JType], graph: DepGraph): DepGraph = {
      graph mapDown { recurse => {
        case ld @ LoadLocal(loc, parent, _) =>
          LoadLocal(loc, recurse(parent), typing(ld))
      }}
    }
    
    val collectedTypes = collectTypes(jtpe, Map(), graph)
    val typing = collectedTypes.mapValues(_.reduce(JUnionT)) 
    applyTypes(typing, graph)
  }
}
