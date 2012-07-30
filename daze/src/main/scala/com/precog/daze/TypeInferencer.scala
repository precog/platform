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

        case Memoize(parent, _) => collectTypes(jtpe, typing, parent)
  
        case Distinct(_, parent) => collectTypes(jtpe, typing, parent)
  
        case s @ Split(_, spec, child) => collectTypes(jtpe, collectSpecTypes(jtpe, typing, spec), child)
  
        case _ : SplitGroup | _ : SplitParam => typing
      }
    }

    def applySpecTypes(typing: Map[DepGraph, JType], splits: => Map[Split, Split], spec: BucketSpec): BucketSpec = spec match {
      case UnionBucketSpec(left, right) =>
        UnionBucketSpec(applySpecTypes(typing, splits, left), applySpecTypes(typing, splits, right))
      
      case IntersectBucketSpec(left, right) =>
        IntersectBucketSpec(applySpecTypes(typing, splits, left), applySpecTypes(typing, splits, right))
      
      case Group(id, target, child) =>
        Group(id, applyTypes(typing, splits, target), applySpecTypes(typing, splits, child))
      
      case UnfixedSolution(id, target) =>
        UnfixedSolution(id, applyTypes(typing, splits, target))
      
      case Extra(target) =>
        Extra(applyTypes(typing, splits, target))
    }

    def applyTypes(typing: Map[DepGraph, JType], splits0: => Map[Split, Split], graph: DepGraph) : DepGraph = {
      lazy val splits = splits0

      def inner(graph: DepGraph): DepGraph = graph match {
        case r : Root => r
  
        case New(loc, parent) => New(loc, applyTypes(typing, splits, parent))
  
        case l @ LoadLocal(loc, parent, _) => LoadLocal(loc, applyTypes(typing, splits, parent), typing(l))
  
        case Operate(loc, op, parent) => Operate(loc, op, applyTypes(typing, splits, parent))
  
        case Reduce(loc, red, parent) => Reduce(loc, red, applyTypes(typing, splits, parent))
  
        case Morph1(loc, m, parent) => Morph1(loc, m, applyTypes(typing, splits, parent))
  
        case Morph2(loc, m, left, right) => Morph2(loc, m, applyTypes(typing, splits, left), applyTypes(typing, splits, right))
  
        case Join(loc, op, joinSort, left, right) => Join(loc, op, joinSort, applyTypes(typing, splits, left), applyTypes(typing, splits, right))

        case IUI(loc, union, left, right) => IUI(loc, union, applyTypes(typing, splits, left), applyTypes(typing, splits, right))

        case Diff(loc, left, right) => Diff(loc, applyTypes(typing, splits, left), applyTypes(typing, splits, right))
  
        case Filter(loc, cross, target, boolean) =>
          Filter(loc, cross, applyTypes(typing, splits, target), applyTypes(typing, splits, boolean))
  
        case Sort(parent, indices) => Sort(applyTypes(typing, splits, parent), indices)
        
        case SortBy(parent, sortField, valueField, id) => SortBy(applyTypes(typing, splits, parent), sortField, valueField, id)
  
        case Memoize(parent, priority) => Memoize(applyTypes(typing, splits, parent), priority)
  
        case Distinct(loc, parent) => Distinct(loc, applyTypes(typing, splits, parent))
  
        case s @ Split(loc, spec, child) => {
          lazy val splits2 = splits + (s -> s2)
          lazy val spec2 = applySpecTypes(typing, splits2, spec)
          lazy val child2 = applyTypes(typing, splits2, child)
          lazy val s2: Split = Split(loc, spec2, child2)
          s2
        }
  
        case s @ SplitGroup(loc, id, provenance) => SplitGroup(loc, id, provenance)(splits(s.parent))
  
        case s @ SplitParam(loc, id) => SplitParam(loc, id)(splits(s.parent))
      }

      memotable.get(graph) getOrElse {
        val result = inner(graph)
        memotable += (graph -> result)
        result
      }
    }
    
    val typing = collectTypes(jtpe, Map(), graph).mapValues(_.reduce(JUnionT)) 
    applyTypes(typing, Map(), graph)
  }
}
