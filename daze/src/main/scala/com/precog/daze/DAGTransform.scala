package com.precog
package daze

import scala.collection.mutable

import com.precog.util.IdGen

trait DAGTransform extends DAG {
  import dag._
  import instructions.{ DerefObject, Eq, JoinObject, Line, PushString, WrapObject }

  def transformBottomUp(graph: DepGraph)(f : DepGraph => DepGraph) : DepGraph = {

    val memotable = mutable.Map[DepGraph, DepGraph]()

    def transformSpec(spec: BucketSpec): BucketSpec = spec match {
      case UnionBucketSpec(left, right) =>
        UnionBucketSpec(transformSpec(left), transformSpec(right))
      
      case IntersectBucketSpec(left, right) =>
        IntersectBucketSpec(transformSpec(left), transformSpec(right))
      
      case Group(id, target, child) =>
        Group(id, transformAux(target), transformSpec(child))
      
      case UnfixedSolution(id, target) =>
        UnfixedSolution(id, transformAux(target))
      
      case Extra(target) =>
        Extra(transformAux(target))
    }

    def transformAux(graph: DepGraph) : DepGraph = {
      def inner(graph: DepGraph): DepGraph = graph match {
        case r : Root => f(r)
  
        case graph @ New(parent) => f(New(transformAux(parent))(graph.loc))
        
        case graph @ LoadLocal(parent, jtpe) => f(LoadLocal(transformAux(parent), jtpe)(graph.loc))
  
        case graph @ Operate(op, parent) => f(Operate(op, transformAux(parent))(graph.loc))
  
        case graph @ Reduce(red, parent) => f(Reduce(red, transformAux(parent))(graph.loc))
        
        case MegaReduce(reds, parent) => f(MegaReduce(reds, transformAux(parent)))
  
        case graph @ Morph1(m, parent) => f(Morph1(m, transformAux(parent))(graph.loc))
  
        case graph @ Morph2(m, left, right) => f(Morph2(m, transformAux(left), transformAux(right))(graph.loc))
  
        case graph @ Join(op, joinSort, left, right) => f(Join(op, joinSort, transformAux(left), transformAux(right))(graph.loc))
  
        case graph @ Assert(pred, child) => f(Assert(transformAux(pred), transformAux(child))(graph.loc))
        
        case graph @ Cond(pred, left, leftJoin, right, rightJoin) => f(Cond(transformAux(pred), transformAux(left), leftJoin, transformAux(right), rightJoin)(graph.loc))
        
        case graph @ Observe(data, samples) => f(Observe(transformAux(data), transformAux(samples))(graph.loc))
        
        case graph @ IUI(union, left, right) => f(IUI(union, transformAux(left), transformAux(right))(graph.loc))

        case graph @ Diff(left, right) => f(Diff(transformAux(left), transformAux(right))(graph.loc))

        case graph @ Filter(cross, target, boolean) =>
          f(Filter(cross, transformAux(target), transformAux(boolean))(graph.loc))
  
        case Sort(parent, indices) => f(Sort(transformAux(parent), indices))

        case SortBy(parent, sortField, valueField, id) => f(SortBy(transformAux(parent), sortField, valueField, id))
        
        case ReSortBy(parent, id) => f(ReSortBy(transformAux(parent), id))
  
        case Memoize(parent, priority) => f(Memoize(transformAux(parent), priority))
  
        case graph @ Distinct(parent) => f(Distinct(transformAux(parent))(graph.loc))
  
        case s @ Split(spec, child, id) => {
          val spec2 = transformSpec(spec)
          val child2 = transformAux(child)
          f(Split(spec2, child2, id)(s.loc))
        }
  
        // not using extractors due to bug
        case s: SplitGroup =>
          f(SplitGroup(s.id, s.identities, s.parentId)(s.loc))
  
        // not using extractors due to bug
        case s: SplitParam =>
          f(SplitParam(s.id, s.parentId)(s.loc))
      }

      memotable.get(graph) getOrElse {
        val result = inner(graph)
        memotable += (graph -> result)
        result
      }
    }
    
    transformAux(graph)
  }
}
